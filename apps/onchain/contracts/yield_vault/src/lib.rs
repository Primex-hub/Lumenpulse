#![no_std]

mod events;
mod storage;

use soroban_sdk::token::TokenClient;
use soroban_sdk::{contract, contractclient, contractimpl, vec, Address, Env, Symbol, Vec};
use storage::{DataKey, ProviderMetrics, YieldProvider};

#[contractclient(name = "YieldProviderClient")]
pub trait YieldProviderTrait {
    fn deposit(env: Env, from: Address, amount: i128) -> i128;
    fn withdraw(env: Env, to: Address, amount: i128) -> i128;
    fn balance(env: Env, address: Address) -> i128;
}

#[contract]
pub struct YieldVaultContract;

/// YieldVault - Multi-provider yield optimization
///
/// This contract wraps multiple yield providers and:
/// - Manages allocation across providers
/// - Tracks yields earned per provider
/// - Allows yield harvesting
/// - Routes deposits to highest-yield providers
/// - Maintains user balances and claims
#[contractimpl]
impl YieldVaultContract {
    /// Initialize the vault
    pub fn initialize(env: Env, admin: Address, asset: Address) -> Result<(), Symbol> {
        if env.storage().instance().has(&DataKey::Admin) {
            return Err(Symbol::new(&env, "already_initialized"));
        }

        env.storage().instance().set(&DataKey::Admin, &admin);
        env.storage().instance().set(&DataKey::Asset, &asset);
        env.storage().instance().set(&DataKey::ProviderCount, &0u32);
        env.storage().instance().bump(100, 100);

        events::VaultInitializedEvent { admin, asset }.publish(&env);

        Ok(())
    }

    /// Register a new yield provider
    pub fn register_provider(
        env: Env,
        name: Symbol,
        address: Address,
        priority: u32, // Higher = preferred for deposits
    ) -> Result<u32, Symbol> {
        let admin: Address = env
            .storage()
            .instance()
            .get(&DataKey::Admin)
            .ok_or_else(|| Symbol::new(&env, "not_initialized"))?;

        admin.require_auth();

        let provider_count: u32 = env
            .storage()
            .instance()
            .get(&DataKey::ProviderCount)
            .unwrap_or(0);

        let provider_id = provider_count;

        let provider = YieldProvider {
            id: provider_id,
            name: name.clone(),
            address: address.clone(),
            priority,
            total_deposited: 0,
            total_withdrawn: 0,
            total_yield_earned: 0,
            is_active: true,
        };

        env.storage()
            .persistent()
            .set(&DataKey::Provider(provider_id), &provider);

        let new_count = provider_count + 1;
        env.storage()
            .instance()
            .set(&DataKey::ProviderCount, &new_count);

        events::ProviderRegisteredEvent {
            provider_id,
            name,
            address,
            priority,
        }
        .publish(&env);

        Ok(provider_id)
    }

    /// Deposit tokens into the vault
    /// Automatically allocates to highest-yield provider
    pub fn deposit(
        env: Env,
        amount: i128,
        user: Address,
        request_id: soroban_sdk::BytesN<32>,
    ) -> Result<i128, Symbol> {
        // Idempotency check
        if idempotency_guard::claim_request(&env, &request_id).is_err() {
            return Err(Symbol::new(&env, "already_executed"));
        }

        if amount <= 0 {
            return Err(Symbol::new(&env, "invalid_amount"));
        }

        let asset_addr: Address = env
            .storage()
            .instance()
            .get(&DataKey::Asset)
            .ok_or_else(|| Symbol::new(&env, "not_initialized"))?;

        // Transfer tokens from caller
        let token = TokenClient::new(&env, &asset_addr);
        token.transfer(&env.invoker(), &env.current_contract_address(), &amount);

        // Find best provider (highest priority, is_active)
        let best_provider = Self::find_best_provider(&env)?;

        // Deposit to provider
        let provider: YieldProvider = env
            .storage()
            .persistent()
            .get(&DataKey::Provider(best_provider))
            .ok_or_else(|| Symbol::new(&env, "provider_not_found"))?;

        let provider_client = YieldProviderClient::new(&env, &provider.address);
        let _yield_tokens = provider_client.deposit(&env.current_contract_address(), &amount);

        // Update vault state
        let mut updated_provider = provider.clone();
        updated_provider.total_deposited += amount;
        env.storage()
            .persistent()
            .set(&DataKey::Provider(best_provider), &updated_provider);

        // Track user deposit
        let user_balance: i128 = env
            .storage()
            .persistent()
            .get(&DataKey::UserBalance(user.clone()))
            .unwrap_or(0);

        env.storage().persistent().set(
            &DataKey::UserBalance(user.clone()),
            &(user_balance + amount),
        );

        // Track allocation per user-provider
        let user_allocation: i128 = env
            .storage()
            .persistent()
            .get(&DataKey::UserProviderAllocation(
                user.clone(),
                best_provider,
            ))
            .unwrap_or(0);

        env.storage().persistent().set(
            &DataKey::UserProviderAllocation(user.clone(), best_provider),
            &(user_allocation + amount),
        );

        // Update total vault AUM
        let total_aum: i128 = env
            .storage()
            .persistent()
            .get(&DataKey::TotalAUM)
            .unwrap_or(0);

        env.storage()
            .persistent()
            .set(&DataKey::TotalAUM, &(total_aum + amount));

        events::DepositEvent {
            user: user.clone(),
            amount,
            provider_id: best_provider,
        }
        .publish(&env);

        Ok(amount)
    }

    /// Withdraw tokens from the vault
    /// Attempts to withdraw from provider with user's allocation
    pub fn withdraw(
        env: Env,
        amount: i128,
        user: Address,
        request_id: soroban_sdk::BytesN<32>,
    ) -> Result<i128, Symbol> {
        // Idempotency check
        if idempotency_guard::claim_request(&env, &request_id).is_err() {
            return Err(Symbol::new(&env, "already_executed"));
        }

        if amount <= 0 {
            return Err(Symbol::new(&env, "invalid_amount"));
        }

        let user_balance: i128 = env
            .storage()
            .persistent()
            .get(&DataKey::UserBalance(user.clone()))
            .unwrap_or(0);

        if user_balance < amount {
            return Err(Symbol::new(&env, "insufficient_balance"));
        }

        // Find provider with user's allocation (FIFO: use first provider with balance)
        let provider_count: u32 = env
            .storage()
            .instance()
            .get(&DataKey::ProviderCount)
            .unwrap_or(0);

        let mut withdrawn = 0i128;
        let mut remaining = amount;

        for provider_id in 0..provider_count {
            if remaining == 0 {
                break;
            }

            let allocation: i128 = env
                .storage()
                .persistent()
                .get(&DataKey::UserProviderAllocation(user.clone(), provider_id))
                .unwrap_or(0);

            if allocation > 0 {
                let to_withdraw = if remaining > allocation {
                    allocation
                } else {
                    remaining
                };

                let provider: YieldProvider = env
                    .storage()
                    .persistent()
                    .get(&DataKey::Provider(provider_id))
                    .ok_or_else(|| Symbol::new(&env, "provider_not_found"))?;

                let provider_client = YieldProviderClient::new(&env, &provider.address);
                let _received = provider_client.withdraw(&env.invoker(), &to_withdraw);

                // Update tracking
                let mut updated_provider = provider.clone();
                updated_provider.total_withdrawn += to_withdraw;
                env.storage()
                    .persistent()
                    .set(&DataKey::Provider(provider_id), &updated_provider);

                let new_allocation = allocation - to_withdraw;
                if new_allocation > 0 {
                    env.storage().persistent().set(
                        &DataKey::UserProviderAllocation(user.clone(), provider_id),
                        &new_allocation,
                    );
                } else {
                    env.storage()
                        .persistent()
                        .remove(&DataKey::UserProviderAllocation(user.clone(), provider_id));
                }

                withdrawn += to_withdraw;
                remaining -= to_withdraw;
            }
        }

        // Update user balance
        let new_balance = user_balance - withdrawn;
        if new_balance > 0 {
            env.storage()
                .persistent()
                .set(&DataKey::UserBalance(user.clone()), &new_balance);
        } else {
            env.storage()
                .persistent()
                .remove(&DataKey::UserBalance(user.clone()));
        }

        // Update total vault AUM
        let total_aum: i128 = env
            .storage()
            .persistent()
            .get(&DataKey::TotalAUM)
            .unwrap_or(0);

        env.storage()
            .persistent()
            .set(&DataKey::TotalAUM, &(total_aum - withdrawn));

        events::WithdrawEvent {
            user: user.clone(),
            amount: withdrawn,
        }
        .publish(&env);

        Ok(withdrawn)
    }

    /// Harvest yield earned by a specific provider
    /// Routes yield to a reward pool or redistributes to LPs
    pub fn harvest_yield(env: Env, provider_id: u32) -> Result<i128, Symbol> {
        let admin: Address = env
            .storage()
            .instance()
            .get(&DataKey::Admin)
            .ok_or_else(|| Symbol::new(&env, "not_initialized"))?;

        admin.require_auth();

        let mut provider: YieldProvider = env
            .storage()
            .persistent()
            .get(&DataKey::Provider(provider_id))
            .ok_or_else(|| Symbol::new(&env, "provider_not_found"))?;

        let provider_client = YieldProviderClient::new(&env, &provider.address);
        let balance = provider_client.balance(&env.current_contract_address());

        // Estimate yield = (balance - deposited + withdrawn)
        let yield_earned = balance - provider.total_deposited + provider.total_withdrawn;

        if yield_earned > 0 {
            provider.total_yield_earned += yield_earned;
            env.storage()
                .persistent()
                .set(&DataKey::Provider(provider_id), &provider);

            // Track total yield for later distribution
            let total_yield: i128 = env
                .storage()
                .persistent()
                .get(&DataKey::TotalYieldHarvested)
                .unwrap_or(0);

            env.storage()
                .persistent()
                .set(&DataKey::TotalYieldHarvested, &(total_yield + yield_earned));
        }

        events::YieldHarvestedEvent {
            provider_id,
            yield_earned,
        }
        .publish(&env);

        Ok(yield_earned)
    }

    /// Get user's balance
    pub fn balance_of(env: Env, user: Address) -> i128 {
        env.storage()
            .persistent()
            .get(&DataKey::UserBalance(user))
            .unwrap_or(0)
    }

    /// Get total vault AUM
    pub fn get_total_aum(env: Env) -> i128 {
        env.storage()
            .persistent()
            .get(&DataKey::TotalAUM)
            .unwrap_or(0)
    }

    /// Get total harvested yield
    pub fn get_total_yield_harvested(env: Env) -> i128 {
        env.storage()
            .persistent()
            .get(&DataKey::TotalYieldHarvested)
            .unwrap_or(0)
    }

    /// Get provider info
    pub fn get_provider(env: Env, provider_id: u32) -> Result<YieldProvider, Symbol> {
        env.storage()
            .persistent()
            .get(&DataKey::Provider(provider_id))
            .ok_or_else(|| Symbol::new(&env, "provider_not_found"))
    }

    /// Find best active provider by priority
    fn find_best_provider(env: &Env) -> Result<u32, Symbol> {
        let provider_count: u32 = env
            .storage()
            .instance()
            .get(&DataKey::ProviderCount)
            .unwrap_or(0);

        if provider_count == 0 {
            return Err(Symbol::new(env, "no_providers_available"));
        }

        let mut best_id = 0u32;
        let mut best_priority = 0u32;

        for provider_id in 0..provider_count {
            if let Ok(Some(provider)) = env
                .storage()
                .persistent()
                .get::<_, Option<YieldProvider>>(&DataKey::Provider(provider_id))
            {
                if provider.is_active && provider.priority > best_priority {
                    best_priority = provider.priority;
                    best_id = provider_id;
                }
            }
        }

        Ok(best_id)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use soroban_sdk::testutils::Address as _;
    use soroban_sdk::token::StellarAssetClient;

    #[contract]
    struct MockYieldProvider;

    #[contractimpl]
    impl MockYieldProvider {
        pub fn deposit(env: Env, from: Address, amount: i128) -> i128 {
            let current: i128 = env.storage().persistent().get(&from).unwrap_or(0);
            env.storage().persistent().set(&from, &(current + amount));
            amount // return same amount as "yield tokens"
        }

        pub fn withdraw(env: Env, to: Address, amount: i128) -> i128 {
            let current: i128 = env.storage().persistent().get(&to).unwrap_or(0);
            if current < amount {
                panic!("insufficient balance in mock");
            }
            env.storage().persistent().set(&to, &(current - amount));
            amount
        }

        pub fn balance(env: Env, address: Address) -> i128 {
            env.storage().persistent().get(&address).unwrap_or(0)
        }
    }

    fn request_id(env: &Env) -> soroban_sdk::BytesN<32> {
        soroban_sdk::BytesN::from_array(env, &[0; 32])
    }

    #[test]
    fn test_deposit_idempotency() {
        let env = Env::default();
        env.mock_all_auths();

        let admin = Address::generate(&env);
        let user = Address::generate(&env);
        let token_admin = Address::generate(&env);

        let token_id = env.register_stellar_asset_contract_v2(token_admin.clone());
        let token_client = TokenClient::new(&env, &token_id.address());
        let token_admin_client = StellarAssetClient::new(&env, &token_id.address());

        // Deploy vault and mock provider
        let vault_id = env.register(YieldVaultContract, ());
        let vault_client = YieldProviderClient::new(&env, &vault_id);

        let mock_id = env.register(MockYieldProvider, ());

        // Initialize vault
        vault_client.initialize(&admin, &token_id.address());

        // Register mock provider
        vault_client.register_provider(
            &Symbol::new(&env, "mock_provider"),
            &mock_id,
            &1, // priority
        );

        // Mint tokens to user
        let deposit_amount = 1000i128;
        token_admin_client.mint(&user, &deposit_amount);

        // First deposit should succeed
        let result = vault_client.deposit(&deposit_amount, &user, &request_id(&env));
        assert_eq!(result, deposit_amount);

        // Second deposit with same request_id should fail
        let result = vault_client.try_deposit(&deposit_amount, &user, &request_id(&env));
        assert_eq!(result, Err(Ok(Symbol::new(&env, "already_executed"))));
    }

    #[test]
    fn test_withdraw_idempotency() {
        let env = Env::default();
        env.mock_all_auths();

        let admin = Address::generate(&env);
        let user = Address::generate(&env);
        let token_admin = Address::generate(&env);

        let token_id = env.register_stellar_asset_contract_v2(token_admin.clone());
        let token_client = TokenClient::new(&env, &token_id.address());
        let token_admin_client = StellarAssetClient::new(&env, &token_id.address());

        let vault_id = env.register(YieldVaultContract, ());
        let vault_client = YieldProviderClient::new(&env, &vault_id);

        let mock_id = env.register(MockYieldProvider, ());

        vault_client.initialize(&admin, &token_id.address());
        vault_client.register_provider(&Symbol::new(&env, "mock_provider"), &mock_id, &1);

        // Mint tokens to user and deposit
        let deposit_amount = 1000i128;
        token_admin_client.mint(&user, &deposit_amount);
        vault_client.deposit(&deposit_amount, &user, &BytesN::from_array(&env, &[1; 32]));

        // First withdraw should succeed
        let withdraw_amount = 500i128;
        let result = vault_client.withdraw(&withdraw_amount, &user, &request_id(&env));
        assert_eq!(result, withdraw_amount);

        // Second withdraw with same request_id should fail
        let result = vault_client.try_withdraw(&withdraw_amount, &user, &request_id(&env));
        assert_eq!(result, Err(Ok(Symbol::new(&env, "already_executed"))));
    }
}
