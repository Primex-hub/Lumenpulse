# Yield-Bearing Vault Extensions - Backend Integration Guide

## Overview

The YieldVault contract manages multi-provider yield optimization. This guide covers backend integration for:
- Yield tracking and analytics
- APY calculations
- Provider performance monitoring
- Reward distribution
- User yield statements

## Architecture

```
Yield Providers (Soroban)
    ├── AaveLendingPool (interest accrual)
    ├── StableSwapPool (trading fees)
    └── LiquidityPool (LP fees)
    ↓
YieldVault (Aggregator)
    ↓
Backend Yield Service
    ├── Performance Tracking
    ├── APY Calculations
    ├── Reward Distribution
    └── Analytics
```

## Smart Contract Integration

### Mock Yield Providers

#### 1. Aave Lending Pool
- **Mechanism**: Interest accrual on deposits
- **APY**: Variable, based on utilization (3% base + utilization * 2%)
- **Key Methods**: `deposit()`, `withdraw()`, `accrue_interest()`
- **Yield Source**: Interest earned over time

#### 2. Stable Swap Pool
- **Mechanism**: LP fees from stable coin swaps
- **APY**: Varies with swap volume
- **Key Methods**: `add_liquidity()`, `swap()`, `remove_liquidity()`
- **Yield Source**: 0.04% swap fees distributed to LPs

#### 3. Liquidity Pool
- **Mechanism**: AMM trading fees + accrued fees
- **APY**: Varies with trading volume (0.3% swap fee)
- **Key Methods**: `add_liquidity()`, `swap_exact_in()`, `remove_liquidity()`
- **Yield Source**: Trading fees accumulated in reserves

### YieldVault Integration

```typescript
// Smart contract types
interface YieldProvider {
  id: u32;
  name: Symbol;
  address: Address;
  priority: u32;        // Higher = preferred
  total_deposited: i128;
  total_withdrawn: i128;
  total_yield_earned: i128;
  is_active: bool;
}

// Key operations
vault.deposit(amount, user)           // Routes to highest-priority provider
vault.withdraw(amount, user)          // FIFO withdrawal from providers
vault.harvest_yield(provider_id)      // Collect earned yield
vault.balance_of(user)                // User's total balance
vault.get_total_aum()                 // Total assets under management
```

## Backend Services

### 1. Yield Tracker Service

Track yield earned across all providers:

```typescript
// packages/backend/src/yield/yield-tracker.service.ts

@Injectable()
export class YieldTrackerService {
  constructor(
    private db: PrismaClient,
    private sorobanClient: SorobanClient,
    private cache: CacheService,
  ) {}

  /**
   * Harvest yield from a specific provider
   */
  async harvestYield(providerId: string): Promise<YieldHarvest> {
    const provider = await this.db.yieldProvider.findUnique({
      where: { id: providerId },
      include: { vault: true },
    });

    if (!provider) {
      throw new NotFoundException(`Provider ${providerId} not found`);
    }

    const vaultClient = new YieldVaultClient(
      this.sorobanClient.env,
      provider.vault.contractAddress,
    );

    // Harvest yield on-chain
    const harvestedAmount = await vaultClient.harvest_yield(provider.sorobanId);

    // Record harvest
    const harvest = await this.db.yieldHarvest.create({
      data: {
        provider: { connect: { id: providerId } },
        amount: harvestedAmount.toString(),
        timestamp: new Date(),
        transactionHash: this.sorobanClient.lastTxHash,
      },
    });

    // Update provider metrics
    await this.updateProviderMetrics(provider.id);

    // Invalidate cache
    this.cache.del(`yield:${provider.id}`);

    return harvest;
  }

  /**
   * Calculate APY for a provider
   */
  async calculateAPY(providerId: string, days: number = 30): Promise<number> {
    const harvests = await this.db.yieldHarvest.findMany({
      where: {
        providerId,
        timestamp: {
          gte: new Date(Date.now() - days * 24 * 60 * 60 * 1000),
        },
      },
    });

    if (harvests.length === 0) {
      return 0;
    }

    const totalYield = harvests.reduce(
      (sum, h) => sum + BigInt(h.amount),
      BigInt(0),
    );

    const provider = await this.db.yieldProvider.findUnique({
      where: { id: providerId },
      include: {
        vault: {
          include: { deposits: true },
        },
      },
    });

    // Average TVL for period
    const avgTVL = this.calculateAverageTVL(provider.vault.deposits, days);

    if (avgTVL === 0n) {
      return 0;
    }

    // APY = (totalYield / avgTVL) * (365 / days) * 100
    const apy =
      (Number(totalYield) / Number(avgTVL)) * (365 / days) * 100;

    return apy;
  }

  /**
   * Get all active provider metrics
   */
  async getAllProviderMetrics(): Promise<ProviderMetricsDto[]> {
    const providers = await this.db.yieldProvider.findMany({
      where: { isActive: true },
      include: {
        vault: true,
        metrics: { orderBy: { timestamp: 'desc' }, take: 1 },
      },
    });

    return Promise.all(
      providers.map(async (provider) => ({
        id: provider.id,
        name: provider.name,
        type: provider.type,
        apy: await this.calculateAPY(provider.id),
        tvl: await this.getTVL(provider.id),
        riskRating: provider.riskRating,
        lastHarvest: provider.metrics[0]?.timestamp || null,
      })),
    );
  }

  private async updateProviderMetrics(providerId: string): Promise<void> {
    const apy = await this.calculateAPY(providerId, 7); // 7-day APY
    const tvl = await this.getTVL(providerId);

    await this.db.providerMetrics.create({
      data: {
        provider: { connect: { id: providerId } },
        apy,
        tvl: tvl.toString(),
        timestamp: new Date(),
      },
    });
  }

  private calculateAverageTVL(
    deposits: UserDeposit[],
    days: number,
  ): bigint {
    // Group deposits by day and calculate average
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const relevantDeposits = deposits.filter(
      (d) => d.timestamp >= cutoff,
    );

    if (relevantDeposits.length === 0) {
      return 0n;
    }

    const total = relevantDeposits.reduce(
      (sum, d) => sum + BigInt(d.amount),
      BigInt(0),
    );

    return total / BigInt(relevantDeposits.length);
  }

  private async getTVL(providerId: string): Promise<bigint> {
    const deposits = await this.db.userDeposit.aggregate({
      where: {
        providerId,
        // Not withdrawn
        withdraw: null,
      },
      _sum: { amount: true },
    });

    return BigInt(deposits._sum.amount || 0);
  }
}
```

### 2. Yield Event Listener

Listen to yield-related events:

```typescript
// packages/backend/src/yield/yield-event.listener.ts

@Injectable()
export class YieldEventListener {
  constructor(
    private yieldTracker: YieldTrackerService,
    private notificationService: NotificationService,
    private db: PrismaClient,
  ) {}

  /**
   * Handle vault deposit event
   */
  async handleVaultDeposit(event: VaultDepositEvent) {
    // Event: { user, amount, provider_id }

    const deposit = await this.db.userDeposit.create({
      data: {
        user: { connect: { address: event.user } },
        provider: { connect: { sorobanId: event.provider_id } },
        amount: event.amount.toString(),
        timestamp: new Date(),
      },
    });

    // Notify user
    await this.notificationService.create({
      user: { connect: { address: event.user } },
      type: 'DEPOSIT_CONFIRMED',
      title: 'Yield deposit confirmed',
      body: `Your deposit of ${formatAmount(event.amount)} has been allocated to a yield provider`,
      severity: 'LOW',
    });

    // Trigger APY recalculation for provider
    await this.yieldTracker.updateProviderMetrics(
      `provider_${event.provider_id}`,
    );
  }

  /**
   * Handle yield harvested event
   */
  async handleYieldHarvested(event: YieldHarvestedEvent) {
    // Event: { provider_id, yield_earned }

    const provider = await this.db.yieldProvider.findFirst({
      where: { sorobanId: event.provider_id },
    });

    // Record harvest
    await this.db.yieldHarvest.create({
      data: {
        provider: { connect: { id: provider.id } },
        amount: event.yield_earned.toString(),
        timestamp: new Date(),
      },
    });

    // Queue reward distribution
    await this.distributeRewards(provider.id, event.yield_earned);
  }

  private async distributeRewards(
    providerId: string,
    yieldAmount: bigint,
  ): Promise<void> {
    // Get all users with deposits in this provider
    const deposits = await this.db.userDeposit.groupBy({
      by: ['userId'],
      where: { providerId },
      _sum: { amount: true },
    });

    const totalAmount = deposits.reduce(
      (sum, d) => sum + BigInt(d._sum.amount),
      0n,
    );

    if (totalAmount === 0n) {
      return;
    }

    // Distribute yield proportionally
    for (const userDeposit of deposits) {
      const share = (BigInt(userDeposit._sum.amount) * yieldAmount) / totalAmount;

      await this.db.yieldReward.create({
        data: {
          user: { connect: { id: userDeposit.userId } },
          provider: { connect: { id: providerId } },
          amount: share.toString(),
          timestamp: new Date(),
        },
      });

      // Create notification
      await this.notificationService.create({
        userId: userDeposit.userId,
        type: 'YIELD_EARNED',
        title: 'Yield earned',
        body: `You earned ${formatAmount(share)} in yield`,
        severity: 'LOW',
      });
    }
  }
}
```

### 3. Yield Analytics Service

Generate yield analytics and reports:

```typescript
// packages/backend/src/yield/yield-analytics.service.ts

@Injectable()
export class YieldAnalyticsService {
  constructor(private db: PrismaClient) {}

  /**
   * Get user's yield statement
   */
  async getUserYieldStatement(userId: string, month?: Date): Promise<YieldStatement> {
    const start = month
      ? new Date(month.getFullYear(), month.getMonth(), 1)
      : new Date(new Date().getFullYear(), new Date().getMonth(), 1);

    const end = new Date(start);
    end.setMonth(end.getMonth() + 1);

    const deposits = await this.db.userDeposit.findMany({
      where: {
        userId,
        timestamp: { gte: start, lt: end },
      },
      include: { provider: true },
    });

    const rewards = await this.db.yieldReward.findMany({
      where: {
        userId,
        timestamp: { gte: start, lt: end },
      },
      include: { provider: true },
    });

    const totalDeposited = deposits.reduce(
      (sum, d) => sum + BigInt(d.amount),
      0n,
    );

    const totalEarned = rewards.reduce(
      (sum, r) => sum + BigInt(r.amount),
      0n,
    );

    return {
      month: start.toISOString().substring(0, 7),
      totalDeposited: totalDeposited.toString(),
      totalEarned: totalEarned.toString(),
      apy: this.calculateMonthlyAPY(totalDeposited, totalEarned),
      byProvider: this.groupByProvider(deposits, rewards),
    };
  }

  /**
   * Get comparative provider performance
   */
  async getProviderComparison(days: number = 30): Promise<ProviderComparisonDto[]> {
    const metrics = await this.db.providerMetrics.findMany({
      where: {
        timestamp: {
          gte: new Date(Date.now() - days * 24 * 60 * 60 * 1000),
        },
      },
      include: { provider: true },
      orderBy: { timestamp: 'desc' },
    });

    // Group by provider and calculate averages
    const grouped = new Map<string, typeof metrics>();
    for (const metric of metrics) {
      const key = metric.provider.id;
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key)!.push(metric);
    }

    return Array.from(grouped.entries()).map(([, providerMetrics]) => {
      const avgAPY =
        providerMetrics.reduce((sum, m) => sum + m.apy, 0) /
        providerMetrics.length;
      const avgTVL =
        providerMetrics.reduce(
          (sum, m) => sum + BigInt(m.tvl),
          0n,
        ) / BigInt(providerMetrics.length);

      return {
        providerId: providerMetrics[0].provider.id,
        providerName: providerMetrics[0].provider.name,
        avgAPY,
        avgTVL: avgTVL.toString(),
        volatility: this.calculateVolatility(providerMetrics.map(m => m.apy)),
        riskRating: providerMetrics[0].provider.riskRating,
      };
    });
  }

  private calculateMonthlyAPY(
    deposited: bigint,
    earned: bigint,
  ): number {
    if (deposited === 0n) {
      return 0;
    }

    // APY = (earned / deposited) * 12 * 100
    return (Number(earned) / Number(deposited)) * 12 * 100;
  }

  private groupByProvider(
    deposits: any[],
    rewards: any[],
  ): Record<string, { deposited: string; earned: string }> {
    const result: Record<string, { deposited: string; earned: string }> = {};

    for (const deposit of deposits) {
      if (!result[deposit.provider.id]) {
        result[deposit.provider.id] = {
          deposited: '0',
          earned: '0',
        };
      }
      result[deposit.provider.id].deposited = (
        BigInt(result[deposit.provider.id].deposited) +
        BigInt(deposit.amount)
      ).toString();
    }

    for (const reward of rewards) {
      if (!result[reward.provider.id]) {
        result[reward.provider.id] = {
          deposited: '0',
          earned: '0',
        };
      }
      result[reward.provider.id].earned = (
        BigInt(result[reward.provider.id].earned) +
        BigInt(reward.amount)
      ).toString();
    }

    return result;
  }

  private calculateVolatility(values: number[]): number {
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const variance =
      values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) /
      values.length;
    return Math.sqrt(variance);
  }
}
```

## Database Schema

```prisma
// packages/backend/prisma/schema.prisma

model YieldProvider {
  id              String   @id @default(cuid())
  sorobanId       Int      // Contract-side ID
  name            String
  type            String   // "AAVE" | "STABLE_SWAP" | "LIQUIDITY_POOL"
  contractAddress String   @unique
  priority        Int
  riskRating      Int      @default(5) // 1-10
  isActive        Boolean  @default(true)

  vault           YieldVault
  deposits        UserDeposit[]
  harvests        YieldHarvest[]
  rewards         YieldReward[]
  metrics         ProviderMetrics[]

  createdAt       DateTime @default(now())
  updatedAt       DateTime @updatedAt

  @@index([type])
  @@index([isActive])
}

model YieldVault {
  id              String   @id @default(cuid())
  contractAddress String   @unique
  admin           String
  assetToken      String

  providers       YieldProvider[]
  deposits        UserDeposit[]

  createdAt       DateTime @default(now())
  updatedAt       DateTime @updatedAt
}

model UserDeposit {
  id              String   @id @default(cuid())
  userId          String
  providerId      String
  vaultId         String
  amount          String   // Big number as string

  user            User     @relation(fields: [userId], references: [id])
  provider        YieldProvider @relation(fields: [providerId], references: [id])
  vault           YieldVault @relation(fields: [vaultId], references: [id])

  timestamp       DateTime @default(now())

  @@index([userId])
  @@index([providerId])
  @@index([vaultId])
}

model YieldHarvest {
  id              String   @id @default(cuid())
  providerId      String
  amount          String   // Big number
  transactionHash String?

  provider        YieldProvider @relation(fields: [providerId], references: [id])

  timestamp       DateTime @default(now())

  @@index([providerId])
  @@index([timestamp])
}

model YieldReward {
  id              String   @id @default(cuid())
  userId          String
  providerId      String
  amount          String   // Big number

  user            User     @relation(fields: [userId], references: [id])
  provider        YieldProvider @relation(fields: [providerId], references: [id])

  claimedAt       DateTime?
  timestamp       DateTime @default(now())

  @@index([userId])
  @@index([providerId])
  @@index([claimedAt])
}

model ProviderMetrics {
  id              String   @id @default(cuid())
  providerId      String
  apy             Float    // Annual percentage yield
  tvl             String   // Total value locked
  volume24h       String?  // 24h trading volume

  provider        YieldProvider @relation(fields: [providerId], references: [id])

  timestamp       DateTime @default(now())

  @@index([providerId])
  @@index([timestamp])
  @@unique([providerId, timestamp])
}
```

## Grafana Dashboard Queries

### APY Over Time
```sql
SELECT
  timestamp,
  provider_id,
  apy
FROM provider_metrics
WHERE created_at > now() - interval '90 days'
ORDER BY timestamp DESC;
```

### Provider TVL Comparison
```sql
SELECT
  provider_id,
  timestamp,
  tvl::decimal / 1e18 as tvl_tokens
FROM provider_metrics
WHERE timestamp > now() - interval '30 days'
ORDER BY timestamp DESC;
```

### User Yield Distribution
```sql
SELECT
  DATE_TRUNC('month', timestamp) as month,
  COUNT(DISTINCT user_id) as users,
  SUM(amount::decimal) / 1e18 as total_yield
FROM yield_rewards
WHERE timestamp > now() - interval '1 year'
GROUP BY DATE_TRUNC('month', timestamp)
ORDER BY month DESC;
```

## Testing

```typescript
// test/yield.integration.spec.ts

describe('YieldVault Integration', () => {
  it('should harvest yield from Aave provider', async () => {
    const vault = await deployYieldVault();
    const aave = await deployAaveLendingPool();

    // Register provider
    await vault.register_provider(
      Symbol('Aave'),
      aave.address,
      100,
    );

    // User deposits
    await vault.deposit(toI128(1000), user);

    // Wait for interest accrual
    await advanceTime(24 * 3600); // 1 day

    // Harvest yield
    const harvested = await vault.harvest_yield(0);
    
    expect(harvested).toBeGreaterThan(0);
  });

  it('should calculate correct APY across multiple providers', async () => {
    // ... test setup

    const apy30d = await yieldTracker.calculateAPY(provider1.id, 30);
    const apy7d = await yieldTracker.calculateAPY(provider1.id, 7);

    expect(apy30d).toBeCloseTo(expectedAPY, 1); // Within 1%
  });
});
```

## Deployment Checklist

- [ ] Deploy yield provider contracts (Aave, StableSwap, LiquidityPool)
- [ ] Deploy YieldVault contract
- [ ] Create database tables
- [ ] Deploy YieldTrackerService
- [ ] Deploy YieldEventListener
- [ ] Set up Grafana dashboards
- [ ] Create BullMQ jobs for periodic yield harvests
- [ ] Test end-to-end flow on testnet
- [ ] Configure alerting for provider failures
- [ ] Document APY calculation methodology for users
