# Cross-Contract Event Notification System - Backend Integration Guide

## Overview

The NotificationBroker contract enables decoupled cross-contract communication on Soroban. This guide shows how to integrate it with the backend for monitoring and reacting to notifications.

## Architecture

```
Smart Contracts (Soroban)
    ↓
NotificationBroker Registry
    ↓ Subscribes / Receives Notifications
    ↓
Backend Event Listener (BullMQ)
    ↓
Notification Delivery Service
    ↓
Multi-channel Delivery (Webhook, Email, In-App, Push)
```

## How It Works

### 1. Contract-Side Subscription

Contracts can subscribe to events from other contracts:

```typescript
// Example: VestingWallet listening to CrowdfundVault milestones
const notificationBroker = new NotificationBrokerClient(env, brokerAddress);

// Subscribe to all events from vault
await notificationBroker.subscribe(
  vestingWalletAddress,  // listener
  vaultAddress,          // source
  null                   // event_type: null = all events
);

// Subscribe to specific event type
await notificationBroker.subscribe(
  vestingWalletAddress,
  vaultAddress,
  Symbol.new(env, "MilestoneApproved")
);
```

### 2. Contract Emitting Notification

When a contract wants to notify listeners:

```rust
// In vault contract
let notification = Notification {
    source: vault_address,
    event_type: Symbol::new(env, "MilestoneApproved"),
    data: milestone_data.to_xdr(&env),
};

let broker_client = NotificationBrokerClient::new(env, broker_address);
let notified_count = broker_client.notify(notification)?;

// Broker will call on_notify() on all subscribed contracts
```

### 3. Listener Contract Handling

Contracts implement NotificationReceiverTrait:

```rust
#[contractimpl]
pub fn on_notify(env: Env, notification: Notification) {
    match notification.event_type {
        Symbol("MilestoneApproved") => {
            // Parse data and update state
            let milestone_data = MilestoneData::from_xdr(&env, &notification.data);
            // Handle milestone approval logic
        },
        _ => {}
    }
}
```

## Backend Integration

### 1. Event Listener Service

Listen to NotificationBroker events and store them:

```typescript
// packages/backend/src/soroban-events/soroban-events.listener.ts

@Injectable()
export class NotificationBrokerListener {
  constructor(
    private db: PrismaClient,
    private eventQueue: BullQueue,
  ) {}

  async handleNotificationEmitted(event: NotificationEmittedEvent) {
    // Event: { source, event_type, notified_count }
    
    const notification = await this.db.crossContractNotification.create({
      data: {
        sourceContract: event.source,
        eventType: event.event_type.toString(),
        notifiedCount: event.notified_count,
        timestamp: new Date(),
        status: NotificationStatus.RECEIVED,
      },
    });

    // Queue for processing
    await this.eventQueue.add('process-notification', {
      notificationId: notification.id,
    });
  }

  async handleSubscriptionEvent(event: SubscriptionEvent) {
    // Track all subscriptions for analytics
    const subscription = await this.db.contractSubscription.upsert({
      where: {
        listener_source_eventType: {
          listener: event.listener,
          source: event.source,
          eventType: event.event_type?.toString() || null,
        },
      },
      update: {
        action: event.action.toString(),
        updatedAt: new Date(),
      },
      create: {
        listener: event.listener,
        source: event.source,
        eventType: event.event_type?.toString(),
        action: event.action.toString(),
        timestamp: new Date(),
      },
    });
  }
}
```

### 2. Notification Processing Service

Process cross-contract notifications:

```typescript
// packages/backend/src/notifications/cross-contract-notification.processor.ts

@Processor('process-notification')
export class CrossContractNotificationProcessor {
  constructor(
    private db: PrismaClient,
    private notificationService: NotificationService,
  ) {}

  @Process()
  async processNotification(job: Job<{ notificationId: string }>) {
    const notification = await this.db.crossContractNotification.findUnique({
      where: { id: job.data.notificationId },
    });

    try {
      // Route to appropriate handler based on event type
      switch (notification.eventType) {
        case 'MilestoneApproved':
          await this.handleMilestoneApproved(notification);
          break;
        case 'RefundProcessed':
          await this.handleRefundProcessed(notification);
          break;
        case 'DepositReceived':
          await this.handleDepositReceived(notification);
          break;
        default:
          // Generic event handling
          await this.handleGenericEvent(notification);
      }

      await this.db.crossContractNotification.update({
        where: { id: notification.id },
        data: { status: NotificationStatus.PROCESSED },
      });
    } catch (error) {
      await this.eventQueue.add('process-notification-retry', {
        notificationId: notification.id,
        error: error.message,
      });

      throw error; // Retry by queue
    }
  }

  private async handleMilestoneApproved(notification: CrossContractNotification) {
    // Parse XDR data
    const data = parseNotificationData(notification.sourceData);

    // Create user notification
    const milestone = await this.db.projectMilestone.findUnique({
      where: { id: data.milestoneId },
      include: { project: { include: { contributors: true } } },
    });

    for (const contributor of milestone.project.contributors) {
      await this.notificationService.create({
        userId: contributor.userId,
        type: 'MILESTONE_APPROVED',
        title: `Milestone approved: ${milestone.title}`,
        body: `Funds will be released to ${milestone.project.name}`,
        severity: 'HIGH',
        metadata: {
          projectId: milestone.project.id,
          milestoneId: milestone.id,
          sourceContract: notification.sourceContract,
        },
      });
    }
  }

  private async handleRefundProcessed(notification: CrossContractNotification) {
    const data = parseNotificationData(notification.sourceData);
    
    // Notify user that refund was processed
    await this.notificationService.create({
      userId: data.userId,
      type: 'REFUND_PROCESSED',
      title: 'Your refund has been processed',
      body: `${data.amount} ${data.tokenSymbol} refunded to your wallet`,
      severity: 'MEDIUM',
      metadata: {
        projectId: data.projectId,
        transactionHash: data.txHash,
      },
    });
  }

  private async handleDepositReceived(notification: CrossContractNotification) {
    const data = parseNotificationData(notification.sourceData);

    // Update portfolio tracking
    await this.db.userPortfolio.update({
      where: { userId: data.userId },
      data: {
        totalDeposited: { increment: data.amount },
        lastDepositAt: new Date(),
      },
    });

    // Create in-app notification
    await this.notificationService.create({
      userId: data.userId,
      type: 'DEPOSIT_CONFIRMED',
      title: 'Deposit confirmed',
      body: `Your deposit of ${data.amount} has been confirmed`,
      severity: 'LOW',
    });
  }

  private async handleGenericEvent(notification: CrossContractNotification) {
    // Log for analysis
    console.log('[CrossContractNotification] Unhandled event type:', {
      type: notification.eventType,
      source: notification.sourceContract,
      count: notification.notifiedCount,
    });
  }
}
```

### 3. Database Schema

```prisma
// packages/backend/prisma/schema.prisma

model CrossContractNotification {
  id                String                 @id @default(cuid())
  sourceContract    String                 // Contract address
  eventType         String                 // e.g., "MilestoneApproved"
  sourceData        Bytes?                 // XDR encoded data
  notifiedCount     Int
  status            NotificationStatus     @default(RECEIVED)
  
  createdAt         DateTime               @default(now())
  updatedAt         DateTime               @updatedAt
  
  // Relationships
  subscriptions     ContractSubscription[]

  @@index([eventType])
  @@index([sourceContract])
  @@index([status])
  @@index([createdAt])
}

model ContractSubscription {
  id                String                 @id @default(cuid())
  listener          String                 // Contract address
  source            String                 // Contract address
  eventType         String?                // Specific event or null for all
  action            String                 // "subscribe" or "unsubscribe"
  timestamp         DateTime               @default(now())

  notification      CrossContractNotification? @relation(fields: [notificationId], references: [id])
  notificationId    String?

  @@unique([listener, source, eventType])
  @@index([listener])
  @@index([source])
}

enum NotificationStatus {
  RECEIVED
  PROCESSING
  PROCESSED
  FAILED
  RETRYING
}
```

## Usage Examples

### Example 1: Vault Notifying Pool About Milestone

```typescript
// Contract side: crowdfund_vault
const notification = Notification {
    source: vault_address,
    event_type: Symbol::new(env, "MilestoneApproved"),
    data: /* encoded milestone data */,
};

broker.notify(notification); // Notifies all listening matching pools

// Backend: automatically processes and creates user notifications
```

### Example 2: Portfolio Tracking via Notifications

```typescript
@Injectable()
export class PortfolioService {
  async subscribeToVaultNotifications(userId: string) {
    const broker = new NotificationBrokerClient(env, brokerAddress);
    
    // Subscribe portfolio tracker to all vault events
    await broker.subscribe(
      portfolioTrackerAddress,
      vaultAddress,
      null // all events
    );
  }

  async handleVaultNotification(notification: Notification) {
    // Update user's portfolio in real-time
    const portfolio = await this.db.userPortfolio.findUnique({
      where: { userId },
    });

    switch (notification.eventType) {
      case 'DepositReceived':
        portfolio.totalDeposited += amount;
        break;
      case 'WithdrawProcessed':
        portfolio.totalWithdrawn += amount;
        break;
    }
  }
}
```

## Monitoring & Analytics

### Metrics to Track

1. **Subscription Graph**: Which contracts listen to which
2. **Event Frequency**: Notifications per source per time period
3. **Latency**: Time from emission to processing
4. **Reliability**: Delivery success rate
5. **Routing**: How many contracts receive each event

### Grafana Dashboard Query

```sql
SELECT
  source_contract,
  event_type,
  COUNT(*) as event_count,
  AVG(EXTRACT(EPOCH FROM (processed_at - created_at))) as avg_latency_seconds
FROM cross_contract_notifications
WHERE created_at > now() - interval '24 hours'
GROUP BY source_contract, event_type
ORDER BY event_count DESC;
```

## Best Practices

1. **Unsubscribe when done**: Contracts should unsubscribe to avoid unnecessary calls
2. **Handle errors gracefully**: Listener failures shouldn't affect other subscribers
3. **Limit event data**: Keep notification data small (< 1KB recommended)
4. **Use specific event types**: Subscribe to specific events, not all events, when possible
5. **Log all notifications**: For debugging and audit trails
6. **Rate limit**: Prevent notification storms (e.g., max 1000/second per contract)

## Troubleshooting

- **Notification not delivered**: Check subscription status in `contract_subscriptions` table
- **High latency**: Check BullMQ queue depth and worker count
- **Lost events**: Enable dead-letter queue in BullMQ configuration
- **Memory leak**: Ensure listeners call `unsubscribe()` when no longer needed

## Next Steps

1. Deploy NotificationBroker contract to testnet
2. Update CrowdfundVault and MatchingPool to emit notifications
3. Deploy backend listeners
4. Test end-to-end notification flow
5. Set up monitoring dashboards
