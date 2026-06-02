import { Injectable } from '@nestjs/common';
import { PrismaClient, Prisma } from '@prisma/client';
import { BullQueue } from '@nestjs/bull';
import { Queue, Job } from 'bull';
import { SorobanClient } from '../soroban/soroban.client';

interface CrossContractNotification {
  id: string;
  sourceContract: string;
  eventType: string;
  sourceData?: Buffer;
  notifiedCount: number;
  status: 'RECEIVED' | 'PROCESSING' | 'PROCESSED' | 'FAILED';
  createdAt: Date;
}

interface ContractSubscription {
  listener: string;
  source: string;
  eventType: string | null;
  action: 'subscribe' | 'unsubscribe';
  timestamp: Date;
}

/**
 * Handles cross-contract event notifications
 * Listens to NotificationBroker events and routes them to handlers
 */
@Injectable()
export class CrossContractNotificationService {
  private notificationQueue: Queue;

  constructor(
    private db: PrismaClient,
    private sorobanClient: SorobanClient,
    @BullQueue('cross-contract-notifications')
    notificationQueue: Queue,
  ) {
    this.notificationQueue = notificationQueue;
  }

  /**
   * Record a notification emission event
   */
  async recordNotificationEmitted(
    sourceContract: string,
    eventType: string,
    notifiedCount: number,
    sourceData?: Buffer,
  ): Promise<CrossContractNotification> {
    const notification = await this.db.crossContractNotification.create({
      data: {
        sourceContract,
        eventType,
        notifiedCount,
        sourceData,
        status: 'RECEIVED',
      },
    });

    // Queue for processing
    await this.notificationQueue.add(
      'process-notification',
      { notificationId: notification.id },
      {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
      },
    );

    return notification;
  }

  /**
   * Record subscription/unsubscription event
   */
  async recordSubscriptionChange(
    listener: string,
    source: string,
    eventType: string | null,
    action: 'subscribe' | 'unsubscribe',
  ): Promise<void> {
    const uniqueKey = {
      listener,
      source,
      eventType,
    };

    if (action === 'subscribe') {
      await this.db.contractSubscription.upsert({
        where: {
          listener_source_eventType: uniqueKey,
        },
        create: {
          listener,
          source,
          eventType,
          action,
          timestamp: new Date(),
        },
        update: {
          action,
          timestamp: new Date(),
        },
      });
    } else {
      await this.db.contractSubscription.deleteMany({
        where: uniqueKey,
      });
    }
  }

  /**
   * Get all subscriptions for a contract
   */
  async getSubscriptionsFor(
    contract: string,
  ): Promise<ContractSubscription[]> {
    return this.db.contractSubscription.findMany({
      where: { listener: contract },
    });
  }

  /**
   * Get all listeners for a source contract
   */
  async getListenersFor(source: string): Promise<string[]> {
    const subscriptions = await this.db.contractSubscription.findMany({
      where: { source },
      distinct: ['listener'],
      select: { listener: true },
    });

    return subscriptions.map((s) => s.listener);
  }

  /**
   * Get notification metrics
   */
  async getMetrics(days: number = 30): Promise<any> {
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const [totalNotifications, byEventType, bySource, avgNotifiedCount] =
      await Promise.all([
        this.db.crossContractNotification.count({
          where: { createdAt: { gte: cutoff } },
        }),

        this.db.crossContractNotification.groupBy({
          by: ['eventType'],
          where: { createdAt: { gte: cutoff } },
          _count: { id: true },
          orderBy: { _count: { id: 'desc' } },
        }),

        this.db.crossContractNotification.groupBy({
          by: ['sourceContract'],
          where: { createdAt: { gte: cutoff } },
          _count: { id: true },
          orderBy: { _count: { id: 'desc' } },
        }),

        this.db.crossContractNotification.aggregate({
          where: { createdAt: { gte: cutoff } },
          _avg: { notifiedCount: true },
        }),
      ]);

    return {
      totalNotifications,
      byEventType: byEventType.map((item) => ({
        eventType: item.eventType,
        count: item._count.id,
      })),
      bySource: bySource.map((item) => ({
        sourceContract: item.sourceContract,
        count: item._count.id,
      })),
      avgNotifiedCount: avgNotifiedCount._avg.notifiedCount || 0,
    };
  }
}

/**
 * Processor for cross-contract notifications
 */
@Injectable()
export class CrossContractNotificationProcessor {
  constructor(
    private db: PrismaClient,
    private notificationService: CrossContractNotificationService,
  ) {}

  async processNotification(job: Job<{ notificationId: string }>): Promise<void> {
    const notification = await this.db.crossContractNotification.findUnique({
      where: { id: job.data.notificationId },
    });

    if (!notification) {
      throw new Error(
        `Notification ${job.data.notificationId} not found`,
      );
    }

    try {
      // Mark as processing
      await this.db.crossContractNotification.update({
        where: { id: notification.id },
        data: { status: 'PROCESSING' },
      });

      // Route to appropriate handler
      switch (notification.eventType) {
        case 'MilestoneApproved':
          await this.handleMilestoneApproved(notification);
          break;
        case 'DepositReceived':
          await this.handleDepositReceived(notification);
          break;
        case 'RefundProcessed':
          await this.handleRefundProcessed(notification);
          break;
        case 'WithdrawProcessed':
          await this.handleWithdrawProcessed(notification);
          break;
        default:
          console.log(
            `[CrossContractNotification] Unhandled event: ${notification.eventType}`,
          );
      }

      // Mark as processed
      await this.db.crossContractNotification.update({
        where: { id: notification.id },
        data: { status: 'PROCESSED' },
      });
    } catch (error) {
      console.error(
        `[CrossContractNotification] Error processing notification:`,
        error,
      );

      await this.db.crossContractNotification.update({
        where: { id: notification.id },
        data: { status: 'FAILED' },
      });

      throw error;
    }
  }

  private async handleMilestoneApproved(
    notification: CrossContractNotification,
  ): Promise<void> {
    // This would typically:
    // 1. Parse the XDR data from notification.sourceData
    // 2. Fetch milestone details from DB
    // 3. Create user notifications
    // 4. Update project status
    // 5. Trigger reward calculations

    console.log(
      `[MilestoneApproved] Processing notification from ${notification.sourceContract}`,
    );
    // Implementation would go here
  }

  private async handleDepositReceived(
    notification: CrossContractNotification,
  ): Promise<void> {
    // This would typically:
    // 1. Parse deposit amount and user from data
    // 2. Update user portfolio
    // 3. Create deposit record
    // 4. Send confirmation notification

    console.log(
      `[DepositReceived] Processing deposit from ${notification.sourceContract}`,
    );
    // Implementation would go here
  }

  private async handleRefundProcessed(
    notification: CrossContractNotification,
  ): Promise<void> {
    // This would typically:
    // 1. Mark project as refundable
    // 2. Create refund records
    // 3. Send notification to affected users

    console.log(
      `[RefundProcessed] Processing refund from ${notification.sourceContract}`,
    );
    // Implementation would go here
  }

  private async handleWithdrawProcessed(
    notification: CrossContractNotification,
  ): Promise<void> {
    // Similar to refund processing

    console.log(
      `[WithdrawProcessed] Processing withdrawal from ${notification.sourceContract}`,
    );
    // Implementation would go here
  }
}
