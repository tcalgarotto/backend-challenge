import { createHash } from 'node:crypto';
import type { MikroORM } from '@mikro-orm/postgresql';
import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { runSqlTransaction, sqlQuery, sqlTransactionPolicy, type SqlTransactionPolicy } from '../../shared/infrastructure/sql-transaction.js';
import { validateIntegrationEventData } from './validate-event-data.js';
import { canonicalJson } from '../../wagering/application/process-wager.js';
import type { OutboxEnvelope } from './sql-outbox-store.js';

export const EVENT_INBOX_CONSUMER = 'wager-events-demo-consumer';

export class InvalidIntegrationEventError extends Error {
  readonly code = 'INVALID_INTEGRATION_EVENT' as const;
  constructor() {
    super('INVALID_INTEGRATION_EVENT');
    this.name = 'InvalidIntegrationEventError';
  }
}

export class EventInboxConflictError extends Error {
  readonly code = 'EVENT_INBOX_CONFLICT' as const;
  constructor() {
    super('EVENT_INBOX_CONFLICT');
    this.name = 'EventInboxConflictError';
  }
}

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const eventTypes = new Set([
  'WagerTransactionProcessed',
  'WagerTransactionRejected',
  'WagerTransactionFailed',
  'WalletBalanceChanged',
  'WagerTransactionPendingReference',
]);
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

export function parseIntegrationEvent(body: unknown): OutboxEnvelope {
  let value: unknown;
  try {
    value = typeof body === 'string' ? JSON.parse(body) : body;
  } catch {
    throw new InvalidIntegrationEventError();
  }
  const required = ['eventId', 'eventType', 'aggregateId', 'correlationId', 'occurredAt', 'version', 'data'];
  const allowed = new Set([...required, 'causationId']);
  if (
    !record(value) ||
    !required.every(key => Object.hasOwn(value, key)) ||
    Object.keys(value).some(key => !allowed.has(key)) ||
    typeof value['eventId'] !== 'string' ||
    !uuid.test(value['eventId']) ||
    typeof value['eventType'] !== 'string' ||
    !eventTypes.has(value['eventType']) ||
    typeof value['aggregateId'] !== 'string' ||
    !uuid.test(value['aggregateId']) ||
    typeof value['correlationId'] !== 'string' ||
    value['correlationId'].length < 1 ||
    value['correlationId'].length > 128 ||
    (value['causationId'] !== undefined && (
      typeof value['causationId'] !== 'string' ||
      value['causationId'].length < 1 ||
      value['causationId'].length > 128
    )) ||
    typeof value['occurredAt'] !== 'string' ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/.test(value['occurredAt']) ||
    !Number.isFinite(Date.parse(value['occurredAt'])) ||
    value['version'] !== 1 ||
    !record(value['data']) ||
    typeof value['data']['walletId'] !== 'string' ||
    !uuid.test(value['data']['walletId'])
  ) {
    throw new InvalidIntegrationEventError();
  }
  if (!validateIntegrationEventData(value['eventType'] as string, value['data'])) throw new InvalidIntegrationEventError();
  const aggregate = value['eventType'] === 'WalletBalanceChanged' ? value['data']['walletId'] : value['data']['transactionId'];
  if (value['aggregateId'] !== aggregate) throw new InvalidIntegrationEventError();
  return value as unknown as OutboxEnvelope;
}

export class SqlEventDeduplicator {
  private readonly policy: SqlTransactionPolicy;
  constructor(private readonly orm: MikroORM, policy: Partial<SqlTransactionPolicy> = {}) {
    this.policy = sqlTransactionPolicy(policy);
  }

  async accept(event: OutboxEnvelope): Promise<{ readonly duplicate: boolean }> {
    const hash = createHash('sha256').update(canonicalJson(event), 'utf8').digest('hex');
    // INSERT por eventId/hash é idempotente mesmo quando o commit é incerto;
    // repetir a unidade só reencontra a mesma linha durável da inbox.
    return runSqlTransaction(this.orm, this.policy, async em => {
      const inserted = await sqlQuery(
        em,
        `INSERT INTO wager_event_inbox(consumer_name,event_id,envelope_hash)
          VALUES (?,?,?) ON CONFLICT (consumer_name,event_id) DO NOTHING
          RETURNING event_id`,
        [EVENT_INBOX_CONSUMER, event.eventId, hash],
      );
      if (inserted.length === 1) return { duplicate: false };
      const [existing] = await sqlQuery(
        em,
        `SELECT envelope_hash FROM wager_event_inbox
          WHERE consumer_name=? AND event_id=?`,
        [EVENT_INBOX_CONSUMER, event.eventId],
      );
      if (existing?.['envelope_hash'] !== hash) throw new EventInboxConflictError();
      return { duplicate: true };
    }, { readOnly: false });
  }
}

export type EventConsumption =
  | {
      readonly eventId: string;
      readonly disposition: 'ACKNOWLEDGED';
      readonly duplicate: boolean;
      readonly receiveCount: number;
    }
  | {
      readonly eventId?: string;
      readonly disposition: 'PERMANENT' | 'RETRY';
      readonly errorCode: string;
      readonly receiveCount: number;
    };

export interface EventConsumerOptions {
  readonly waitTimeSeconds?: number;
  readonly maxMessages?: number;
}

export class SqsEventConsumer {
  private readonly waitTimeSeconds: number;
  private readonly maxMessages: number;

  constructor(
    private readonly sqs: SQSClient,
    private readonly queueUrl: string,
    private readonly inbox: SqlEventDeduplicator,
    options: EventConsumerOptions = {},
  ) {
    this.waitTimeSeconds = options.waitTimeSeconds ?? 1;
    this.maxMessages = options.maxMessages ?? 10;
    if (
      !queueUrl.trim() ||
      !Number.isSafeInteger(this.waitTimeSeconds) ||
      this.waitTimeSeconds < 0 ||
      this.waitTimeSeconds > 20 ||
      !Number.isSafeInteger(this.maxMessages) ||
      this.maxMessages < 1 ||
      this.maxMessages > 10
    ) {
      throw new Error('Invalid event consumer options.');
    }
  }

  async pollOnce(): Promise<readonly EventConsumption[]> {
    const response = await this.sqs.send(new ReceiveMessageCommand({
      QueueUrl: this.queueUrl,
      MaxNumberOfMessages: this.maxMessages,
      WaitTimeSeconds: this.waitTimeSeconds,
      MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
    }));
    return Promise.all((response.Messages ?? []).map(message => this.consume(message)));
  }

  private async consume(message: Message): Promise<EventConsumption> {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
    let event: OutboxEnvelope | undefined;
    try {
      event = parseIntegrationEvent(message.Body);
      if (message.Attributes?.MessageGroupId !== event.data.walletId) {
        throw new InvalidIntegrationEventError();
      }
      if (!message.ReceiptHandle) throw new InvalidIntegrationEventError();
      const accepted = await this.inbox.accept(event);
      await this.sqs.send(new DeleteMessageCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: message.ReceiptHandle,
      }));
      return {
        eventId: event.eventId,
        disposition: 'ACKNOWLEDGED',
        duplicate: accepted.duplicate,
        receiveCount,
      };
    } catch (error) {
      const errorCode = error instanceof InvalidIntegrationEventError || error instanceof EventInboxConflictError
        ? error.code
        : 'EVENT_CONSUMER_RETRY';
      return {
        ...(event ? { eventId: event.eventId } : {}),
        disposition: errorCode === 'EVENT_CONSUMER_RETRY' ? 'RETRY' : 'PERMANENT',
        errorCode,
        receiveCount,
      };
    }
  }
}
