import type { SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WAGER_INBOX_CONSUMER, type InboxMessageMetadata } from '../messaging/sql-inbox-process-wager-store.js';
import { SqsWagerConsumer, type ConsumerObservation, type SqsWagerConsumerOptions, type WagerMessageProcessor }
  from '../messaging/sqs-wager-consumer.js';
import { SqsOutboxPublisher, type OutboxPublication, type OutboxPublisherOptions } from '../messaging/outbox/sqs-outbox-publisher.js';
import type { OutboxStore } from '../messaging/outbox/sql-outbox-store.js';
import type { EventConsumption } from '../messaging/outbox/sqs-event-consumer.js';
import { SqlPendingReferenceScheduler, type PendingReferenceClaim, type PendingReferenceOutcome,
  type PendingReferenceSchedulerOptions } from '../messaging/pending-reference-scheduler.js';
import type { ProcessWagerResult } from '../wagering/application/process-wager.js';
import { WagerTransactionKind } from '../wagering/domain/wager-transaction.js';
import { failureCode } from './log.js';
import type { Observability } from './observability.js';

const KINDS = new Set<string>(Object.values(WagerTransactionKind));
const field = (input: unknown, key: string): string | undefined => {
  const value = (input as Record<string, unknown> | null | undefined)?.[key];
  return typeof value === 'string' ? value : undefined;
};
/** Kind desconhecido vira unknown para manter o label limitado. */
export function wagerKind(input: unknown): string {
  const kind = field(input, 'kind');
  return kind && KINDS.has(kind) ? kind : 'unknown';
}

/** SQLSTATE vem da cadeia de causas; texto de erro não é interpretado. */
export function sqlFailureReason(error: unknown): string | undefined {
  const reasons: Record<string, string> = { '55P03': 'lock_timeout', '40P01': 'deadlock', '40001': 'serialization',
    '57014': 'statement_timeout', '25P04': 'transaction_timeout', '57P01': 'connection', '08000': 'connection',
    '08003': 'connection', '08006': 'connection', ECONNREFUSED: 'connection', ECONNRESET: 'connection', ETIMEDOUT: 'connection' };
  for (let current = error, depth = 0; current && depth < 8; current = (current as { cause?: unknown }).cause, depth++) {
    const code = (current as { code?: unknown }).code;
    if (typeof code === 'string' && reasons[code]) return reasons[code];
    if ((current as { name?: unknown }).name === 'KnexTimeoutError') return 'pool_timeout';
  }
  return undefined;
}

/** Observa resultado e latência sem ler campos monetários do comando. */
export class ObservedWagerProcessor implements WagerMessageProcessor {
  constructor(private readonly inner: WagerMessageProcessor, private readonly observability: Observability) {}

  async execute(input: unknown, idempotencyKey: unknown, metadata: InboxMessageMetadata): Promise<ProcessWagerResult> {
    const { metrics, logger } = this.observability;
    const started = performance.now();
    const kind = wagerKind(input);
    const identity = { correlationId: metadata.messageId, messageId: metadata.messageId,
      walletId: field(input, 'walletId'), providerId: field(input, 'providerId'), consumer: metadata.consumerName, kind };
    try {
      const result = await this.inner.execute(input, idempotencyKey, metadata);
      const durationMs = performance.now() - started;
      metrics.processing.observe({ channel: 'sqs', outcome: 'completed' }, durationMs / 1000);
      metrics.transactions.inc({ channel: 'sqs', status: result.body.status, kind });
      if (result.body.idempotentReplay) metrics.duplicates.inc({ layer: 'idempotency' });
      logger.info('wager.processed', { ...identity, transactionId: result.body.transactionId, status: result.body.status,
        failureCode: result.body.failureCode, idempotentReplay: result.body.idempotentReplay, durationMs: Math.round(durationMs) });
      return result;
    } catch (error) {
      const durationMs = performance.now() - started;
      metrics.processing.observe({ channel: 'sqs', outcome: 'failed' }, durationMs / 1000);
      const reason = sqlFailureReason(error);
      if (reason) metrics.sqlTransient.inc({ channel: 'sqs', reason });
      logger.warn('wager.failed', { ...identity, errorCode: failureCode(error), reason, durationMs: Math.round(durationMs) });
      throw error;
    }
  }
}

export function recordConsumerObservation(observability: Observability, consumer: string, observation: ConsumerObservation): void {
  const { metrics, logger } = observability;
  metrics.sqsMessages.inc({ consumer, disposition: observation.disposition, error_code: observation.errorCode });
  if (observation.disposition === 'RETRY') metrics.retries.inc({ component: 'sqs_consumer' });
  if (observation.disposition === 'ACKNOWLEDGED' && observation.receiveCount > 1) metrics.duplicates.inc({ layer: 'sqs_redelivery' });
  const level = observation.disposition === 'ACKNOWLEDGED' ? 'info' : observation.disposition === 'RETRY' ? 'warn' : 'error';
  logger.log(level, 'sqs.message', { messageId: observation.messageId, correlationId: observation.messageId, consumer,
    disposition: observation.disposition, receiveCount: observation.receiveCount, errorCode: observation.errorCode });
}

/** O loop usa este pollOnce, incluindo observação dos workers contínuos. */
export class ObservedSqsWagerConsumer extends SqsWagerConsumer {
  private readonly consumerName: string;
  constructor(client: SQSClient, queueUrl: string, processor: WagerMessageProcessor,
    options: Partial<SqsWagerConsumerOptions>, private readonly observability: Observability) {
    super(client, queueUrl, new ObservedWagerProcessor(processor, observability), options);
    this.consumerName = options.consumerName ?? WAGER_INBOX_CONSUMER;
  }
  override async pollOnce(): Promise<ConsumerObservation[]> {
    const observations = await super.pollOnce();
    for (const observation of observations) recordConsumerObservation(this.observability, this.consumerName, observation);
    return observations;
  }
}

export function recordOutboxPublication(observability: Observability, publication: OutboxPublication): void {
  observability.metrics.outboxPublications.inc({ status: publication.status });
  if (publication.status === 'RETRY') observability.metrics.retries.inc({ component: 'outbox_publisher' });
  observability.logger.log(publication.status === 'PUBLISHED' ? 'info' : 'warn', 'outbox.publication',
    { eventId: publication.eventId, status: publication.status });
}

export class ObservedSqsOutboxPublisher extends SqsOutboxPublisher {
  constructor(sqs: SQSClient, queueUrl: string, store: OutboxStore, options: OutboxPublisherOptions,
    private readonly observability: Observability) {
    super(sqs, queueUrl, store, options);
  }
  override async publishBatch(now?: Date): Promise<readonly OutboxPublication[]> {
    const publications = await (now === undefined ? super.publishBatch() : super.publishBatch(now));
    for (const publication of publications) recordOutboxPublication(this.observability, publication);
    return publications;
  }
}

export function recordEventConsumption(observability: Observability, consumption: EventConsumption): void {
  const errorCode = consumption.disposition === 'ACKNOWLEDGED' ? undefined : consumption.errorCode;
  observability.metrics.eventMessages.inc({ disposition: consumption.disposition, error_code: errorCode });
  if (consumption.disposition === 'RETRY') observability.metrics.retries.inc({ component: 'event_consumer' });
  const duplicate = consumption.disposition === 'ACKNOWLEDGED' && consumption.duplicate;
  if (duplicate) observability.metrics.duplicates.inc({ layer: 'event_inbox' });
  observability.logger.log(consumption.disposition === 'ACKNOWLEDGED' ? 'info' : 'warn', 'event.message',
    { eventId: consumption.eventId, disposition: consumption.disposition, receiveCount: consumption.receiveCount,
      errorCode, ...(consumption.disposition === 'ACKNOWLEDGED' ? { duplicate } : {}) });
}

export function recordPendingReference(observability: Observability, outcome: PendingReferenceOutcome): void {
  if (outcome.status === 'IDLE') return;
  const failure = outcome.status === 'REJECTED' || outcome.status === 'FAILED' ? outcome.failureCode : undefined;
  observability.metrics.pendingReferences.inc({ status: outcome.status, failure_code: failure });
  if (outcome.status === 'RESCHEDULED') observability.metrics.retries.inc({ component: 'pending_reference' });
  observability.logger.info('pending_reference.outcome', { transactionId: outcome.transactionId, status: outcome.status, failureCode: failure });
}

/** Ambas as entradas passam por processClaim e são observadas uma única vez. */
export class ObservedPendingReferenceScheduler extends SqlPendingReferenceScheduler {
  constructor(orm: MikroORM, options: Partial<PendingReferenceSchedulerOptions>, private readonly observability: Observability) {
    super(orm, options);
  }
  override async processClaim(claim: PendingReferenceClaim, at?: Date): Promise<PendingReferenceOutcome> {
    const outcome = await (at === undefined ? super.processClaim(claim) : super.processClaim(claim, at));
    recordPendingReference(this.observability, outcome);
    return outcome;
  }
}
