import { OutboxMessage } from '../domain/outbox-message.js';
import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { canonicalJson } from '../../wagering/application/process-wager.js';
import type { OutboxClaim, OutboxStore } from './sql-outbox-store.js';

export type OutboxPublication =
  | { readonly eventId: string; readonly status: 'PUBLISHED' }
  | { readonly eventId: string; readonly status: 'RETRY'; readonly nextAttemptAt: Date }
  | { readonly eventId: string; readonly status: 'LOST_LEASE' }
  /** Falha anterior da mesma wallet mantém este evento aguardando, sem envio. */
  | { readonly eventId: string; readonly status: 'DEFERRED'; readonly nextAttemptAt: Date };

export interface OutboxPublisherOptions {
  readonly batchSize?: number;
  readonly retryBaseMs?: number;
  readonly retryMaxMs?: number;
  readonly sendTimeoutMs?: number;
  readonly clock?: () => Date;
}

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Invalid ${name}.`);
}

/** Claim confirma antes do envio. Lease perdido após envio pode causar redelivery; eventId estável e inbox durável impedem novo efeito. */
export class SqsOutboxPublisher {
  /** Lote cheio sinaliza backlog ao loop, evitando pausa artificial. */
  readonly batchSize: number;
  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;
  private readonly clock: () => Date;
  private readonly sendTimeoutMs: number;

  constructor(
    private readonly sqs: SQSClient,
    private readonly queueUrl: string,
    private readonly store: OutboxStore,
    options: OutboxPublisherOptions = {},
  ) {
    if (!queueUrl.trim()) throw new Error('Outbox events queue URL is required.');
    this.batchSize = options.batchSize ?? 10;
    this.retryBaseMs = options.retryBaseMs ?? 1_000;
    this.retryMaxMs = options.retryMaxMs ?? 60_000;
    this.clock = options.clock ?? (() => new Date());
    this.sendTimeoutMs = options.sendTimeoutMs ?? 10000;
    positiveInteger(this.sendTimeoutMs, 'outbox send timeout');
    positiveInteger(this.batchSize, 'outbox batch size');
    positiveInteger(this.retryBaseMs, 'outbox retry base');
    positiveInteger(this.retryMaxMs, 'outbox retry maximum');
    if (this.batchSize > 100 || this.retryBaseMs > this.retryMaxMs) {
      throw new Error('Invalid outbox publisher options.');
    }
  }

  /** Wallets publicam em paralelo; dentro de cada wallet, seq é serial. A primeira falha devolve a cauda sem enviar. */
  async publishBatch(now = this.clock()): Promise<readonly OutboxPublication[]> {
    const claims = await this.store.claimBatch(this.batchSize, now);
    const byWallet = new Map<string, OutboxClaim[]>();
    for (const claim of claims) byWallet.set(claim.walletId, [...(byWallet.get(claim.walletId) ?? []), claim]);
    const results = await Promise.all([...byWallet.values()].map(group => this.publishInOrder(group)));
    return results.flat();
  }

  private async publishInOrder(group: readonly OutboxClaim[]): Promise<OutboxPublication[]> {
    const results: OutboxPublication[] = [];
    for (const [index, claim] of group.entries()) {
      const result = await this.publish(claim);
      results.push(result);
      if (result.status === 'PUBLISHED') continue;
      const nextAttemptAt = result.status === 'RETRY' ? result.nextAttemptAt : this.clock();
      for (const rest of group.slice(index + 1)) {
        results.push(await this.store.release(rest, nextAttemptAt, this.clock())
          ? { eventId: rest.eventId, status: 'DEFERRED', nextAttemptAt }
          : { eventId: rest.eventId, status: 'LOST_LEASE' });
      }
      break;
    }
    return results;
  }

  private async publish(claim: OutboxClaim): Promise<OutboxPublication> {
    const remaining = claim.leaseUntil.getTime() - this.clock().getTime();
    if (remaining <= 0) return { eventId: claim.eventId, status: 'LOST_LEASE' };
    try {
      await this.sqs.send(new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: canonicalJson(claim.envelope),
        MessageGroupId: claim.walletId,
        MessageDeduplicationId: claim.eventId,
      }), { abortSignal: AbortSignal.timeout(Math.min(this.sendTimeoutMs, remaining)) });
    } catch {
      const failedAt = this.clock();
      // O claim SQL e a transição de domínio devem contar a mesma tentativa.
      const message=OutboxMessage.rehydrate({id:claim.eventId,aggregateId:claim.envelope.aggregateId,
        eventType:claim.envelope.eventType,payload:{...claim.envelope},occurredAt:new Date(claim.envelope.occurredAt),attempts:claim.attempts-1});
      message.scheduleRetry(failedAt,{baseMs:this.retryBaseMs,maxMs:this.retryMaxMs});
      const nextAttemptAt=message.nextAttemptAt!;
      return await this.store.reschedule(claim, nextAttemptAt, failedAt)
        ? { eventId: claim.eventId, status: 'RETRY', nextAttemptAt }
        : { eventId: claim.eventId, status: 'LOST_LEASE' };
    }
    return await this.store.markPublished(claim, this.clock())
      ? { eventId: claim.eventId, status: 'PUBLISHED' }
      : { eventId: claim.eventId, status: 'LOST_LEASE' };
  }
}
