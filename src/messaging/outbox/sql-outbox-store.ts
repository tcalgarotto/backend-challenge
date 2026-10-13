import { OutboxMessage } from '../domain/outbox-message.js';
import { randomUUID } from 'node:crypto';
import { IsolationLevel } from '@mikro-orm/core';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { sqlQuery } from '../../shared/infrastructure/sql-transaction.js';

export interface OutboxEnvelope {
  readonly eventId: string;
  readonly eventType: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: string;
  readonly version: number;
  readonly data: Readonly<Record<string, unknown>> & { readonly walletId: string };
}

export interface OutboxClaim {
  readonly eventId: string;
  readonly walletId: string;
  readonly token: string;
  readonly leaseUntil: Date;
  readonly attempts: number;
  readonly envelope: OutboxEnvelope;
}

interface ClaimRow extends Record<string, unknown> {
  event_id: string;
  wallet_id: string;
  claim_token: string;
  lease_until: Date | string;
  attempts: number;
  envelope: unknown;
}

export interface OutboxStore {
  claimBatch(limit: number, now?: Date): Promise<readonly OutboxClaim[]>;
  markPublished(claim: OutboxClaim, publishedAt?: Date): Promise<boolean>;
  reschedule(claim: OutboxClaim, nextAttemptAt: Date, failedAt?: Date): Promise<boolean>;
  /** Devolve eventos não enviados após falha anterior na mesma wallet. */
  release(claim: OutboxClaim, nextAttemptAt: Date, at?: Date): Promise<boolean>;
}

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

function date(value: Date | string): Date {
  const parsed = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(parsed.getTime())) throw new Error('Invalid outbox lease timestamp.');
  return parsed;
}

function envelope(value: unknown, eventId: string, walletId: string): OutboxEnvelope {
  if (
    !record(value) ||
    value['eventId'] !== eventId ||
    typeof value['eventType'] !== 'string' ||
    !uuid.test(String(value['aggregateId'])) ||
    typeof value['correlationId'] !== 'string' ||
    typeof value['occurredAt'] !== 'string' ||
    value['version'] !== 1 ||
    !record(value['data']) ||
    value['data']['walletId'] !== walletId
  ) {
    throw new Error('Invalid persisted outbox envelope.');
  }
  return value as unknown as OutboxEnvelope;
}

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Invalid ${name}.`);
}

/** O claim confirma o lease em TX curta; chamadas SQS não seguram transação SQL. */
export class SqlOutboxStore implements OutboxStore {
  constructor(
    private readonly orm: MikroORM,
    private readonly leaseMs = 30_000,
  ) {
    positiveInteger(leaseMs, 'outbox lease');
  }

  /** SKIP LOCKED na cabeça torna o claim exclusivo por wallet. O prefixo contíguo em seq impede ultrapassagens. */
  async claimBatch(limit: number, now = new Date()): Promise<readonly OutboxClaim[]> {
    positiveInteger(limit, 'outbox batch size');
    if (limit > 100 || !Number.isFinite(now.getTime())) throw new Error('Invalid outbox claim parameters.');
    const token = randomUUID();
    const leaseUntil = new Date(now.getTime() + this.leaseMs);
    return this.transaction(async em => {
      const heads = await sqlQuery(
        em,
        `SELECT head.wallet_id FROM outbox_events head
          WHERE head.published_at IS NULL
            AND head.next_attempt_at<=?
            AND (head.claim_token IS NULL OR head.lease_until<=?)
            AND NOT EXISTS (SELECT 1 FROM outbox_events earlier
              WHERE earlier.wallet_id=head.wallet_id AND earlier.published_at IS NULL AND earlier.seq<head.seq)
          ORDER BY head.next_attempt_at,head.seq
          FOR UPDATE SKIP LOCKED
          LIMIT ?`,
        [now, now, limit],
      );
      if (heads.length === 0) return [];
      const walletIds = heads.map(head => head['wallet_id'] as string);
      const prefixes = await sqlQuery(
        em,
        `SELECT pending.event_id,pending.wallet_id,pending.claim_token,pending.lease_until
          FROM unnest(?::uuid[]) AS wallet(wallet_id)
          CROSS JOIN LATERAL (
            SELECT event_id,wallet_id,claim_token,lease_until,seq FROM outbox_events
            WHERE wallet_id=wallet.wallet_id AND published_at IS NULL
            ORDER BY seq LIMIT ? FOR UPDATE
          ) pending
          ORDER BY pending.wallet_id,pending.seq`,
        [`{${walletIds.join(',')}}`, limit],
      );
      // O orçamento restante estende os prefixos na ordem das cabeças;
      // um lease de outro publisher interrompe o prefixo.
      const byWallet = new Map<string, string[]>(walletIds.map(id => [id, []]));
      const stopped = new Set<string>();
      for (const row of prefixes) {
        const walletId = row['wallet_id'] as string;
        if (stopped.has(walletId)) continue;
        if (row['claim_token'] !== null && date(row['lease_until'] as Date | string) > now) { stopped.add(walletId); continue; }
        byWallet.get(walletId)!.push(row['event_id'] as string);
      }
      let budget = limit;
      const chosen: string[] = [];
      for (const prefix of byWallet.values()) if (prefix.length && budget > 0) { chosen.push(prefix[0]!); budget--; }
      for (const prefix of byWallet.values()) {
        for (const eventId of prefix.slice(1)) { if (budget <= 0) break; chosen.push(eventId); budget--; }
      }
      if (chosen.length === 0) return [];
      const rows = await sqlQuery(
        em,
        `UPDATE outbox_events AS event
          SET claim_token=?,lease_until=?,attempts=event.attempts+1
          WHERE event.event_id=ANY(?::uuid[])
          RETURNING event.event_id,event.wallet_id,event.claim_token,event.lease_until,event.attempts,event.envelope,event.seq`,
        [token, leaseUntil, `{${chosen.join(',')}}`],
      ) as (ClaimRow & { seq: string | number })[];
      rows.sort((a, b) => a.wallet_id.localeCompare(b.wallet_id) || Number(BigInt(a.seq) - BigInt(b.seq)));
      return rows.map(row => ({
        eventId: row.event_id,
        walletId: row.wallet_id,
        token: row.claim_token,
        leaseUntil: date(row.lease_until),
        attempts: Number(row.attempts),
        envelope: envelope(row.envelope, row.event_id, row.wallet_id),
      }));
    });
  }

  async markPublished(claim: OutboxClaim, publishedAt = new Date()): Promise<boolean> {
    const message=OutboxMessage.rehydrate({id:claim.eventId,aggregateId:claim.envelope.aggregateId,
      eventType:claim.envelope.eventType,payload:{...claim.envelope},occurredAt:new Date(claim.envelope.occurredAt),attempts:claim.attempts});
    message.markPublished(publishedAt);
    return this.transaction(async em => {
      const rows = await sqlQuery(
        em,
        `UPDATE outbox_events
          SET published_at=?,claim_token=NULL,lease_until=NULL
          WHERE event_id=? AND claim_token=? AND published_at IS NULL
            AND lease_until>greatest(?::timestamptz,clock_timestamp())
          RETURNING event_id`,
        [publishedAt, claim.eventId, claim.token, publishedAt],
      );
      return rows.length === 1;
    });
  }

  async reschedule(claim: OutboxClaim, nextAttemptAt: Date, failedAt = new Date()): Promise<boolean> {
    if (!Number.isFinite(nextAttemptAt.getTime()) || !Number.isFinite(failedAt.getTime())) throw new Error('Invalid retry timestamp.');
    return this.transaction(async em => {
      const rows = await sqlQuery(
        em,
        `UPDATE outbox_events
          SET next_attempt_at=?,claim_token=NULL,lease_until=NULL
          WHERE event_id=? AND claim_token=? AND published_at IS NULL
            AND lease_until>greatest(?::timestamptz,clock_timestamp())
          RETURNING event_id`,
        [nextAttemptAt, claim.eventId, claim.token, failedAt],
      );
      return rows.length === 1;
    });
  }

  async release(claim: OutboxClaim, nextAttemptAt: Date, at = new Date()): Promise<boolean> {
    if (!Number.isFinite(nextAttemptAt.getTime()) || !Number.isFinite(at.getTime())) throw new Error('Invalid release timestamp.');
    return this.transaction(async em => {
      const rows = await sqlQuery(
        em,
        `UPDATE outbox_events
          SET next_attempt_at=?,claim_token=NULL,lease_until=NULL,attempts=greatest(attempts-1,0)
          WHERE event_id=? AND claim_token=? AND published_at IS NULL
            AND lease_until>greatest(?::timestamptz,clock_timestamp())
          RETURNING event_id`,
        [nextAttemptAt, claim.eventId, claim.token, at],
      );
      return rows.length === 1;
    });
  }

  private async transaction<T>(work: (em: EntityManager) => Promise<T>): Promise<T> {
    return this.orm.em.fork().transactional(async em => {
      await sqlQuery(
        em,
        `SELECT set_config('lock_timeout','5000',true),
          set_config('statement_timeout','10000',true),
          set_config('transaction_timeout','15000',true)`,
      );
      await sqlQuery(em, 'SET LOCAL ROLE jwp_runtime');
      return work(em);
    }, { isolationLevel: IsolationLevel.READ_COMMITTED });
  }
}
