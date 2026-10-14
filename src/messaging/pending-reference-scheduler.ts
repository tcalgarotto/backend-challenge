import { persistWagerEffects } from '../wagering/infrastructure/persist-wager-effects.js';
import { appliedReversalKinds } from '../wagering/infrastructure/sql-reversal-guard.js';
import { reversalPolicy } from '../wagering/domain/reversal-policy.js';
import { WalletRecord, walletRecordRow, WagerTransactionLockRecord } from '../persistence/financial-orm.js';
import { isKnownPermanentWagerFailure, permanentWagerDiagnostic } from '../shared/infrastructure/permanent-wager-failure.js';
import { randomUUID } from 'node:crypto';
import { IsolationLevel, LockMode } from '@mikro-orm/core';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Money } from '../shared/domain/money.js';
import { sqlQuery } from '../shared/infrastructure/sql-transaction.js';
import { Wallet, type WalletMovement } from '../wallet/domain/wallet.js';
import { ProcessWager, type ProcessWagerResult } from '../wagering/application/process-wager.js';
import type { FailureCode, RejectionCode, WagerFailureDiagnostic } from '../wagering/domain/wager-error.js';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../wagering/domain/wager-transaction.js';

type Row = Record<string, unknown>;
class PendingReferenceLeaseLost extends Error {}

interface StoredSnapshot extends ProcessWagerResult {
  readonly pendingReferenceAt?: string;
  readonly referenceExpiresAt?: string;
}

export interface PendingReferenceClaim {
  readonly transactionId: string;
  readonly token: string;
  readonly attempts: number;
  readonly expiresAt: Date;
  readonly leaseUntil: Date;
}

export type PendingReferenceOutcome =
  | { readonly status: 'IDLE' }
  | { readonly status: 'LOST_LEASE'; readonly transactionId: string }
  | { readonly status: 'STALE'; readonly transactionId: string }
  | { readonly status: 'RESCHEDULED'; readonly transactionId: string; readonly nextAttemptAt: Date }
  | { readonly status: 'PROCESSED'; readonly transactionId: string }
  | { readonly status: 'FAILED'; readonly transactionId: string; readonly failureCode: 'PERMANENT_INFRASTRUCTURE_FAILURE' }
  | { readonly status: 'REJECTED'; readonly transactionId: string; readonly failureCode: RejectionCode };

export interface PendingReferenceSchedulerOptions {
  readonly leaseMs: number;
  readonly baseBackoffMs: number;
  readonly maxBackoffMs: number;
  readonly jitter: () => number;
  readonly permanentFailureAttempts: number;
}

export const defaultPendingReferenceSchedulerOptions: PendingReferenceSchedulerOptions = Object.freeze({
  leaseMs: 30_000,
  baseBackoffMs: 1_000,
  maxBackoffMs: 60_000,
  jitter: Math.random,
  permanentFailureAttempts: 3,
});

const validDate = (value: Date): void => {
  if (!(value instanceof Date) || !Number.isFinite(value.getTime())) throw new Error('Invalid scheduler time.');
};

const date = (value: unknown): Date => {
  const parsed = value instanceof Date ? new Date(value) : new Date(value as string);
  validDate(parsed);
  return parsed;
};

const rehydrate = (row: Row): WagerTransaction => {
  const snapshot = row['response_snapshot'] as StoredSnapshot | null;
  return WagerTransaction.rehydrate({
    id: row['id'] as string,
    walletId: row['wallet_id'] as string,
    playerId: row['player_id'] as string,
    providerId: row['provider_id'] as string,
    externalTransactionId: row['external_transaction_id'] as string,
    idempotencyKey: row['idempotency_key'] as string,
    payloadHash: row['payload_hash'] as string,
    roundId: row['round_id'] as string,
    gameId: row['game_id'] as string,
    kind: row['kind'] as WagerTransactionKind,
    status: row['status'] as WagerTransactionStatus,
    money: Money.from({ amount: row['amount'] as string, currency: row['currency'] as string }),
    createdAt: date(row['created_at']),
    ...(row['reference_external_transaction_id'] === null
      ? {}
      : { referenceExternalTransactionId: row['reference_external_transaction_id'] as string }),
    ...(row['reference_transaction_id'] === null
      ? {}
      : { referenceTransactionId: row['reference_transaction_id'] as string }),
    ...(row['failure_code'] === null ? {} : { failureCode: row['failure_code'] as FailureCode }),
    ...(row['processed_at'] === null ? {} : { processedAt: date(row['processed_at']) }),
    ...(snapshot?.pendingReferenceAt
      ? {
          pendingReferenceAt: date(snapshot.pendingReferenceAt),
          referenceExpiresAt: date(snapshot.referenceExpiresAt),
        }
      : {}),
  });
};

export class SqlPendingReferenceScheduler {
  private readonly options: PendingReferenceSchedulerOptions;

  constructor(
    private readonly orm: MikroORM,
    options: Partial<PendingReferenceSchedulerOptions> = {},
  ) {
    const configured = { ...defaultPendingReferenceSchedulerOptions, ...options };
    if (
      !Number.isSafeInteger(configured.permanentFailureAttempts) || configured.permanentFailureAttempts<1 || configured.permanentFailureAttempts>10 ||
      !Number.isSafeInteger(configured.leaseMs) ||
      !Number.isSafeInteger(configured.baseBackoffMs) ||
      !Number.isSafeInteger(configured.maxBackoffMs) ||
      configured.leaseMs < 1 ||
      configured.baseBackoffMs < 1 ||
      configured.maxBackoffMs < configured.baseBackoffMs ||
      configured.maxBackoffMs > 60_000 ||
      typeof configured.jitter !== 'function'
    ) {
      throw new Error('Invalid pending-reference scheduler options.');
    }
    this.options = Object.freeze(configured);
  }

  async runOnce(at?: Date): Promise<PendingReferenceOutcome> {
    const claim = await this.claimOne(at);
    return claim ? this.processClaim(claim, at) : { status: 'IDLE' };
  }

  async claimOne(at = new Date()): Promise<PendingReferenceClaim | null> {
    validDate(at);
    const token = randomUUID();
    const leaseUntil = new Date(at.getTime() + this.options.leaseMs);
    return this.transaction(async em => {
      const rows = await sqlQuery(
        em,
        `WITH eligible AS (
          SELECT transaction_id FROM pending_reference_jobs
          WHERE next_attempt_at <= ?
            AND (claim_token IS NULL OR lease_until <= ?)
          ORDER BY next_attempt_at,transaction_id
          FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE pending_reference_jobs job
        SET claim_token=?,lease_until=?,attempts=job.attempts+1,updated_at=?
        FROM eligible
        WHERE job.transaction_id=eligible.transaction_id
        RETURNING job.transaction_id,job.attempts,job.expires_at,job.lease_until`,
        [at, at, token, leaseUntil, at],
      );
      const row = rows[0];
      return row
        ? {
            transactionId: row['transaction_id'] as string,
            token,
            attempts: Number(row['attempts']),
            expiresAt: date(row['expires_at']),
            leaseUntil: date(row['lease_until']),
          }
        : null;
    });
  }

  async processClaim(claim: PendingReferenceClaim, at?: Date): Promise<PendingReferenceOutcome> {
    try { return await this.processClaimAttempt(claim,at); }
    catch(error) {
      if(!isKnownPermanentWagerFailure(error) || claim.attempts<this.options.permanentFailureAttempts) throw error;
      return this.processClaimAttempt(claim,at,permanentWagerDiagnostic(claim.attempts));
    }
  }

  private async processClaimAttempt(claim: PendingReferenceClaim, at?: Date,
    diagnostic?: WagerFailureDiagnostic): Promise<PendingReferenceOutcome> {
    if (at) validDate(at);
    // Instante explícito permite relógio determinístico nos testes. Workers reais
    // atualizam o relógio após esperas SQL; instante anterior ao lock não valida lease.
    const currentTime = () => at ?? new Date();
    try {
      return await this.transaction(async em => {
        const [identity] = await sqlQuery(
          em,
          `SELECT wager.wallet_id FROM pending_reference_jobs job
            JOIN wager_transactions wager ON wager.id=job.transaction_id
            WHERE job.transaction_id=? AND job.claim_token=?`,
          [claim.transactionId, claim.token],
        );
        if (!identity) return { status: 'LOST_LEASE', transactionId: claim.transactionId };

        const lockedWallet = await em.findOne(WalletRecord, identity['wallet_id'] as string,
          { lockMode: LockMode.PESSIMISTIC_WRITE, disableIdentityMap: true });
        const walletRow = lockedWallet ? walletRecordRow(lockedWallet) : undefined;
        if (!walletRow) throw new Error('Pending wager refers to a missing wallet.');

        const [job] = await sqlQuery(
          em,
          `SELECT * FROM pending_reference_jobs
            WHERE transaction_id=? AND claim_token=? FOR UPDATE`,
          [claim.transactionId, claim.token],
        );
        if (!job || date(job['lease_until']) <= currentTime()) return { status: 'LOST_LEASE', transactionId: claim.transactionId };

        const lockedTransaction = await em.findOne(WagerTransactionLockRecord, claim.transactionId,
          { lockMode: LockMode.PESSIMISTIC_WRITE, disableIdentityMap: true });
        const [transactionRow] = lockedTransaction
          ? await sqlQuery(em, 'SELECT * FROM wager_transactions WHERE id=?', [claim.transactionId]) : [];
        if (!transactionRow || transactionRow['status'] !== WagerTransactionStatus.PendingReference) {
          await this.deleteClaim(em, claim, currentTime());
          return { status: 'STALE', transactionId: claim.transactionId };
        }

        const transaction = rehydrate(transactionRow);
        const walletVersion = Number(walletRow['version']);
        if (!Number.isSafeInteger(walletVersion) || walletVersion < 1) {
          throw new Error('Wallet version exceeds supported exact integer range.');
        }
        const wallet = Wallet.rehydrate({
          id: walletRow['id'] as string,
          playerId: walletRow['player_id'] as string,
          balance: Money.from({
            amount: walletRow['balance'] as string,
            currency: walletRow['currency'] as string,
          }),
          version: walletVersion,
          currency: walletRow.currency, createdAt: walletRow.created_at, updatedAt: walletRow.updated_at,
        });

        const [referenceRow] = await sqlQuery(
          em,
          `SELECT * FROM wager_transactions
            WHERE provider_id=? AND external_transaction_id=?`,
          [transaction.providerId, transaction.referenceExternalTransactionId],
        );
        const reference = referenceRow ? rehydrate(referenceRow) : undefined;
        // Reversões já aplicadas à referência; a política (JWP_REVERSAL_POLICY) decide a colisão (JWP-ADR-005).
        const policy = reversalPolicy();
        const applied = reference && transaction.requiresReference() ? await appliedReversalKinds(em, reference.id, policy) : [];
        const schedulerNow = currentTime();
        if (date(job['lease_until']) <= schedulerNow) return { status: 'LOST_LEASE', transactionId: claim.transactionId };
        // O lease usa o relógio do worker; a transição financeira não pode anteceder
        // os instantes de aceitação e pendência persistidos por outro host.
        const checkedAt = new Date(Math.max(
          schedulerNow.getTime(), transaction.createdAt.getTime(), transaction.pendingReferenceAt?.getTime() ?? -Infinity,
        ));
        if(diagnostic) ProcessWager.recordFailure(transaction);
        const movement = diagnostic ? undefined : ProcessWager.applyToWallet(transaction, wallet, reference, applied, checkedAt, date(job['expires_at']), policy);
        if (transaction.status === WagerTransactionStatus.PendingReference) {
          const nextAttemptAt = this.nextAttempt(checkedAt, Number(job['attempts']), date(job['expires_at']));
          const updated = await sqlQuery(
            em,
            `UPDATE pending_reference_jobs
              SET next_attempt_at=?,claim_token=NULL,lease_until=NULL,updated_at=?
              WHERE transaction_id=? AND claim_token=? AND lease_until>? RETURNING transaction_id`,
            [nextAttemptAt, checkedAt, claim.transactionId, claim.token, currentTime()],
          );
          if (!updated.length) return { status: 'LOST_LEASE', transactionId: claim.transactionId };
          return { status: 'RESCHEDULED', transactionId: claim.transactionId, nextAttemptAt };
        }

      await this.persistTerminal(em, transaction, wallet, movement, reference, checkedAt, diagnostic);
      await this.deleteClaim(em, claim, currentTime());
      return transaction.status === WagerTransactionStatus.Processed
        ? { status: 'PROCESSED', transactionId: transaction.id }
        : transaction.status===WagerTransactionStatus.Failed
          ? {status:'FAILED',transactionId:transaction.id,failureCode:'PERMANENT_INFRASTRUCTURE_FAILURE'}
          : {
            status: 'REJECTED',
            transactionId: transaction.id,
            failureCode: transaction.failureCode as RejectionCode,
          };
    });
    } catch (error) {
      if (error instanceof PendingReferenceLeaseLost) return { status: 'LOST_LEASE', transactionId: claim.transactionId };
      throw error;
    }
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

  private nextAttempt(at: Date, attempts: number, expiresAt: Date): Date {
    if (!Number.isSafeInteger(attempts) || attempts < 1) throw new Error('Invalid persisted attempt count.');
    const jitter = this.options.jitter();
    if (!Number.isFinite(jitter) || jitter < 0 || jitter >= 1) throw new Error('Invalid scheduler jitter.');
    const exponent = Math.min(attempts - 1, 30);
    const exponential = Math.min(
      this.options.maxBackoffMs,
      this.options.baseBackoffMs * 2 ** exponent,
    );
    const delayed = Math.min(
      this.options.maxBackoffMs,
      exponential + Math.floor(jitter * this.options.baseBackoffMs),
    );
    return new Date(Math.min(at.getTime() + delayed, expiresAt.getTime()));
  }

  private async deleteClaim(em: EntityManager, claim: PendingReferenceClaim, at: Date): Promise<void> {
    const deleted = await sqlQuery(
      em,
      `DELETE FROM pending_reference_jobs
        WHERE transaction_id=? AND claim_token=? AND lease_until>? RETURNING transaction_id`,
      [claim.transactionId, claim.token, at],
    );
    if (!deleted.length) throw new PendingReferenceLeaseLost('Pending-reference claim expired before completion.');
  }

  private async persistTerminal(
    em: EntityManager,
    transaction: WagerTransaction,
    wallet: Wallet,
    movement: WalletMovement | undefined,
    reference: WagerTransaction | undefined,
    at: Date,
    diagnostic?: WagerFailureDiagnostic,
  ): Promise<void> {
    const snapshot = ProcessWager.snapshot(transaction, wallet);
    await sqlQuery(
      em,
      `UPDATE wager_transactions
        SET status=?,reference_transaction_id=?,failure_code=?,response_snapshot=?::jsonb,processed_at=?
        WHERE id=?`,
      [
        transaction.status,
        transaction.referenceTransactionId ?? null,
        transaction.failureCode ?? null,
        JSON.stringify(snapshot),
        transaction.processedAt ?? null,
        transaction.id,
      ],
    );
    // O primeiro evento da outbox preserva a correlação da aceitação PENDING_REFERENCE.
    const [origin] = await sqlQuery(
      em,
      `SELECT envelope->>'correlationId' AS correlation_id FROM outbox_events
        WHERE transaction_id=? ORDER BY created_at,event_id LIMIT 1`,
      [transaction.id],
    );
    const correlationId = typeof origin?.['correlation_id'] === 'string' && origin['correlation_id'].trim()
      ? origin['correlation_id'] : randomUUID();
    await persistWagerEffects(em, { transaction, wallet, movement, reference, at, correlationId, diagnostic });
  }
}
