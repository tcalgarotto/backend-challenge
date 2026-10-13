import { createHash, randomUUID } from 'node:crypto';
import { IsolationLevel } from '@mikro-orm/core';
import type { MikroORM, EntityManager } from '@mikro-orm/postgresql';
import { WalletLedgerEntry, LedgerDirection } from '../domain/wallet-ledger-entry.js';
import { OutboxMessage } from '../../messaging/domain/outbox-message.js';
import { Money } from '../../shared/domain/money.js';
import { recordedTransactionFailure } from '../../shared/infrastructure/sql-transaction.js';
import { WagerTransaction } from '../../wagering/domain/wager-transaction.js';
import { WagerTransactionProcessed } from '../../wagering/events/wager-events.js';
import { WalletBalanceChanged } from '../events/wallet-balance-changed.js';
import type { Wallet } from '../domain/wallet.js';
import { WalletOpeningError, type WalletOpeningStore, type WalletOpeningContext } from '../application/open-wallet.js';

export const openingProvider = 'internal:wallet-opening';
const query = (em: EntityManager, sql: string, args: unknown[] = []) =>
  em.getConnection().execute(sql, args, 'all', em.getTransactionContext());

export interface WalletOpeningSqlPolicy {
  readonly lockTimeoutMs: number;
  readonly statementTimeoutMs: number;
  readonly transactionTimeoutMs: number;
  readonly budgetMs: number;
  readonly maxAttempts: number;
  readonly retryBaseMs: number;
}
export const defaultWalletOpeningSqlPolicy: WalletOpeningSqlPolicy = Object.freeze({
  lockTimeoutMs: 1000, statementTimeoutMs: 5000, transactionTimeoutMs: 5000,
  budgetMs: 15000, maxAttempts: 3, retryBaseMs: 25,
});
const transientSqlCodes = new Set(['40001', '40P01', '55P03', '57014']);
export class SqlWalletOpeningStore implements WalletOpeningStore {
  private readonly policy: WalletOpeningSqlPolicy;
  constructor(private readonly orm: MikroORM, policy: Partial<WalletOpeningSqlPolicy> = {}) {
    const configured = { ...defaultWalletOpeningSqlPolicy, ...policy };
    if (Object.values(configured).some(value => !Number.isSafeInteger(value) || value <= 0) ||
        configured.maxAttempts > 10 || configured.lockTimeoutMs > configured.statementTimeoutMs ||
        configured.statementTimeoutMs > configured.transactionTimeoutMs ||
        configured.transactionTimeoutMs > configured.budgetMs) {
      throw new Error('Invalid wallet opening SQL policy.');
    }
    this.policy = Object.freeze(configured);
  }
  async create(wallet: Wallet, context: WalletOpeningContext): Promise<void> {
    const deadline = Date.now() + this.policy.budgetMs;
    for (let attempt = 1; ; attempt++) {
      try {
        await this.createAttempt(wallet, context, deadline);
        return;
      } catch (error) {
        const code = (error as {code?: string}).code;
        if (!code || !transientSqlCodes.has(code) || attempt >= this.policy.maxAttempts) throw error;
        const delay = this.policy.retryBaseMs * 2 ** (attempt - 1) + Math.floor(Math.random() * this.policy.retryBaseMs);
        if (Date.now() + delay >= deadline) throw error;
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  private async createAttempt(wallet: Wallet, context: WalletOpeningContext, deadline: number): Promise<void> {
    let trx: unknown;
    try {
      await this.orm.em.fork().transactional(async em => {
        trx = em.getTransactionContext();
        // O papel runtime não pode alterar o histórico do ledger nem possui tabelas financeiras.
        const remaining = deadline - Date.now();
        if (remaining <= 0) throw Object.assign(new Error('Wallet opening SQL budget exhausted.'), { code: '57014' });
        // PostgreSQL 17 limita a tentativa inteira, incluindo validações diferidas no commit.
        await query(em, `SELECT set_config('lock_timeout', ?, true),
          set_config('statement_timeout', ?, true), set_config('transaction_timeout', ?, true)`,
        [String(Math.min(this.policy.lockTimeoutMs, remaining)),
          String(Math.min(this.policy.statementTimeoutMs, remaining)),
          String(Math.min(this.policy.transactionTimeoutMs, remaining))]);
        await query(em, 'SET LOCAL ROLE jwp_runtime');
        await query(em, `INSERT INTO wallets(id,player_id,currency,balance,version,created_at,updated_at)
          VALUES (?,?,?,?,1,?,?)`, [wallet.id, wallet.playerId, wallet.currency, wallet.balance.toString(), wallet.createdAt, wallet.updatedAt]);
        if (wallet.balance.isZero()) return;

        const transactionId = randomUUID();
        const identity = `opening:${wallet.id}`;
        const payloadHash = createHash('sha256').update(JSON.stringify({ walletId: wallet.id,
          playerId: wallet.playerId, kind: 'OPENING', money: wallet.balance.toJSON() })).digest('hex');
        const transaction = WagerTransaction.createOpening({ id: transactionId, providerId: openingProvider,
          externalTransactionId: identity, idempotencyKey: identity, payloadHash, walletId: wallet.id,
          playerId: wallet.playerId, roundId: identity, gameId: 'wallet-opening',
          money: wallet.balance, createdAt: context.occurredAt });
        transaction.markProcessed(undefined, context.occurredAt);
        const snapshot = { transactionId, status: 'PROCESSED', balance: wallet.balance.toJSON(), idempotentReplay: false };
        await query(em, `INSERT INTO wager_transactions(id,wallet_id,player_id,currency,provider_id,
          external_transaction_id,idempotency_key,payload_hash,round_id,game_id,kind,status,amount,response_snapshot,created_at,processed_at)
          VALUES (?,?,?,?,?,?,?,?,?,?,'OPENING','PROCESSED',?,?::jsonb,?,?)`,
        [transactionId,wallet.id,wallet.playerId,wallet.currency,openingProvider,identity,identity,payloadHash,
          identity,'wallet-opening',wallet.balance.toString(),JSON.stringify(snapshot),context.occurredAt,context.occurredAt]);
        const entry=WalletLedgerEntry.create({id:randomUUID(),walletId:wallet.id,transactionId,
          direction:LedgerDirection.Credit,money:wallet.balance,balanceBefore:Money.zero(wallet.currency),
          balanceAfter:wallet.balance,createdAt:context.occurredAt});
        await query(em, `INSERT INTO ledger_entries(id,wallet_id,transaction_id,currency,wallet_version,
          direction,amount,balance_before,balance_after,created_at) VALUES (?,?,?,?,1,'CREDIT',?,'0.00',?,?)`,
        [entry.id,entry.walletId,entry.transactionId,wallet.currency,entry.money.toString(),entry.balanceAfter.toString(),entry.createdAt]);

        const common = { correlationId: context.correlationId, causationId: transactionId, occurredAt: context.occurredAt };
        const events = [
          WagerTransactionProcessed.from(transaction, wallet, { ...common, eventId: randomUUID() }),
          WalletBalanceChanged.from(wallet, transaction, { direction: LedgerDirection.Credit, money: wallet.balance,
            balanceBefore: Money.zero(wallet.currency), balanceAfter: wallet.balance, walletVersion: 1 },
          { ...common, eventId: randomUUID() }),
        ];
        for (const event of events) {
          const message=OutboxMessage.enqueue(event);
          await query(em, `INSERT INTO outbox_events(event_id,wallet_id,transaction_id,currency,event_type,envelope,created_at,next_attempt_at)
            VALUES (?,?,?,?,?,?::jsonb,?,?)`, [event.eventId,wallet.id,transactionId,wallet.currency,event.eventType,
              JSON.stringify(message.payload),message.occurredAt,message.nextAttemptAt]);
        }
      }, { isolationLevel: IsolationLevel.READ_COMMITTED });
    } catch (error) {
      // Só traduz conflito da identidade de wallet; outras falhas preservam a causa
      // tipada do driver. BEGIN em socket ocioso morto recupera a causa
      // que o knex preservou.
      const failure = await recordedTransactionFailure(error, trx);
      if (failure !== error) throw failure;
      const sqlError = error as { code?: string; constraint?: string };
      if (sqlError.code === '23505' && sqlError.constraint === 'wallets_player_id_currency_key') {
        throw new WalletOpeningError('WALLET_ALREADY_EXISTS');
      }
      throw error;
    }
  }
}
