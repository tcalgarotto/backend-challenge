import type { MikroORM } from '@mikro-orm/postgresql';
import { Money } from '../../shared/domain/money.js';
import { runSqlTransaction, sqlQuery, sqlTransactionPolicy, type SqlRow, type SqlTransactionPolicy } from '../../shared/infrastructure/sql-transaction.js';
import type { TransactionQueryStore, TransactionView } from '../application/transaction-queries.js';
import type { FailureCode } from '../domain/wager-error.js';
import { WagerTransactionStatus, type WagerTransactionKind } from '../domain/wager-transaction.js';

const columns = `id::text id,provider_id,external_transaction_id,wallet_id::text wallet_id,player_id,round_id,game_id,kind,
  currency,amount::text amount,status,created_at,reference_external_transaction_id,reference_transaction_id::text reference_transaction_id,
  failure_code,processed_at,response_snapshot`;
const terminal = new Set<string>([WagerTransactionStatus.Processed, WagerTransactionStatus.Rejected, WagerTransactionStatus.Failed]);
const iso = (value: unknown) => new Date(value as string | Date).toISOString();

function view(row: SqlRow): TransactionView {
  const status = row['status'] as WagerTransactionStatus;
  // Snapshot terminal preserva saldo histórico; pendência não tem saldo no snapshot.
  const snapshot = row['response_snapshot'] as {
    body?: { balance?: { amount: string; currency: string } };
    balance?: { amount: string; currency: string };
  } | null;
  // OPENING persiste o corpo direto; submissões usam { httpStatus, body }.
  // Ambas as formas preservam o saldo terminal histórico.
  const balance = terminal.has(status) ? snapshot?.body?.balance ?? snapshot?.balance : undefined;
  return {
    transactionId: row['id'] as string, providerId: row['provider_id'] as string,
    externalTransactionId: row['external_transaction_id'] as string, walletId: row['wallet_id'] as string,
    playerId: row['player_id'] as string, roundId: row['round_id'] as string, gameId: row['game_id'] as string,
    kind: row['kind'] as WagerTransactionKind,
    money: Money.from({ amount: row['amount'] as string, currency: row['currency'] as string }).toJSON(),
    status, createdAt: iso(row['created_at']),
    ...(row['reference_external_transaction_id'] === null ? {} : { referenceExternalTransactionId: row['reference_external_transaction_id'] as string }),
    ...(row['reference_transaction_id'] === null ? {} : { referenceTransactionId: row['reference_transaction_id'] as string }),
    ...(row['failure_code'] === null ? {} : { failureCode: row['failure_code'] as FailureCode }),
    ...(row['processed_at'] === null ? {} : { processedAt: iso(row['processed_at']) }),
    ...(balance === undefined ? {} : { resultBalance: Money.from(balance).toJSON() }),
  };
}

export class SqlTransactionQueryStore implements TransactionQueryStore {
  private readonly policy: SqlTransactionPolicy;
  constructor(private readonly orm: MikroORM, policy: Partial<SqlTransactionPolicy> = {}) {
    this.policy = sqlTransactionPolicy(policy);
  }
  findById(transactionId: string): Promise<TransactionView | undefined> {
    return this.one(`SELECT ${columns} FROM wager_transactions WHERE id=?`, [transactionId]);
  }
  findByProviderIdentity(providerId: string, externalTransactionId: string): Promise<TransactionView | undefined> {
    return this.one(`SELECT ${columns} FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?`, [providerId, externalTransactionId]);
  }
  private one(sql: string, args: unknown[]): Promise<TransactionView | undefined> {
    return runSqlTransaction(this.orm, this.policy, async em => {
      const [row] = await sqlQuery(em, sql, args);
      return row ? view(row) : undefined;
    }, { readOnly: true });
  }
}
