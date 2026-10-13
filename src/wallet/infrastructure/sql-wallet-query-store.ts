import { LockMode } from '@mikro-orm/core';
import { WalletRecord } from '../../persistence/financial-orm.js';
import type { MikroORM } from '@mikro-orm/postgresql';
import { Money } from '../../shared/domain/money.js';
import { runSqlTransaction, sqlQuery, sqlTransactionPolicy, type SqlRow, type SqlTransactionPolicy } from '../../shared/infrastructure/sql-transaction.js';
import type { LedgerEntryView, LedgerPosition, WalletQueryStore, WalletView } from '../application/wallet-queries.js';
import type { ReconciliationSnapshot, ReconciliationStore } from '../application/reconcile-wallet.js';

const iso = (value: unknown) => new Date(value as string | Date).toISOString();
function exactInteger(value: unknown): number {
  const parsed = Number(value);
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value) || !Number.isSafeInteger(parsed))
    throw new Error('Persisted integer exceeds the supported exact range.');
  return parsed;
}
// Revalida dinheiro persistido sem conversão por Number.
const money = (amount: unknown, currency: unknown) => Money.from({ amount: amount as string, currency: currency as string }).toJSON();
function ledgerEntry(row: SqlRow): LedgerEntryView {
  return {
    id: row['id'] as string, walletId: row['wallet_id'] as string, transactionId: row['transaction_id'] as string,
    walletVersion: exactInteger(row['wallet_version']), direction: row['direction'] as 'CREDIT' | 'DEBIT',
    money: money(row['amount'], row['currency']), balanceBefore: money(row['balance_before'], row['currency']),
    balanceAfter: money(row['balance_after'], row['currency']), createdAt: iso(row['created_at']),
  };
}

export class SqlWalletQueryStore implements WalletQueryStore, ReconciliationStore {
  private readonly policy: SqlTransactionPolicy;
  constructor(private readonly orm: MikroORM, policy: Partial<SqlTransactionPolicy> = {}) {
    this.policy = sqlTransactionPolicy(policy);
  }

  findWallet(walletId: string): Promise<WalletView | undefined> {
    return runSqlTransaction(this.orm, this.policy, async em => {
      const [row] = await sqlQuery(em, `SELECT id::text id,player_id,currency,balance::text balance,version::text version,created_at,updated_at
        FROM wallets WHERE id=?`, [walletId]);
      if (!row) return undefined;
      return { id: row['id'] as string, playerId: row['player_id'] as string, balance: money(row['balance'], row['currency']),
        version: exactInteger(row['version']), createdAt: iso(row['created_at']), updatedAt: iso(row['updated_at']) };
    }, { readOnly: true });
  }

  listLedger(walletId: string, after: LedgerPosition | undefined, limit: number): Promise<LedgerEntryView[] | undefined> {
    return runSqlTransaction(this.orm, this.policy, async em => {
      if ((await sqlQuery(em, 'SELECT 1 FROM wallets WHERE id=?', [walletId])).length === 0) return undefined;
      const columns = `l.id::text id,l.wallet_id::text wallet_id,l.transaction_id::text transaction_id,l.wallet_version::text wallet_version,
        l.direction,l.currency,l.amount::text amount,l.balance_before::text balance_before,l.balance_after::text balance_after,l.created_at`;
      // Novos lançamentos têm versão maior; keyset não desloca páginas anteriores.
      // UNIQUE(wallet_id,wallet_version) atende essa ordem.
      // Qualificar ORDER BY evita ordenar pelo alias textual das colunas.
      const rows = after
        ? await sqlQuery(em, `SELECT ${columns} FROM ledger_entries l WHERE l.wallet_id=? AND (l.wallet_version,l.id)>(?::bigint,?::uuid)
            ORDER BY l.wallet_version,l.id LIMIT ?`, [walletId, String(after.walletVersion), after.id, limit])
        : await sqlQuery(em, `SELECT ${columns} FROM ledger_entries l WHERE l.wallet_id=? ORDER BY l.wallet_version,l.id LIMIT ?`, [walletId, limit]);
      return rows.map(ledgerEntry);
    }, { readOnly: true });
  }

  snapshot(walletId: string): Promise<ReconciliationSnapshot | undefined> {
    return runSqlTransaction(this.orm, this.policy, async em => {
      // Após obter o lock, não há append em voo para a wallet;
      // a próxima leitura em READ COMMITTED vê todos os commits anteriores.
      const wallet = await em.findOne(WalletRecord, walletId, { lockMode: LockMode.PESSIMISTIC_WRITE, disableIdentityMap: true });
      if (!wallet) return undefined;
      // Agregação numeric é exata e sem limite de saldo para subtotais históricos.
      const [ledger] = await sqlQuery(em, `SELECT count(*)::text entries,
        round(coalesce(sum(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END),0),2)::text total
        FROM ledger_entries WHERE wallet_id=?`, [walletId]);
      return { walletId: wallet.id, currency: wallet.currency, storedBalance: wallet.balance,
        ledgerTotal: ledger!['total'] as string, checkedEntries: exactInteger(ledger!['entries']) };
    }, { readOnly: false });
  }
}
