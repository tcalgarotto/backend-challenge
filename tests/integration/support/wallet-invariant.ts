import { expect } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';

/** Uma leitura SQL compara todas as wallets no mesmo snapshot, sem converter dinheiro. */
export async function assertWalletLedgerInvariant(orm: MikroORM): Promise<void> {
  const connection=orm.em.fork().getConnection();
  const [count]=await connection.execute<{wallets:number}[]>('SELECT count(*)::int wallets FROM wallets');
  expect(count!.wallets).toBeGreaterThan(0);
  const invalid=await connection.execute(`SELECT w.id FROM wallets w WHERE w.balance<0 OR w.balance<>
    (SELECT coalesce(sum(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0)
      FROM ledger_entries l WHERE l.wallet_id=w.id)`);
  expect(invalid).toEqual([]);
}
