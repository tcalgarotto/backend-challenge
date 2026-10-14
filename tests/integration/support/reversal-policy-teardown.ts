import { expect } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';

const FINANCIAL_TABLES = 'wallets, wager_transactions, ledger_entries, outbox_events, wager_message_inbox, pending_reference_jobs, wager_event_inbox';

/**
 * Down completo de um banco descartável que pode conter REFUND e ROLLBACK aplicados à mesma referência
 * (política per_type). O down da Migration202610140010 recusa restaurar o índice por referência nesse caso
 * (REVERSAL_POLICY_DOWN_BLOCKED) em vez de apagar dados; aqui, e só em teste, os dados são descartados e o
 * down é repetido. `expectBlocked` exige que a recusa aconteça, provando a guarda do down.
 */
export async function downToZeroAfterReversals(orm: MikroORM, expectBlocked = false): Promise<void> {
  try {
    await orm.migrator.down({ to: 0 });
    expect(expectBlocked).toBe(false);
    return;
  } catch (error) {
    if (!String(error).includes('REVERSAL_POLICY_DOWN_BLOCKED')) throw error;
  }
  await orm.em.fork().transactional(async tx => {
    const connection = tx.getConnection(), context = tx.getTransactionContext();
    // Gatilhos append-only também barram TRUNCATE; replica vale só nesta TX do banco descartável.
    await connection.execute('SET LOCAL session_replication_role=replica', [], 'all', context);
    await connection.execute(`TRUNCATE ${FINANCIAL_TABLES} CASCADE`, [], 'all', context);
  });
  await orm.migrator.down({ to: 0 });
}
