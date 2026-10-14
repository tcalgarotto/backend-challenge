import type { EntityManager } from '@mikro-orm/postgresql';
import { WagerTransactionKind } from '../domain/wager-transaction.js';
import { REVERSAL_POLICY_SETTING, type ReversalPolicy } from '../domain/reversal-policy.js';

/**
 * Lê, sob o lock da wallet já obtido pelo chamador, os tipos de reversão PROCESSED contra a referência e
 * publica a política na transação (set_config local) para o gatilho jwp_reversal_policy_guard.
 * Duas reversões da mesma referência pertencem à mesma wallet, então o lock da wallet as serializa; o índice
 * único (referência, tipo) e o gatilho são a segunda barreira, também para escritas SQL diretas.
 */
export async function appliedReversalKinds(em: EntityManager, referenceId: string, policy: ReversalPolicy): Promise<WagerTransactionKind[]> {
  const connection = em.getConnection(), context = em.getTransactionContext();
  await connection.execute('SELECT set_config(?, ?, true)', [REVERSAL_POLICY_SETTING, policy], 'all', context);
  const rows = await connection.execute<Record<string, unknown>[]>(
    `SELECT kind FROM wager_transactions
      WHERE reference_transaction_id=? AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'`,
    [referenceId], 'all', context);
  return rows.map(row => row['kind'] as WagerTransactionKind);
}
