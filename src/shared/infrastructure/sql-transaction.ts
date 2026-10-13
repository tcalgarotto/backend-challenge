import { IsolationLevel } from '@mikro-orm/core';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { isPostgresConnectionFailure, transactionFailureCause } from '../../persistence/postgres-connection-failure.js';

export type SqlRow = Record<string, unknown>;
export const sqlQuery = (em: EntityManager, sql: string, args: unknown[] = []) =>
  em.getConnection().execute<SqlRow[]>(sql, args, 'all', em.getTransactionContext());

// Inclui conflitos de TX e timeouts, SQLSTATEs de conexão e falhas de socket
// anteriores ao recebimento de SQLSTATE.
const transientStates = new Set(['40001','40P01','55P03','57014','25P03','25P04','08000','08003','08006','57P01',
  'ECONNREFUSED','ECONNRESET','ETIMEDOUT']);

/** A classificação usa origem tipada e SQLSTATE em todas as rotas, nunca texto do erro. */
export function isTransientInfrastructureFailure(failure: unknown): boolean {
  // Timeout de aquisição do knex tem tipo próprio, mas não SQLSTATE.
  const poolTimeout = failure instanceof Error && failure.name === 'KnexTimeoutError';
  const code = (failure as { code?: unknown } | null | undefined)?.code;
  return poolTimeout || isPostgresConnectionFailure(failure) || (typeof code === 'string' && transientStates.has(code));
}

/** BEGIN em socket morto pode virar erro genérico; a causa preservada pelo knex é a falha real. */
export async function recordedTransactionFailure(error: unknown, trx: unknown): Promise<unknown> {
  const cause = await transactionFailureCause(trx);
  return isTransientInfrastructureFailure(cause) ? cause : error;
}

export class InfrastructureUnavailableError extends Error {
  readonly code = 'INFRASTRUCTURE_UNAVAILABLE' as const;
  readonly httpStatus = 503;
  constructor(options?: ErrorOptions) { super('INFRASTRUCTURE_UNAVAILABLE', options); this.name = 'InfrastructureUnavailableError'; }
}

export interface SqlTransactionPolicy {
  readonly lockTimeoutMs: number;
  readonly statementTimeoutMs: number;
  readonly transactionTimeoutMs: number;
  readonly budgetMs: number;
  readonly maxAttempts: number;
  readonly retryBaseMs: number;
}
export const defaultSqlTransactionPolicy: SqlTransactionPolicy = Object.freeze({
  lockTimeoutMs: 1000, statementTimeoutMs: 5000, transactionTimeoutMs: 5000,
  budgetMs: 15000, maxAttempts: 3, retryBaseMs: 25,
});
export function sqlTransactionPolicy(policy: Partial<SqlTransactionPolicy> = {}): SqlTransactionPolicy {
  const configured = { ...defaultSqlTransactionPolicy, ...policy };
  if (Object.values(configured).some(value => !Number.isSafeInteger(value) || value <= 0) ||
      configured.maxAttempts > 10 || configured.lockTimeoutMs > configured.statementTimeoutMs ||
      configured.statementTimeoutMs > configured.transactionTimeoutMs || configured.transactionTimeoutMs > configured.budgetMs)
    throw new Error('Invalid SQL transaction policy.');
  return Object.freeze(configured);
}

/** Retry reinicia a TX inteira e exige trabalho sem efeitos duráveis; outras falhas preservam a causa original. */
export async function runSqlTransaction<T>(orm: MikroORM, policy: SqlTransactionPolicy,
  work: (em: EntityManager) => Promise<T>, options: { readOnly: boolean }): Promise<T> {
  const deadline = Date.now() + policy.budgetMs;
  for (let attempt = 1; ; attempt++) {
    let trx: unknown;
    try {
      return await orm.em.fork().transactional(async em => {
        trx = em.getTransactionContext();
        const remaining = deadline - Date.now();
        if (remaining <= 0) throw new InfrastructureUnavailableError();
        if (options.readOnly) await sqlQuery(em, 'SET TRANSACTION READ ONLY');
        await sqlQuery(em, `SELECT set_config('lock_timeout',?,true),set_config('statement_timeout',?,true),set_config('transaction_timeout',?,true)`,
          [String(Math.min(remaining, policy.lockTimeoutMs)), String(Math.min(remaining, policy.statementTimeoutMs)),
            String(Math.min(remaining, policy.transactionTimeoutMs))]);
        await sqlQuery(em, 'SET LOCAL ROLE jwp_runtime');
        return work(em);
      }, { isolationLevel: IsolationLevel.READ_COMMITTED });
    } catch (error) {
      if (error instanceof InfrastructureUnavailableError) throw error;
      const failure = await recordedTransactionFailure(error, trx);
      if (!isTransientInfrastructureFailure(failure)) throw error;
      if (attempt < policy.maxAttempts) {
        const delay = policy.retryBaseMs * 2 ** (attempt - 1) + Math.floor(Math.random() * policy.retryBaseMs);
        if (Date.now() + delay < deadline) { await Bun.sleep(delay); continue; }
      }
      throw new InfrastructureUnavailableError({ cause: failure });
    }
  }
}
