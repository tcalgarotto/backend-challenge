import { persistWagerEffects } from './persist-wager-effects.js';
import { appliedReversalKinds } from './sql-reversal-guard.js';
import { reversalPolicy } from '../domain/reversal-policy.js';
import { WalletRecord, walletRecordRow } from '../../persistence/financial-orm.js';
import { isKnownPermanentWagerFailure, permanentWagerDiagnostic } from '../../shared/infrastructure/permanent-wager-failure.js';
import { runSqlTransaction, sqlTransactionPolicy } from '../../shared/infrastructure/sql-transaction.js';
import { IsolationLevel, LockMode } from '@mikro-orm/core';
import { transactionFailureCause } from '../../persistence/postgres-connection-failure.js';
import { isTransientInfrastructureFailure } from '../../shared/infrastructure/sql-transaction.js';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Money } from '../../shared/domain/money.js';
import { Wallet } from '../../wallet/domain/wallet.js';
import { defaultWalletOpeningSqlPolicy, type WalletOpeningSqlPolicy } from '../../wallet/infrastructure/sql-wallet-opening-store.js';
import { ProcessWager, ProcessWagerError, type ProcessWagerStore, type ProcessWagerContext, type ProcessWagerResult } from '../application/process-wager.js';
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus, type CreateWagerTransactionProps } from '../domain/wager-transaction.js';
import type { FailureCode, WagerFailureDiagnostic } from '../domain/wager-error.js';

const query = (em: EntityManager, sql: string, args: unknown[] = []) =>
  em.getConnection().execute<Row[]>(sql,args,'all',em.getTransactionContext());
type Row = Record<string, unknown>;
interface StoredSnapshot extends ProcessWagerResult {
  readonly pendingReferenceAt?: string;
  readonly referenceExpiresAt?: string;
}
const identityConstraints = new Set(['wager_transactions_provider_id_idempotency_key_key','wager_transactions_provider_id_external_transaction_id_key']);
function isRetryable(failure: unknown): boolean {
  const sql = (failure ?? {}) as {code?:string;constraint?:string};
  const collision = sql.code === '23505' && identityConstraints.has(sql.constraint ?? '');
  // Compartilha a classificação tipada das rotas; não interpreta texto de erro.
  return collision || isTransientInfrastructureFailure(failure);
}
function rehydrate(row: Row): WagerTransaction {
  const snapshot = row['response_snapshot'] as StoredSnapshot | null;
  return WagerTransaction.rehydrate({
    id:row['id'] as string, walletId:row['wallet_id'] as string,playerId:row['player_id'] as string,
    providerId:row['provider_id'] as string,externalTransactionId:row['external_transaction_id'] as string,
    idempotencyKey:row['idempotency_key'] as string,payloadHash:row['payload_hash'] as string,
    roundId:row['round_id'] as string,gameId:row['game_id'] as string,
    kind:row['kind'] as WagerTransactionKind,status:row['status'] as WagerTransactionStatus,
    money:Money.from({amount:row['amount'] as string,currency:row['currency'] as string}),
    createdAt:new Date(row['created_at'] as string),
    ...(row['reference_external_transaction_id'] === null ? {} : {referenceExternalTransactionId:row['reference_external_transaction_id'] as string}),
    ...(row['reference_transaction_id'] === null ? {} : {referenceTransactionId:row['reference_transaction_id'] as string}),
    ...(row['failure_code'] === null ? {} : {failureCode:row['failure_code'] as FailureCode}),
    ...(row['processed_at'] === null ? {} : {processedAt:new Date(row['processed_at'] as string)}),
    ...(snapshot?.pendingReferenceAt ? {pendingReferenceAt:new Date(snapshot.pendingReferenceAt),referenceExpiresAt:new Date(snapshot.referenceExpiresAt!)} : {}),
  });
}
/** Só entrega resultado após o commit e a validação das constraints diferidas. */
export class SqlProcessWagerStore implements ProcessWagerStore {
  private readonly policy: WalletOpeningSqlPolicy;
  constructor(private readonly orm: MikroORM, policy: Partial<WalletOpeningSqlPolicy> = {}) {
    const configured = {...defaultWalletOpeningSqlPolicy,...policy};
    if (Object.values(configured).some(value => !Number.isSafeInteger(value) || value <= 0) ||
        configured.maxAttempts > 10 || configured.lockTimeoutMs > configured.statementTimeoutMs ||
        configured.statementTimeoutMs > configured.transactionTimeoutMs || configured.transactionTimeoutMs > configured.budgetMs)
      throw new Error('Invalid wager SQL policy.');
    this.policy = Object.freeze(configured);
  }
  async process(command: CreateWagerTransactionProps, context: ProcessWagerContext): Promise<ProcessWagerResult> {
    const deadline = Date.now() + this.policy.budgetMs;
    for (let attempt=1;;attempt++) {
      let trx: unknown;
      try {
        return await this.orm.em.fork().transactional(async em => {
          trx = em.getTransactionContext();
          const remaining = deadline-Date.now();
          if (remaining <= 0) throw new ProcessWagerError('INFRASTRUCTURE_UNAVAILABLE',503);
          await query(em,`SELECT set_config('lock_timeout',?,true),set_config('statement_timeout',?,true),set_config('transaction_timeout',?,true)`,
            [String(Math.min(remaining,this.policy.lockTimeoutMs)),String(Math.min(remaining,this.policy.statementTimeoutMs)),String(Math.min(remaining,this.policy.transactionTimeoutMs))]);
          await query(em,'SET LOCAL ROLE jwp_runtime');
          return this.processInTransaction(em,command,context);
        },{isolationLevel:IsolationLevel.READ_COMMITTED});
      } catch(error) {
        // A causa preservada só acrescenta retryabilidade; outros erros continuam inalterados.
        const cause = await transactionFailureCause(trx);
        const failure = isRetryable(cause) || isKnownPermanentWagerFailure(cause) ? cause : error;
        const retryable = isRetryable(failure);
        const permanent = isKnownPermanentWagerFailure(failure);
        // Após colisão UNIQUE, iniciar EM/TX nova para ler o vencedor confirmado;
        // nunca consultar dentro da transação abortada.
        if ((retryable || permanent) && attempt < this.policy.maxAttempts) {
          const delay = this.policy.retryBaseMs*2**(attempt-1)+Math.floor(Math.random()*this.policy.retryBaseMs);
          if (Date.now()+delay < deadline) { await Bun.sleep(delay); continue; }
        }
        if (permanent && attempt >= this.policy.maxAttempts) {
          return this.auditPermanentFailure(command,context,permanentWagerDiagnostic(attempt),deadline-Date.now());
        }
        if (retryable) throw new ProcessWagerError('INFRASTRUCTURE_UNAVAILABLE',503,{cause:failure});
        throw error;
      }
    }
  }
  /** Reconsulta identidade em TX nova antes de FAILED; resultado já confirmado vence, inclusive após commit incerto. */
  async auditPermanentFailure(command: CreateWagerTransactionProps, context: ProcessWagerContext,
    diagnostic: WagerFailureDiagnostic, budgetMs=this.policy.budgetMs): Promise<ProcessWagerResult> {
    if(budgetMs<=0) throw new ProcessWagerError('INFRASTRUCTURE_UNAVAILABLE',503);
    const deadline=Date.now()+budgetMs;
    for(let attempt=1;;attempt++) {
      const remaining=deadline-Date.now();
      if(remaining<=0) throw new ProcessWagerError('INFRASTRUCTURE_UNAVAILABLE',503);
      const policy=sqlTransactionPolicy({...this.policy,budgetMs:remaining,
        transactionTimeoutMs:Math.min(remaining,this.policy.transactionTimeoutMs),
        statementTimeoutMs:Math.min(remaining,this.policy.statementTimeoutMs),
        lockTimeoutMs:Math.min(remaining,this.policy.lockTimeoutMs)});
      try {
        return await runSqlTransaction(this.orm,policy,
          em=>this.processInTransaction(em,command,context,diagnostic),{readOnly:false});
      } catch(error) {
        const collision=error as {code?:string;constraint?:string};
        if(collision.code!=='23505'||!identityConstraints.has(collision.constraint??'')||attempt>=this.policy.maxAttempts) throw error;
        await Bun.sleep(this.policy.retryBaseMs);
      }
    }
  }
  /** Inbox deve obter lock antes da wallet na mesma TX; o chamador controla retry e commit da unidade inteira. */
  async processInTransaction(em: EntityManager, command: CreateWagerTransactionProps, context: ProcessWagerContext, diagnostic?: WagerFailureDiagnostic): Promise<ProcessWagerResult> {
    if (!em.getTransactionContext()) throw new Error('Financial processing requires an active SQL transaction.');
    // O fork conserva a TX e descarta o identity map antes dos UPDATEs explícitos.
    const lockedWallet = await em.findOne(WalletRecord, command.walletId, { lockMode: LockMode.PESSIMISTIC_WRITE, disableIdentityMap: true });
    const walletRow = lockedWallet ? walletRecordRow(lockedWallet) : undefined;
    // Wallet existente é bloqueada antes da identidade. Se faltar, ler identidades
    // preserva conflitos de uma aceitação anterior; UNIQUE por provedor protege
    // corridas entre wallets diferentes na inserção.
    const [sameKey] = await query(em,'SELECT * FROM wager_transactions WHERE provider_id=? AND idempotency_key=?',[command.providerId,command.idempotencyKey]);
    if (sameKey) {
      if (sameKey['payload_hash'] !== command.payloadHash) throw new ProcessWagerError('IDEMPOTENCY_CONFLICT',409);
      const snapshot = sameKey['response_snapshot'] as StoredSnapshot;
      if (!snapshot?.body) throw new Error('Persisted wager is missing its result snapshot.');
      return {httpStatus:snapshot.httpStatus,body:{...snapshot.body,idempotentReplay:true}};
    }
    const [sameExternal] = await query(em,'SELECT id FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',[command.providerId,command.externalTransactionId]);
    if (sameExternal) throw new ProcessWagerError('EXTERNAL_IDENTITY_CONFLICT',409);
    // Identidade persistida prevalece sobre wallet alterada ou inexistente;
    // somente uma identidade nova exige wallet válida antes da aceitação.
    if (!walletRow) throw new ProcessWagerError('WALLET_NOT_FOUND',404);
    if (walletRow['player_id'] !== command.playerId) throw new ProcessWagerError('PLAYER_MISMATCH',400);
    if (walletRow['currency'] !== command.money.currency) throw new ProcessWagerError('CURRENCY_MISMATCH',400);
    const version = Number(walletRow['version']);
    if (!Number.isSafeInteger(version) || version < 1 || version === Number.MAX_SAFE_INTEGER)
      throw new Error('Wallet version exceeds supported exact integer range.');
    const wallet = Wallet.rehydrate({id:command.walletId,playerId:command.playerId,
      balance:Money.from({amount:walletRow['balance'],currency:walletRow['currency']}),version,
      currency:walletRow.currency,createdAt:walletRow.created_at,updatedAt:walletRow.updated_at});
    // Aceitação é capturada na TX que persiste a identidade, após o lock;
    // preparação e retry do comando não são aceitação.
    const at = new Date(Math.max(Date.now(),command.createdAt.getTime()));
    const transaction = WagerTransaction.create({...command,createdAt:at});
    let reference: WagerTransaction | undefined;
    if (command.referenceExternalTransactionId !== undefined) {
      const [row] = await query(em,'SELECT * FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',[command.providerId,command.referenceExternalTransactionId]);
      if (row) reference = rehydrate(row);
    }
    // Reversões já aplicadas à referência; a política (JWP_REVERSAL_POLICY) decide a colisão (JWP-ADR-005).
    const policy = reversalPolicy();
    const applied = reference && transaction.requiresReference() ? await appliedReversalKinds(em, reference.id, policy) : [];
    if(diagnostic) ProcessWager.recordFailure(transaction);
    const movement = diagnostic ? undefined : ProcessWager.applyToWallet(transaction, wallet, reference, applied, at, undefined, policy);
    const snapshot = ProcessWager.snapshot(transaction, wallet);
    await query(em,`INSERT INTO wager_transactions(id,wallet_id,player_id,currency,provider_id,external_transaction_id,idempotency_key,
      payload_hash,round_id,game_id,kind,status,amount,reference_external_transaction_id,reference_transaction_id,
      failure_code,response_snapshot,created_at,processed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::jsonb,?,?)`,
      [transaction.id,wallet.id,wallet.playerId,wallet.currency,command.providerId,command.externalTransactionId,command.idempotencyKey,
        command.payloadHash,command.roundId,command.gameId,transaction.kind,transaction.status,command.money.toString(),
        command.referenceExternalTransactionId ?? null,transaction.referenceTransactionId ?? null,transaction.failureCode ?? null,
        JSON.stringify(snapshot),transaction.createdAt,transaction.processedAt ?? null]);
    await persistWagerEffects(em, { transaction, wallet, movement, reference, at,
      correlationId: context.correlationId, diagnostic });
    return {httpStatus:snapshot.httpStatus,body:snapshot.body};
  }
}
