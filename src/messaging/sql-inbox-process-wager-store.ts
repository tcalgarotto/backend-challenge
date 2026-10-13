import { isKnownPermanentWagerFailure, permanentWagerDiagnostic } from '../shared/infrastructure/permanent-wager-failure.js';
import type { WagerFailureDiagnostic } from '../wagering/domain/wager-error.js';
import { InboxMessage } from './domain/inbox-message.js';
import { AsyncLocalStorage } from 'node:async_hooks';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { canonicalJson, parseWager, ProcessWager, ProcessWagerError, type ProcessWagerContext, type ProcessWagerResult, type ProcessWagerStore } from '../wagering/application/process-wager.js';
import type { CreateWagerTransactionProps } from '../wagering/domain/wager-transaction.js';
import { SqlProcessWagerStore } from '../wagering/infrastructure/sql-process-wager-store.js';
import { runSqlTransaction, sqlQuery, sqlTransactionPolicy, type SqlTransactionPolicy } from '../shared/infrastructure/sql-transaction.js';

export const WAGER_INBOX_CONSUMER = 'wager-transactions-consumer';

export interface InboxMessageMetadata {
  readonly consumerName: string;
  readonly messageId: string;
  readonly envelopeHash: string;
  readonly receiveCount?: number;
  readonly finalAttempt?: boolean;
}
export interface InboxMessageContext extends ProcessWagerContext, InboxMessageMetadata {}

export class InboxConflictError extends Error {
  readonly code = 'INBOX_MESSAGE_CONFLICT' as const;
  constructor() {
    super('INBOX_MESSAGE_CONFLICT');
    this.name = 'InboxConflictError';
  }
}

type InboxRow = {
  envelope_hash: string;
  received_at: string;
  result_snapshot: ProcessWagerResult | null;
  processed_at: string | null;
};
const identityConstraints = new Set([
  'wager_transactions_provider_id_idempotency_key_key',
  'wager_transactions_provider_id_external_transaction_id_key',
]);
const identityCollision = (error: unknown): boolean => {
  const sql = error as { code?: unknown; constraint?: unknown };
  return sql.code === '23505' && typeof sql.constraint === 'string' && identityConstraints.has(sql.constraint);
};

const inboxContext = (context: ProcessWagerContext, metadata: InboxMessageMetadata | undefined): InboxMessageContext => {
  if (!metadata?.consumerName || !metadata.messageId || !metadata.envelopeHash) {
    throw new ProcessWagerError('INVALID_PAYLOAD', 400);
  }
  return { ...context, ...metadata };
};

/** Lock da inbox precede o da wallet; ambos confirmam na mesma transação do caso financeiro. */
export class SqlInboxProcessWagerStore implements ProcessWagerStore {
  private readonly policy: SqlTransactionPolicy;
  private readonly message = new AsyncLocalStorage<InboxMessageMetadata>();
  constructor(
    private readonly orm: MikroORM,
    private readonly financial = new SqlProcessWagerStore(orm),
    policy: Partial<SqlTransactionPolicy> = {},
  ) {
    this.policy = sqlTransactionPolicy(policy);
  }

  async process(command: CreateWagerTransactionProps, rawContext: ProcessWagerContext, diagnostic?: WagerFailureDiagnostic, budgetMs=this.policy.budgetMs): Promise<ProcessWagerResult> {
    const context = inboxContext(rawContext, this.message.getStore());
    const deadline=Date.now()+budgetMs;
    for (let attempt = 1; ; attempt++) {
      const remaining=deadline-Date.now();
      if(remaining<=0) throw new ProcessWagerError('INFRASTRUCTURE_UNAVAILABLE',503);
      const policy=sqlTransactionPolicy({...this.policy,budgetMs:remaining,
        transactionTimeoutMs:Math.min(remaining,this.policy.transactionTimeoutMs),
        statementTimeoutMs:Math.min(remaining,this.policy.statementTimeoutMs),
        lockTimeoutMs:Math.min(remaining,this.policy.lockTimeoutMs)});
      try {
        return await runSqlTransaction(this.orm, policy,
          async em => this.processInTransaction(em, command, context, diagnostic), { readOnly: false });
      } catch (error) {
        // A identidade do provedor pode disputar outra wallet. Antes de ler o vencedor,
        // o PostgreSQL já desfez a transação perdedora.
        if (!identityCollision(error) || attempt >= this.policy.maxAttempts) throw error;
        await Bun.sleep(this.policy.retryBaseMs * 2 ** (attempt - 1));
      }
    }
  }

  async auditPermanentFailure(command: CreateWagerTransactionProps, rawContext: ProcessWagerContext,
    diagnostic: WagerFailureDiagnostic, budgetMs=this.policy.budgetMs): Promise<ProcessWagerResult> {
    return this.process(command,rawContext,diagnostic,budgetMs);
  }

  get budgetMs(): number {return this.policy.budgetMs;}

  withMessage<T>(metadata: InboxMessageMetadata, work: () => Promise<T>): Promise<T> {
    return this.message.run(metadata, work);
  }

  private async processInTransaction(
    em: EntityManager,
    command: CreateWagerTransactionProps,
    context: InboxMessageContext,
    diagnostic?: WagerFailureDiagnostic,
  ): Promise<ProcessWagerResult> {
    await sqlQuery(em, `INSERT INTO wager_message_inbox(consumer_name,message_id,envelope_hash)
      VALUES (?,?,?) ON CONFLICT (consumer_name,message_id) DO NOTHING`,
    [context.consumerName, context.messageId, context.envelopeHash]);

    const [row] = await sqlQuery(em, `SELECT envelope_hash,result_snapshot,processed_at,received_at
      FROM wager_message_inbox WHERE consumer_name=? AND message_id=? FOR UPDATE`,
    [context.consumerName, context.messageId]) as InboxRow[];
    if (!row) throw new Error('Inbox row disappeared inside its transaction.');
    if (row.envelope_hash !== context.envelopeHash) throw new InboxConflictError();
    const inbox=InboxMessage.rehydrate({messageId:context.messageId,consumerName:context.consumerName,
      payloadHash:row.envelope_hash,receivedAt:new Date(row.received_at),
      ...(row.processed_at===null?{}:{processedAt:new Date(row.processed_at)})});
    if (inbox.isProcessed()) {
      if (!row.result_snapshot) throw new Error('Processed inbox row has no result snapshot.');
      return row.result_snapshot;
    }

    const result = await this.financial.processInTransaction(em, command, context, diagnostic);
    inbox.markProcessed(new Date(Math.max(Date.now(),inbox.receivedAt.getTime())));
    await sqlQuery(em, `UPDATE wager_message_inbox SET result_snapshot=?::jsonb,processed_at=?
      WHERE consumer_name=? AND message_id=?`,
    [canonicalJson(result), inbox.processedAt, context.consumerName, context.messageId]);
    return result;
  }
}

/** Metadados de inbox não alteram a validação e as regras do caso financeiro. */
export class ProcessInboxWager {
  private readonly useCase: ProcessWager;
  constructor(private readonly store: SqlInboxProcessWagerStore) {
    this.useCase = new ProcessWager(store);
  }
  execute(input: unknown, idempotencyKey: unknown, metadata: InboxMessageMetadata): Promise<ProcessWagerResult> {
    return this.store.withMessage(metadata, async () => {
      const deadline=Date.now()+this.store.budgetMs;
      try { return await this.useCase.execute(input,idempotencyKey,metadata.messageId); }
      catch(error) {
        if(!metadata.finalAttempt || !isKnownPermanentWagerFailure(error)) throw error;
        return this.store.auditPermanentFailure(parseWager(input,idempotencyKey),{correlationId:metadata.messageId},
          permanentWagerDiagnostic(metadata.receiveCount!),deadline-Date.now());
      }
    });
  }
}
