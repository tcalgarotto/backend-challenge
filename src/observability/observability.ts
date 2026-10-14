import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { ReconciliationMonitor, ReconciliationResult } from '../wallet/application/reconcile-wallet.js';
import { runSqlTransaction, sqlQuery, sqlTransactionPolicy, type SqlTransactionPolicy } from '../shared/infrastructure/sql-transaction.js';
import { JsonLogger } from './log.js';
import { sqlFailureReason } from './messaging.js';
import { MetricsRegistry } from './metrics.js';
import { PostgresProbe, ReadinessService, SqsProbe, type DependencyProbe, type Dependency } from './readiness.js';

/** Labels são dimensões agregadas; IDs não fazem parte das métricas. */
export function jwpMetrics(registry: MetricsRegistry) {
  return {
    httpRequests: registry.counter('jwp_http_requests_total', 'HTTP responses by method, route template and status code', ['method', 'route', 'status_code']),
    httpDuration: registry.histogram('jwp_http_request_duration_seconds', 'HTTP latency by method and route template', ['method', 'route']),
    transactions: registry.counter('jwp_wager_transactions_total', 'Wager results by channel, persisted status and kind', ['channel', 'status', 'kind']),
    processing: registry.histogram('jwp_wager_processing_duration_seconds', 'Wager processing latency by channel and outcome', ['channel', 'outcome']),
    duplicates: registry.counter('jwp_duplicates_total', 'Duplicates absorbed, by layer (idempotency, sqs_redelivery, event_inbox)', ['layer']),
    retries: registry.counter('jwp_retries_total', 'Retries scheduled, by component', ['component']),
    sqsMessages: registry.counter('jwp_sqs_messages_total', 'Command messages by consumer, disposition and error code', ['consumer', 'disposition', 'error_code']),
    eventMessages: registry.counter('jwp_event_messages_total', 'Integration event messages by disposition and error code', ['disposition', 'error_code']),
    queueMessages: registry.gauge('jwp_sqs_queue_messages', 'Approximate messages per configured queue (DLQs included), by state', ['queue', 'state']),
    sqlTransient: registry.counter('jwp_sql_transient_failures_total', 'Transient SQL failures that exhausted retries, by channel and reason', ['channel', 'reason']),
    lockConflicts: registry.counter('jwp_lock_conflicts_total', 'SQL statements aborted by lock contention (lock_timeout, deadlock, serialization), every attempt including retried ones', ['reason']),
    lockWaiters: registry.gauge('jwp_postgres_lock_waiters', 'PostgreSQL backends currently waiting for a lock'),
    outboxPending: registry.gauge('jwp_outbox_pending_events', 'Outbox events not yet published'),
    outboxOldestAge: registry.gauge('jwp_outbox_oldest_pending_age_seconds', 'Age of the oldest unpublished outbox event (0 when empty)'),
    outboxPublications: registry.counter('jwp_outbox_publications_total', 'Outbox publication attempts by result', ['status']),
    pendingReferences: registry.counter('jwp_pending_references_total', 'Pending-reference scheduler outcomes', ['status', 'failure_code']),
    reconciliations: registry.counter('jwp_wallet_reconciliations_total', 'Wallet reconciliations by outcome and currency', ['outcome', 'currency']),
    dependencyUp: registry.gauge('jwp_dependency_up', 'Last readiness result per dependency (1 up, 0 down)', ['dependency']),
    readinessDuration: registry.histogram('jwp_readiness_check_duration_seconds', 'Readiness probe latency by dependency', ['dependency']),
  };
}
export type JwpMetrics = ReturnType<typeof jwpMetrics>;

export interface ObservedQueue {
  /** Nome de fila configurado limita cardinalidade, ao contrário de IDs de mensagens. */
  readonly name: string;
  readonly url: string;
}

export interface ObservabilityOptions {
  readonly orm?: MikroORM;
  readonly sqs?: { readonly client: SQSClient; readonly queues: readonly ObservedQueue[] };
  readonly readinessTimeoutMs?: number;
  readonly requiredDependencies?: readonly Dependency[];
  readonly logger?: JsonLogger;
}

/** Divergência chega a log e contador sem alterar o resultado da reconciliação. */
export class ObservedReconciliationMonitor implements ReconciliationMonitor {
  constructor(private readonly metrics: JwpMetrics, private readonly logger: JsonLogger) {}
  observe(result: ReconciliationResult, context: { readonly correlationId: string }): void {
    const outcome = result.consistent ? 'consistent' : 'divergent';
    this.metrics.reconciliations.inc({ outcome, currency: result.storedBalance.currency });
    if (result.consistent) return;
    this.logger.error('wallet.reconciliation.divergent', { correlationId: context.correlationId, walletId: result.walletId,
      currency: result.storedBalance.currency, checkedEntries: result.checkedEntries, repaired: false });
  }
}

export class Observability {
  readonly registry = new MetricsRegistry();
  readonly metrics: JwpMetrics;
  readonly logger: JsonLogger;
  readonly readiness: ReadinessService;
  readonly reconciliationMonitor: ReconciliationMonitor;
  readonly collectTimeoutMs: number;
  readonly #sqs: SQSClient | undefined;
  private readonly samplePolicy: SqlTransactionPolicy;

  constructor(options: ObservabilityOptions = {}) {
    this.logger = options.logger ?? new JsonLogger();
    this.metrics = jwpMetrics(this.registry);
    this.collectTimeoutMs = options.readinessTimeoutMs ?? 1000;
    // Uma tentativa por scrape; banco lento marca falha em vez de acumular retries.
    this.samplePolicy = sqlTransactionPolicy({ maxAttempts: 1, lockTimeoutMs: this.collectTimeoutMs, statementTimeoutMs: this.collectTimeoutMs,
      transactionTimeoutMs: this.collectTimeoutMs, budgetMs: this.collectTimeoutMs });
    const probes: DependencyProbe[] = [];
    if (options.orm) {
      probes.push(new PostgresProbe(options.orm));
      this.registry.addCollector('postgres', () => this.samplePostgres(options.orm!));
      observeLockConflicts(options.orm, this.metrics);
    }
    if (options.sqs && options.sqs.queues.length > 0) {
      probes.push(new SqsProbe(options.sqs.client, options.sqs.queues.map(queue => queue.url)));
      this.registry.addCollector('sqs', signal => this.sampleQueues(options.sqs!.client, options.sqs!.queues, signal));
    }
    this.readiness = new ReadinessService(probes, this.collectTimeoutMs,
      { up: this.metrics.dependencyUp, duration: this.metrics.readinessDuration, logger: this.logger }, options.requiredDependencies);
    this.reconciliationMonitor = new ObservedReconciliationMonitor(this.metrics, this.logger);
    this.#sqs = options.sqs?.client;
  }

  /** Readiness libera seu cliente SQS; o ORM continua sob responsabilidade da aplicação. */
  close(): void { this.#sqs?.destroy(); }

  /** Consulta agregada somente de leitura como jwp_runtime, sem IDs de linhas. */
  private async samplePostgres(orm: MikroORM): Promise<void> {
    const [row] = await runSqlTransaction(orm, this.samplePolicy, em => sqlQuery(em, `SELECT
      (count(*) FILTER (WHERE published_at IS NULL))::int AS pending,
      COALESCE(EXTRACT(EPOCH FROM now() - min(created_at) FILTER (WHERE published_at IS NULL)), 0)::float8 AS oldest,
      (SELECT count(*) FROM pg_locks WHERE NOT granted)::int AS waiters
      FROM outbox_events`), { readOnly: true });
    this.metrics.outboxPending.set({}, Number(row?.['pending']));
    this.metrics.outboxOldestAge.set({}, Number(row?.['oldest']));
    this.metrics.lockWaiters.set({}, Number(row?.['waiters']));
  }

  private async sampleQueues(client: SQSClient, queues: readonly ObservedQueue[], signal: AbortSignal): Promise<void> {
    await Promise.all(queues.map(async queue => {
      const { Attributes } = await client.send(new GetQueueAttributesCommand({ QueueUrl: queue.url,
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'] }), { abortSignal: signal });
      this.metrics.queueMessages.set({ queue: queue.name, state: 'visible' }, Number(Attributes?.['ApproximateNumberOfMessages'] ?? 0));
      this.metrics.queueMessages.set({ queue: queue.name, state: 'in_flight' }, Number(Attributes?.['ApproximateNumberOfMessagesNotVisible'] ?? 0));
    }));
  }
}


const LOCK_REASONS = new Set(['lock_timeout', 'deadlock', 'serialization']);
/** Uma escuta por ORM; cada comando abortado por disputa de lock soma, mesmo que o retry depois tenha sucesso. */
const observedOrms = new WeakMap<object, JwpMetrics[]>();
export function observeLockConflicts(orm: MikroORM, metrics: JwpMetrics): void {
  const knex = orm.em.getConnection().getKnex() as unknown as { on(event: 'query-error', listener: (error: unknown) => void): unknown };
  let targets = observedOrms.get(knex);
  if (!targets) {
    targets = [];
    observedOrms.set(knex, targets);
    const list = targets;
    knex.on('query-error', error => {
      const reason = sqlFailureReason(error);
      if (reason && LOCK_REASONS.has(reason)) for (const target of list) target.lockConflicts.inc({ reason });
    });
  }
  if (!targets.includes(metrics)) targets.push(metrics);
}

export function observedQueue(url: string): ObservedQueue {
  const name = new URL(url).pathname.split('/').filter(Boolean).at(-1);
  if (!name) throw new Error('Invalid SQS queue URL.');
  return { name, url };
}
