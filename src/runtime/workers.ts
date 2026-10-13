import { loadRuntimeConfig, type WorkerConfig } from '../config/env.js';
export type { WorkerConfig } from '../config/env.js';
import type { SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { setTimeout as delay } from 'node:timers/promises';
import { ProcessInboxWager, SqlInboxProcessWagerStore } from '../messaging/sql-inbox-process-wager-store.js';
import { defaultSqsWagerConsumerOptions } from '../messaging/sqs-wager-consumer.js';
import { SqlOutboxStore } from '../messaging/outbox/sql-outbox-store.js';
import { ObservedPendingReferenceScheduler, ObservedSqsOutboxPublisher, ObservedSqsWagerConsumer } from '../observability/messaging.js';
import type { Observability } from '../observability/observability.js';
import { failureCode } from '../observability/log.js';

/** Validar antes de abrir recursos evita inicialização parcial; HTTP pode operar separado. */
export function runtimeConfig(env: Record<string, string | undefined>): { http: boolean; workers?: WorkerConfig } {
  return loadRuntimeConfig(env);
}

/** Fecha somente os loops; a aplicação libera SQL/SQS compartilhados após o drain. */
export class ApplicationWorkers {
  private readonly consumer: ObservedSqsWagerConsumer;
  private readonly publisher: ObservedSqsOutboxPublisher;
  private readonly scheduler: ObservedPendingReferenceScheduler;
  private readonly idle = new AbortController();
  private loops: Promise<void>[] = [];
  private stopping = false;
  private drain: Promise<void> | undefined;

  constructor(orm: MikroORM, sqs: SQSClient, private readonly config: WorkerConfig, private readonly observation: Observability) {
    this.consumer = new ObservedSqsWagerConsumer(sqs, config.commandsUrl,
      new ProcessInboxWager(new SqlInboxProcessWagerStore(orm)), {}, observation);
    this.publisher = new ObservedSqsOutboxPublisher(sqs, config.eventsUrl, new SqlOutboxStore(orm), {}, observation);
    this.scheduler = new ObservedPendingReferenceScheduler(orm, {}, observation);
  }

  start(): void {
    if (this.loops.length || this.stopping) throw new Error('WORKERS_ALREADY_STARTED');
    this.loops = [
      // Lote cheio com todos os acks indica backlog: consumer continua serial,
      // mas não pausa artificialmente nesse caminho de sucesso.
      // Lote vazio, parcial ou com falha mantém a pausa e o heartbeat.
      this.repeat('consumer', () => this.consumer.pollOnce(), observations =>
        observations.length === defaultSqsWagerConsumerOptions.maxMessages &&
        observations.every(observation => observation.disposition === 'ACKNOWLEDGED')),
      // Lote cheio e publicado indica backlog; reivindicar novamente evita o teto
      // artificial de publicação. RETRY ou LOST_LEASE mantém a pausa para
      // não pressionar o SQS indisponível.
      this.repeat('outbox', () => this.publisher.publishBatch(), published =>
        published.length >= this.publisher.batchSize && published.every(p => p.status === 'PUBLISHED')),
      this.repeat('scheduler', () => this.scheduler.runOnce()),
    ];
    this.observation.logger.info('workers.started', { component: 'consumer,outbox,scheduler' });
  }

  stop(): Promise<void> {
    return this.drain ??= this.stopAndDrain();
  }

  private async stopAndDrain(): Promise<void> {
    this.stopping = true;
    this.idle.abort();
    const consumer = await this.consumer.stop();
    await Promise.all(this.loops);
    this.observation.logger.info('workers.stopped', { consumer, status: 'DRAINED' });
  }

  private async repeat<T>(component: string, work: () => Promise<T>, backlog: (result: T) => boolean = () => false): Promise<void> {
    while (!this.stopping) {
      let busy = false;
      try { busy = backlog(await work()); }
      catch (error) {
        if (!this.stopping) this.observation.logger.warn('worker.failed', { component, errorCode: failureCode(error) });
      }
      if (busy) continue;
      if (!this.stopping) await delay(this.config.intervalMs, undefined, { signal: this.idle.signal }).catch(() => undefined);
    }
  }
}
