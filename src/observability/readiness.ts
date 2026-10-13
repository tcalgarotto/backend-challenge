import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { failureCode, type JsonLogger } from './log.js';
import { deadline, type Gauge, type Histogram } from './metrics.js';

export type Dependency = 'postgres' | 'sqs';
export type DependencyState = 'up' | 'down' | 'not_configured';
export const REQUIRED_DEPENDENCIES: readonly Dependency[] = ['postgres', 'sqs'];

export interface DependencyProbe {
  readonly dependency: Dependency;
  /** Só confirma após ida e volta real; respeita o sinal quando o cliente permite. */
  check(signal: AbortSignal): Promise<void>;
}

/** Usa o mesmo pool das rotas financeiras para provar sua disponibilidade. */
export class PostgresProbe implements DependencyProbe {
  readonly dependency = 'postgres' as const;
  constructor(private readonly orm: MikroORM) {}
  async check(): Promise<void> {
    await this.orm.em.fork().getConnection().execute('SELECT 1', [], 'all');
  }
}

/** GetQueueAttributes verifica existência e acesso a cada fila configurada. */
export class SqsProbe implements DependencyProbe {
  readonly dependency = 'sqs' as const;
  constructor(private readonly client: SQSClient, private readonly queueUrls: readonly string[]) {
    if (queueUrls.length === 0) throw new Error('SQS readiness requires at least one queue URL.');
  }
  async check(signal: AbortSignal): Promise<void> {
    await Promise.all(this.queueUrls.map(QueueUrl =>
      this.client.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }), { abortSignal: signal })));
  }
}

export interface ReadinessReport {
  readonly ready: boolean;
  readonly checks: Readonly<Record<Dependency, DependencyState>>;
}

/** Cada chamada consulta dependências com prazo limitado; probe ausente reprova readiness. */
export class ReadinessService {
  readonly #last = new Map<Dependency, DependencyState>();

  constructor(
    private readonly probes: readonly DependencyProbe[],
    private readonly timeoutMs: number,
    private readonly signals: { readonly up: Gauge; readonly duration: Histogram; readonly logger: JsonLogger },
    private readonly requiredDependencies: readonly Dependency[] = REQUIRED_DEPENDENCIES,
  ) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000) throw new Error('Invalid readiness timeout.');
  }

  async check(): Promise<ReadinessReport> {
    const states = await Promise.all(REQUIRED_DEPENDENCIES.map(async dependency => {
      const probe = this.probes.find(candidate => candidate.dependency === dependency);
      if (!probe) return [dependency, 'not_configured', undefined] as const;
      const started = performance.now();
      try {
        await deadline(signal => probe.check(signal), this.timeoutMs);
        return [dependency, 'up', undefined] as const;
      } catch (error) {
        return [dependency, 'down', failureCode(error)] as const;
      } finally {
        this.signals.duration.observe({ dependency }, (performance.now() - started) / 1000);
      }
    }));
    const checks = {} as Record<Dependency, DependencyState>;
    for (const [dependency, state, errorCode] of states) {
      checks[dependency] = state;
      this.signals.up.set({ dependency }, state === 'up' ? 1 : 0);
      if (this.#last.get(dependency) !== state) {
        // Registra somente transições para não inundar o log durante polling.
        if (state === 'up') this.signals.logger.info('dependency.up', { dependency });
        else this.signals.logger.warn('dependency.unavailable', { dependency, status: state, errorCode });
        this.#last.set(dependency, state);
      }
    }
    return { ready: this.requiredDependencies.every(dependency => checks[dependency] === 'up'), checks };
  }
}
