/** IDs são excluídos dos labels e recusados no registro, evitando cardinalidade ilimitada. */
export const ALLOWED_METRIC_LABELS: ReadonlySet<string> = new Set([
  'channel', 'status', 'kind', 'disposition', 'error_code', 'failure_code', 'layer', 'outcome', 'currency',
  'method', 'route', 'status_code', 'dependency', 'queue', 'state', 'reason', 'consumer', 'component', 'collector',
]);

const METRIC_NAME = /^jwp_[a-z0-9_]+$/;
const SAFE_VALUE = /^[A-Za-z0-9_.:/-]{1,64}$/;
const UUID_LIKE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
const LONG_NUMBER = /[0-9]{6,}/;
/** Excedentes de séries convergem para overflow, limitando o uso de memória. */
export const MAX_SERIES_PER_METRIC = 200;
export const DEFAULT_SECONDS_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] as const;

export class MetricDefinitionError extends Error {
  constructor(readonly code: 'METRIC_NAME_INVALID' | 'METRIC_LABEL_NOT_ALLOWED' | 'METRIC_REDEFINED') {
    super(code); this.name = 'MetricDefinitionError';
  }
}

export type Labels = Readonly<Record<string, string | undefined>>;

/** Valores com aparência de ID ou fora do alfabeto seguro viram other. */
export function metricLabelValue(value: string | undefined): string {
  if (value === undefined || value.length === 0) return 'none';
  return SAFE_VALUE.test(value) && !UUID_LIKE.test(value) && !LONG_NUMBER.test(value) ? value : 'other';
}

type Kind = 'counter' | 'gauge' | 'histogram';

export abstract class Metric {
  protected readonly series = new Map<string, { labels: string[]; value: number; buckets?: number[]; sum?: number }>();
  constructor(readonly name: string, readonly help: string, readonly labelNames: readonly string[], readonly kind: Kind) {}

  /** Ignora labels não declarados para impedir IDs inseridos por engano. */
  protected entry(labels: Labels) {
    let values = this.labelNames.map(name => metricLabelValue(labels[name]));
    let key = values.join('\u0000');
    if (!this.series.has(key) && this.series.size >= MAX_SERIES_PER_METRIC) {
      values = this.labelNames.map(() => 'overflow');
      key = values.join('\u0000');
    }
    let found = this.series.get(key);
    if (!found) { found = this.fresh(values); this.series.set(key, found); }
    return found;
  }

  protected fresh(labels: string[]): { labels: string[]; value: number; buckets?: number[]; sum?: number } {
    return { labels, value: 0 };
  }

  value(labels: Labels = {}): number {
    const key = this.labelNames.map(name => metricLabelValue(labels[name])).join('\u0000');
    return this.series.get(key)?.value ?? 0;
  }

  protected format(labels: string[], extra: Record<string, string> = {}): string {
    const pairs = [...this.labelNames.map((name, index) => [name, labels[index]!] as const), ...Object.entries(extra)];
    if (pairs.length === 0) return '';
    return `{${pairs.map(([name, value]) => `${name}="${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`).join(',')}}`;
  }

  render(): string {
    const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} ${this.kind}`];
    for (const item of this.series.values()) lines.push(...this.renderSeries(item));
    return lines.join('\n');
  }

  protected renderSeries(item: { labels: string[]; value: number }): string[] {
    return [`${this.name}${this.format(item.labels)} ${item.value}`];
  }
}

export class Counter extends Metric {
  constructor(name: string, help: string, labelNames: readonly string[]) { super(name, help, labelNames, 'counter'); }
  inc(labels: Labels = {}, amount = 1): void {
    if (!Number.isFinite(amount) || amount < 0) return;
    this.entry(labels).value += amount;
  }
}

export class Gauge extends Metric {
  constructor(name: string, help: string, labelNames: readonly string[]) { super(name, help, labelNames, 'gauge'); }
  set(labels: Labels, value: number): void {
    if (Number.isFinite(value)) this.entry(labels).value = value;
  }
}

export class Histogram extends Metric {
  constructor(name: string, help: string, labelNames: readonly string[], readonly bounds: readonly number[]) {
    super(name, help, labelNames, 'histogram');
  }
  protected override fresh(labels: string[]) { return { labels, value: 0, buckets: this.bounds.map(() => 0), sum: 0 }; }
  observe(labels: Labels, seconds: number): void {
    if (!Number.isFinite(seconds) || seconds < 0) return;
    const item = this.entry(labels);
    item.value += 1;
    item.sum! += seconds;
    this.bounds.forEach((bound, index) => { if (seconds <= bound) item.buckets![index]! += 1; });
  }
  protected override renderSeries(item: { labels: string[]; value: number; buckets?: number[]; sum?: number }): string[] {
    return [
      ...this.bounds.map((bound, index) => `${this.name}_bucket${this.format(item.labels, { le: String(bound) })} ${item.buckets![index]}`),
      `${this.name}_bucket${this.format(item.labels, { le: '+Inf' })} ${item.value}`,
      `${this.name}_sum${this.format(item.labels)} ${item.sum}`,
      `${this.name}_count${this.format(item.labels)} ${item.value}`,
    ];
  }
}

export type Collector = (signal: AbortSignal) => Promise<void>;

/** Cada coletor tem prazo próprio; falha de coleta fica visível na exposição agregada. */
export class MetricsRegistry {
  readonly #metrics = new Map<string, Metric>();
  readonly #collectors = new Map<string, Collector>();
  readonly #collectorUp: Gauge;

  constructor() {
    this.#collectorUp = this.gauge('jwp_metrics_collector_up', 'Last scrape-time sample of each collector succeeded (1) or failed (0)', ['collector']);
  }

  counter(name: string, help: string, labelNames: readonly string[] = []): Counter {
    return this.define(name, labelNames, 'counter', () => new Counter(name, help, labelNames)) as Counter;
  }

  gauge(name: string, help: string, labelNames: readonly string[] = []): Gauge {
    return this.define(name, labelNames, 'gauge', () => new Gauge(name, help, labelNames)) as Gauge;
  }

  histogram(name: string, help: string, labelNames: readonly string[] = [], bounds: readonly number[] = DEFAULT_SECONDS_BUCKETS): Histogram {
    return this.define(name, labelNames, 'histogram', () => new Histogram(name, help, labelNames, bounds)) as Histogram;
  }

  addCollector(name: string, collector: Collector): void {
    if (!SAFE_VALUE.test(name)) throw new MetricDefinitionError('METRIC_NAME_INVALID');
    this.#collectors.set(name, collector);
  }

  get(name: string): Metric | undefined { return this.#metrics.get(name); }

  async collect(timeoutMs = 1000): Promise<void> {
    await Promise.all([...this.#collectors].map(async ([name, collector]) => {
      try {
        await deadline(collector, timeoutMs);
        this.#collectorUp.set({ collector: name }, 1);
      } catch {
        this.#collectorUp.set({ collector: name }, 0);
      }
    }));
  }

  async render(timeoutMs = 1000): Promise<string> {
    await this.collect(timeoutMs);
    return `${[...this.#metrics.values()].map(metric => metric.render()).join('\n')}\n`;
  }

  private define(name: string, labelNames: readonly string[], kind: Kind, create: () => Metric): Metric {
    if (!METRIC_NAME.test(name)) throw new MetricDefinitionError('METRIC_NAME_INVALID');
    for (const label of labelNames) if (!ALLOWED_METRIC_LABELS.has(label)) throw new MetricDefinitionError('METRIC_LABEL_NOT_ALLOWED');
    const existing = this.#metrics.get(name);
    if (existing) {
      if (existing.kind !== kind || existing.labelNames.join() !== labelNames.join()) throw new MetricDefinitionError('METRIC_REDEFINED');
      return existing;
    }
    const metric = create();
    this.#metrics.set(name, metric);
    return metric;
  }
}

/** O prazo encerra a espera mesmo se o trabalho ignorar AbortSignal. */
export async function deadline<T>(work: (signal: AbortSignal) => Promise<T>, timeoutMs: number): Promise<T> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error('DEADLINE_EXCEEDED');
      error.name = 'DeadlineExceeded';
      controller.abort(error);
      reject(error);
    }, timeoutMs);
  });
  try {
    return await Promise.race([work(controller.signal), expired]);
  } finally {
    clearTimeout(timer);
  }
}
