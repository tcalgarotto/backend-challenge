import { expect, test } from 'bun:test';
import { JsonLogger, sanitizeLogFields } from '../../../src/observability/log.js';
import { MAX_SERIES_PER_METRIC, MetricDefinitionError, MetricsRegistry, metricLabelValue } from '../../../src/observability/metrics.js';
import { ReadinessService, type DependencyProbe } from '../../../src/observability/readiness.js';
import { recordConsumerObservation, recordEventConsumption, sqlFailureReason, wagerKind } from '../../../src/observability/messaging.js';
import { Observability } from '../../../src/observability/observability.js';

const capture = () => {
  const lines: string[] = [];
  return { lines, logger: new JsonLogger(line => { lines.push(line); }, () => new Date('2026-10-11T00:00:00.000Z')),
    parsed: () => lines.map(line => JSON.parse(line) as Record<string, unknown>) };
};

test('logger emits one JSON line with only allow-listed fields; money and payloads have no slot', () => {
  const { lines, logger, parsed } = capture();
  const untyped = { walletId: 'w-1', transactionId: 't-1', amount: '1234.56', balance: { amount: '9.99' }, money: '1.00',
    envelope: '{"amount":"5.00"}', body: { x: 1 }, password: 'secret', status: 'PROCESSED', durationMs: 12, receiveCount: '2' };
  logger.info('wager.processed', untyped as never);
  expect(lines).toHaveLength(1);
  expect(lines[0]!.endsWith('\n')).toBe(true);
  expect(parsed()[0]).toEqual({ level: 'info', type: 'wager.processed', at: '2026-10-11T00:00:00.000Z',
    walletId: 'w-1', transactionId: 't-1', password: '[REDACTED]', status: 'PROCESSED', durationMs: 12 });
  for (const forbidden of ['1234.56', '9.99', 'amount', 'balance', 'secret', 'envelope']) expect(lines[0]).not.toContain(forbidden);
});

test('absent identifiers stay absent instead of being invented', () => {
  expect(sanitizeLogFields({ correlationId: undefined, messageId: '', walletId: 'w' })).toEqual({ walletId: 'w' });
});

test('logger refuses free-form event types and never throws from a broken sink', () => {
  const { logger, parsed } = capture();
  logger.warn('Bad Type {amount: 5}');
  expect(parsed()[0]!['type']).toBe('invalid_event_type');
  expect(() => new JsonLogger(() => { throw new Error('disk full'); }).error('x.y')).not.toThrow();
});

test('identifier labels are refused at registration', () => {
  const registry = new MetricsRegistry();
  for (const label of ['walletId', 'wallet_id', 'transactionId', 'transaction_id', 'messageId', 'event_id', 'player_id', 'correlationId']) {
    expect(() => registry.counter('jwp_bad_total', 'bad', [label])).toThrow(MetricDefinitionError);
  }
  expect(() => registry.counter('wallet_total', 'unprefixed')).toThrow(MetricDefinitionError);
  registry.counter('jwp_ok_total', 'ok', ['status']);
  expect(() => registry.gauge('jwp_ok_total', 'redefined', ['status'])).toThrow(MetricDefinitionError);
});

test('undeclared keys are ignored and identifier-shaped values collapse to "other"', async () => {
  const registry = new MetricsRegistry();
  const counter = registry.counter('jwp_things_total', 'things', ['status']);
  counter.inc({ status: 'PROCESSED', walletId: '1b4e28ba-2fa1-11d2-883f-0016d3cca427' });
  counter.inc({ status: '1b4e28ba-2fa1-11d2-883f-0016d3cca427' });
  counter.inc({ status: 'tx-12345678' });
  counter.inc({ status: 'has space' });
  expect(counter.value({ status: 'PROCESSED' })).toBe(1);
  expect(counter.value({ status: 'other' })).toBe(3);
  const text = await registry.render();
  expect(text).not.toMatch(/[0-9a-f]{8}-[0-9a-f]{4}/);
  expect(text).not.toContain('walletId');
  expect(metricLabelValue('/wallets/:walletId')).toBe('/wallets/:walletId');
  expect(metricLabelValue(undefined)).toBe('none');
});

test('series per metric are capped so cardinality cannot grow without bound', () => {
  const counter = new MetricsRegistry().counter('jwp_capped_total', 'capped', ['error_code']);
  for (let index = 0; index < MAX_SERIES_PER_METRIC + 50; index++) counter.inc({ error_code: `E${index}` });
  expect(counter.value({ error_code: 'overflow' })).toBe(50);
});

test('prometheus exposition renders counters, gauges and cumulative histogram buckets', async () => {
  const registry = new MetricsRegistry();
  registry.counter('jwp_a_total', 'a', ['status']).inc({ status: 'X' }, 2);
  registry.gauge('jwp_b', 'b').set({}, 7);
  const histogram = registry.histogram('jwp_c_seconds', 'c', ['channel'], [0.1, 1]);
  histogram.observe({ channel: 'sqs' }, 0.05);
  histogram.observe({ channel: 'sqs' }, 0.5);
  histogram.observe({ channel: 'sqs' }, 5);
  const text = await registry.render();
  expect(text).toContain('# TYPE jwp_a_total counter\njwp_a_total{status="X"} 2');
  expect(text).toContain('jwp_b 7');
  expect(text).toContain('jwp_c_seconds_bucket{channel="sqs",le="0.1"} 1');
  expect(text).toContain('jwp_c_seconds_bucket{channel="sqs",le="1"} 2');
  expect(text).toContain('jwp_c_seconds_bucket{channel="sqs",le="+Inf"} 3');
  expect(text).toContain('jwp_c_seconds_count{channel="sqs"} 3');
});

test('a hanging or failing collector is bounded and reported as down', async () => {
  const registry = new MetricsRegistry();
  registry.addCollector('hangs', () => new Promise(() => undefined));
  registry.addCollector('fails', async () => { throw new Error('boom'); });
  registry.addCollector('works', async () => undefined);
  const started = performance.now();
  const text = await registry.render(100);
  expect(performance.now() - started).toBeLessThan(1000);
  expect(text).toContain('jwp_metrics_collector_up{collector="hangs"} 0');
  expect(text).toContain('jwp_metrics_collector_up{collector="fails"} 0');
  expect(text).toContain('jwp_metrics_collector_up{collector="works"} 1');
});

const probe = (dependency: 'postgres' | 'sqs', check: (signal: AbortSignal) => Promise<void>): DependencyProbe => ({ dependency, check });
const readiness = (probes: DependencyProbe[], timeoutMs = 100) => {
  const observability = new Observability({ logger: new JsonLogger(() => undefined) });
  const { logger, parsed } = capture();
  const service = new ReadinessService(probes, timeoutMs,
    { up: observability.metrics.dependencyUp, duration: observability.metrics.readinessDuration, logger });
  return { service, parsed, metrics: observability.metrics };
};

test('readiness without configured probes is never ready', async () => {
  expect(await readiness([]).service.check()).toEqual({ ready: false, checks: { postgres: 'not_configured', sqs: 'not_configured' } });
  expect((await new Observability({ logger: new JsonLogger(() => undefined) }).readiness.check()).ready).toBe(false);
});

test('a hanging dependency fails readiness within the deadline and its signal is aborted', async () => {
  let aborted = false;
  const { service, metrics } = readiness([probe('postgres', async () => undefined),
    probe('sqs', signal => new Promise(() => { signal.addEventListener('abort', () => { aborted = true; }); }))]);
  const started = performance.now();
  expect(await service.check()).toEqual({ ready: false, checks: { postgres: 'up', sqs: 'down' } });
  expect(performance.now() - started).toBeLessThan(1000);
  expect(aborted).toBe(true);
  expect(metrics.dependencyUp.value({ dependency: 'sqs' })).toBe(0);
  expect(metrics.dependencyUp.value({ dependency: 'postgres' })).toBe(1);
});

test('readiness logs dependency transitions once, with codes and no error text', async () => {
  let fail = true;
  const failure = Object.assign(new Error('connect ECONNREFUSED postgres://user:pass@db'), { code: 'ECONNREFUSED' });
  const { service, parsed } = readiness([probe('postgres', async () => { if (fail) throw failure; }), probe('sqs', async () => undefined)]);
  await service.check(); await service.check();
  fail = false;
  expect(await service.check()).toEqual({ ready: true, checks: { postgres: 'up', sqs: 'up' } });
  const logs = parsed();
  expect(logs.filter(log => log['type'] === 'dependency.unavailable')).toEqual([expect.objectContaining({
    level: 'warn', dependency: 'postgres', status: 'down', errorCode: 'ECONNREFUSED' })]);
  expect(logs.filter(log => log['type'] === 'dependency.up').map(log => log['dependency']).sort()).toEqual(['postgres', 'sqs']);
  expect(JSON.stringify(logs)).not.toContain('pass@');
});

test('transient SQL reasons come from the SQLSTATE cause chain', () => {
  const lock = Object.assign(new Error('x'), { code: '55P03' });
  expect(sqlFailureReason(new Error('outer', { cause: new Error('mid', { cause: lock }) }))).toBe('lock_timeout');
  expect(sqlFailureReason(Object.assign(new Error('y'), { code: '40P01' }))).toBe('deadlock');
  expect(sqlFailureReason(new Error('canceling statement due to lock timeout'))).toBeUndefined();
  expect(wagerKind({ kind: 'BET' })).toBe('BET');
  expect(wagerKind({ kind: '0b7c...' })).toBe('unknown');
});

test('consumer and event observations move retry, redelivery and duplicate counters', () => {
  const observability = new Observability({ logger: new JsonLogger(() => undefined) });
  recordConsumerObservation(observability, 'c', { messageId: 'm1', disposition: 'RETRY', receiveCount: 1, errorCode: 'INFRASTRUCTURE_UNAVAILABLE' });
  recordConsumerObservation(observability, 'c', { messageId: 'm1', disposition: 'ACKNOWLEDGED', receiveCount: 2 });
  recordEventConsumption(observability, { eventId: 'e1', disposition: 'ACKNOWLEDGED', duplicate: true, receiveCount: 2 });
  const { metrics } = observability;
  expect(metrics.retries.value({ component: 'sqs_consumer' })).toBe(1);
  expect(metrics.duplicates.value({ layer: 'sqs_redelivery' })).toBe(1);
  expect(metrics.duplicates.value({ layer: 'event_inbox' })).toBe(1);
  expect(metrics.sqsMessages.value({ consumer: 'c', disposition: 'RETRY', error_code: 'INFRASTRUCTURE_UNAVAILABLE' })).toBe(1);
});
