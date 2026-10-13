import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { SQSClient } from '@aws-sdk/client-sqs';
import type { INestApplication } from '@nestjs/common';
import { SqlOutboxStore } from '../../../src/messaging/outbox/sql-outbox-store.js';
import { ObservedSqsOutboxPublisher } from '../../../src/observability/messaging.js';
import { Observability, type ObservedQueue } from '../../../src/observability/observability.js';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { call, contractQueue, docker, infrastructure, logCapture, migratedOrm, observedApp, requireSqs, sample, sqsClient,
  until, waitPostgres, wager, type Orm } from './support.js';

const READINESS_TIMEOUT_MS = 500;

const BOUND_MS = READINESS_TIMEOUT_MS + 1500;
const env = infrastructure();
let orm: Orm, app: INestApplication, base: string, observability: Observability;
let probeClient: SQSClient, publisherClient: SQSClient, queues: ObservedQueue[];
const logs = logCapture();

beforeAll(async () => {
  probeClient = sqsClient(env.sqsEndpoint);
  publisherClient = sqsClient(env.sqsEndpoint);
  await requireSqs(probeClient);
  queues = await Promise.all(['wager-transactions.fifo', 'wager-transactions-dlq.fifo', 'wager-events.fifo', 'wager-events-dlq.fifo']
    .map(name => contractQueue(probeClient, name)));
  orm = await migratedOrm(env.databaseUrl);
  observability = new Observability({ orm, sqs: { client: probeClient, queues }, readinessTimeoutMs: READINESS_TIMEOUT_MS, logger: logs.logger });
  ({ app, base } = await observedApp(orm, observability));
}, 30000);

afterAll(async () => {

  Bun.spawnSync(['docker', 'unpause', env.sqsContainer]);
  Bun.spawnSync(['docker', 'unpause', env.postgresContainer]);
  Bun.spawnSync(['docker', 'start', env.postgresContainer]);
  await waitPostgres(env.postgresContainer);
  if (app) await app.close(); else if (orm) await orm.close(true);
  publisherClient?.destroy();
  const cleanup = await openFinancialOrm(env.databaseUrl);
  try { await cleanup.migrator.down({ to: 0 }); } finally { await cleanup.close(true); }
}, 60000);

const ready = () => call(base, 'GET', '/health/ready');
const live = () => call(base, 'GET', '/health/live');
const metrics = async () => (await call(base, 'GET', '/metrics')).text;
const recovered = () => until(ready, result => result.status === 200, 30000);

test('baseline: live and ready are separate routes and ready reflects real checks', async () => {
  expect(await live()).toMatchObject({ status: 200, body: { status: 'ok' } });
  expect(await ready()).toMatchObject({ status: 200, body: { status: 'ready', checks: { postgres: 'up', sqs: 'up' } } });
  const text = await metrics();
  expect(sample(text, 'jwp_dependency_up{dependency="postgres"}')).toBe(1);
  expect(sample(text, 'jwp_dependency_up{dependency="sqs"}')).toBe(1);
  expect(sample(text, 'jwp_metrics_collector_up{collector="sqs"}')).toBe(1);
  expect(sample(text, 'jwp_metrics_collector_up{collector="postgres"}')).toBe(1);
});

test('O03 + outbox lag: SQS withdrawn -> ready 503 within the deadline, live 200; restored -> ready 200 and the outbox drains', async () => {
  const playerId = randomUUID();
  const opened = await call(base, 'POST', '/wallets', { playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });
  expect(opened.status).toBe(201);
  const bet = await call(base, 'POST', '/wagering/transactions', wager({ id: opened.body.id, playerId }, 'BET', '10.00'),
    { 'Idempotency-Key': `obs-${randomUUID()}` });
  expect(bet.status).toBe(200);
  const expectedOutbox = (await orm.em.fork().getConnection().execute(`SELECT
    (count(*) FILTER (WHERE published_at IS NULL))::int pending,
    COALESCE(EXTRACT(EPOCH FROM now() - min(created_at) FILTER (WHERE published_at IS NULL)), 0)::float8 oldest
    FROM outbox_events`))[0]!;
  const publisher = new ObservedSqsOutboxPublisher(publisherClient, queues.find(queue => queue.name === 'wager-events.fifo')!.url,
    new SqlOutboxStore(orm, 2000), { sendTimeoutMs: 400, retryBaseMs: 1, retryMaxMs: 1 }, observability);
  const pendingBefore = sample(await metrics(), 'jwp_outbox_pending_events');
  expect(pendingBefore).toBe(Number(expectedOutbox.pending));
  expect(sample(await metrics(), 'jwp_outbox_oldest_pending_age_seconds')).toBeCloseTo(Number(expectedOutbox.oldest), 1);

  docker('pause', env.sqsContainer);
  try {
    const down = await ready();
    expect(down).toMatchObject({ status: 503, body: { status: 'unavailable', checks: { postgres: 'up', sqs: 'down' } } });
    expect(down.ms).toBeLessThan(BOUND_MS);
    const alive = await live();
    expect(alive).toMatchObject({ status: 200, body: { status: 'ok' } });
    expect(alive.ms).toBeLessThan(500);

    const attempts = await publisher.publishBatch();
    expect(attempts.length).toBeGreaterThanOrEqual(1);

    expect(attempts.some(attempt => attempt.status === 'RETRY')).toBe(true);
    expect(attempts.every(attempt => attempt.status === 'RETRY' || attempt.status === 'DEFERRED')).toBe(true);
    await Bun.sleep(1100);
    const scrape = await call(base, 'GET', '/metrics');
    expect(scrape.ms).toBeLessThan(BOUND_MS);
    expect(sample(scrape.text, 'jwp_dependency_up{dependency="sqs"}')).toBe(0);
    expect(sample(scrape.text, 'jwp_metrics_collector_up{collector="sqs"}')).toBe(0);
    expect(sample(scrape.text, 'jwp_outbox_pending_events')).toBe(pendingBefore);
    expect(sample(scrape.text, 'jwp_outbox_oldest_pending_age_seconds')).toBeGreaterThan(1);
    expect(sample(scrape.text, 'jwp_outbox_publications_total{status="RETRY"}')).toBeGreaterThanOrEqual(1);
    expect(sample(scrape.text, 'jwp_retries_total{component="outbox_publisher"}')).toBeGreaterThanOrEqual(1);
  } finally {
    docker('unpause', env.sqsContainer);
  }

  expect(await recovered()).toMatchObject({ status: 200, body: { checks: { postgres: 'up', sqs: 'up' } } });
  const drained = await until(async () => { await publisher.publishBatch(new Date(Date.now() + 60_000));
    return sample(await metrics(), 'jwp_outbox_pending_events'); }, pending => pending === 0, 20000);
  expect(drained).toBe(0);
  const text = await metrics();
  expect(sample(text, 'jwp_outbox_publications_total{status="PUBLISHED"}')).toBeGreaterThanOrEqual(pendingBefore);
  expect(sample(text, 'jwp_outbox_oldest_pending_age_seconds')).toBe(0);
  expect(sample(text, 'jwp_sqs_queue_messages{queue="wager-events.fifo",state="visible"}')).toBeGreaterThanOrEqual(pendingBefore);
  expect(sample(text, 'jwp_dependency_up{dependency="sqs"}')).toBe(1);
}, 90000);

test('O03: PostgreSQL killed -> ready 503 and financial routes 503 while live stays 200; restarted -> ready 200', async () => {
  // Aquece conexões para que a indisponibilidade atinja sockets já ociosos.
  await Promise.all([1, 2, 3].map(() => orm.em.fork().getConnection().execute('SELECT pg_sleep(0.05)')));
  docker('kill', '--signal=KILL', env.postgresContainer);
  try {
    const down = await ready();
    expect(down).toMatchObject({ status: 503, body: { status: 'unavailable', checks: { postgres: 'down', sqs: 'up' } } });
    expect(down.ms).toBeLessThan(BOUND_MS);
    expect(await live()).toMatchObject({ status: 200, body: { status: 'ok' } });
    const financial = await call(base, 'POST', '/wallets', { playerId: randomUUID(), initialBalance: { amount: '1.00', currency: 'BRL' } });
    expect(financial).toMatchObject({ status: 503, body: { error: { code: 'INFRASTRUCTURE_UNAVAILABLE', retryable: true } } });
    const scrape = await call(base, 'GET', '/metrics');
    expect(scrape.ms).toBeLessThan(BOUND_MS);
    expect(sample(scrape.text, 'jwp_dependency_up{dependency="postgres"}')).toBe(0);
    expect(sample(scrape.text, 'jwp_metrics_collector_up{collector="postgres"}')).toBe(0);
    expect(sample(scrape.text, 'jwp_http_requests_total{method="POST",route="/wallets",status_code="503"}')).toBeGreaterThanOrEqual(1);
  } finally {
    docker('start', env.postgresContainer);
    await waitPostgres(env.postgresContainer);
  }
  expect(await recovered()).toMatchObject({ status: 200, body: { checks: { postgres: 'up', sqs: 'up' } } });
  expect(sample(await metrics(), 'jwp_dependency_up{dependency="postgres"}')).toBe(1);
}, 90000);

test('O03: a hanging PostgreSQL (paused) cannot hold readiness past its deadline', async () => {
  docker('pause', env.postgresContainer);
  try {
    const down = await ready();
    expect(down).toMatchObject({ status: 503, body: { checks: { postgres: 'down', sqs: 'up' } } });
    expect(down.ms).toBeLessThan(BOUND_MS);
    expect((await live()).status).toBe(200);
  } finally {
    docker('unpause', env.postgresContainer);
  }
  expect(await recovered()).toMatchObject({ status: 200, body: { checks: { postgres: 'up', sqs: 'up' } } });
}, 60000);

test('dependency transitions were logged once each, with codes and without credentials or endpoints', () => {
  const entries = logs.entries();
  const unavailable = entries.filter(entry => entry['type'] === 'dependency.unavailable');
  expect(unavailable.map(entry => entry['dependency'])).toEqual(['sqs', 'postgres', 'postgres']);
  for (const entry of unavailable) {
    expect(entry['level']).toBe('warn');
    expect(typeof entry['errorCode']).toBe('string');
  }
  const serialized = logs.lines.join('');
  for (const secret of ['jwp_local', '127.0.0.1', 'postgresql://', 'localhost']) expect(serialized).not.toContain(secret);
});

test('the real entrypoint wires live, ready and metrics against the dedicated dependencies', async () => {
  const cwd = new URL('../../../', import.meta.url).pathname;
  const child = Bun.spawn(['bun', 'src/main.ts'], { cwd, stdout: 'pipe', stderr: 'pipe', env: { ...Bun.env,
    PORT: '0', JWP_DATABASE_URL: env.databaseUrl, JWP_SQS_ENDPOINT: env.sqsEndpoint, JWP_READINESS_TIMEOUT_MS: '500',
    JWP_SQS_QUEUE_URLS: queues.map(queue => queue.url).join(','), AWS_ACCESS_KEY_ID: 'test', AWS_SECRET_ACCESS_KEY: 'test' } });
  const reader = child.stdout.getReader();
  try {
    let output = '';
    const deadline = Date.now() + 15000;
    while (!output.includes('"listening"')) {
      if (Date.now() > deadline) throw new Error(`entrypoint did not listen: ${output}`);
      const chunk = await reader.read();
      if (chunk.done) throw new Error(await new Response(child.stderr).text());
      output += new TextDecoder().decode(chunk.value);
    }
    const line = output.split('\n').find(candidate => candidate.includes('"listening"'))!;
    const url = `http://127.0.0.1:${(JSON.parse(line) as { port: number }).port}`;
    expect(await call(url, 'GET', '/health/live')).toMatchObject({ status: 200, body: { status: 'ok' } });
    expect(await call(url, 'GET', '/health/ready')).toMatchObject({ status: 200, body: { status: 'ready', checks: { postgres: 'up', sqs: 'up' } } });
    const exposed = await call(url, 'GET', '/metrics');
    expect(exposed.status).toBe(200);
    expect(exposed.text).toContain('# TYPE jwp_http_requests_total counter');
    expect(sample(exposed.text, 'jwp_sqs_queue_messages{queue="wager-transactions-dlq.fifo",state="visible"}')).toBeGreaterThanOrEqual(0);
    expect(sample(exposed.text, 'jwp_metrics_collector_up{collector="sqs"}')).toBe(1);
  } finally {
    reader.releaseLock();
    child.kill('SIGTERM');
    await child.exited;
  }
}, 30000);
