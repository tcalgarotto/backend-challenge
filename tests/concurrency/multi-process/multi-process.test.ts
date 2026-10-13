import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { SqlEventDeduplicator, SqsEventConsumer, type EventConsumption } from '../../../src/messaging/outbox/sqs-event-consumer.js';
import { sendWagerTransactionRequested } from '../../../src/messaging/wager-message.js';
import { isolatedPair, requireSqs, sqsClient, type QueuePair } from '../../integration/sqs/support/sqs.js';
import {
  Child, call, envelope, httpStats, openWallet, reconcile, requiredInfrastructure, startApi, submit, wager, waitUntil, walletState,
  type ApiInstance, type Line, type Orm,
} from './support.js';

const role = `jwp_concurrency_${randomUUID().replaceAll('-', '')}`;
let orm: Orm, client: SQSClient, commands: QueuePair, events: QueuePair, runtimeUrl: string, migrated = false;
const apis: ApiInstance[] = [], workers: Child[] = [], publishers: Child[] = [], schedulers: Child[] = [];
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<Record<string, any>[]>(sql, args);
const pgArray = (values: readonly string[]) => `{${values.join(',')}}`;
const proof = (name: string, data: Record<string, unknown>) => console.log(JSON.stringify({ proof: name, ...data }));
const children = () => [...apis.map(api => api.child), ...workers, ...publishers, ...schedulers];

beforeAll(async () => {
  const { ownerUrl } = requiredInfrastructure();
  client = sqsClient();
  await requireSqs(client);
  orm = await openFinancialOrm(ownerUrl);
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]!['relation']).toBeNull();
  const migration = Bun.spawn(['bun', 'run', 'db:migrate'], { env: { ...process.env, JWP_MIGRATION_DATABASE_URL: ownerUrl }, stdout: 'pipe', stderr: 'pipe' });
  expect(await migration.exited).toBe(0);
  migrated = true;

  await q(`CREATE ROLE ${role} LOGIN PASSWORD 'concurrency_test_only'`);
  await q(`GRANT jwp_runtime TO ${role}`);
  const url = new URL(ownerUrl);
  url.username = role;
  url.password = 'concurrency_test_only';
  runtimeUrl = url.toString();

  commands = await isolatedPair(client, 'wager-transactions.fifo');
  events = await isolatedPair(client, 'wager-events.fifo');
  const worker = (name: string, script: string, queueUrl?: string) => Child.spawn(name, ['bun', `tests/concurrency/multi-process/${script}`],
    { JWP_WORKER_DATABASE_URL: runtimeUrl, ...(queueUrl ? { JWP_WORKER_QUEUE_URL: queueUrl } : {}) });
  apis.push(...await Promise.all([1, 2, 3].map(i => startApi(`api-${i}`, runtimeUrl))));
  workers.push(...[1, 2, 3].map(i => worker(`sqs-worker-${i}`, 'wager-worker.ts', commands.url)));
  publishers.push(...[1, 2].map(i => worker(`publisher-${i}`, 'publisher-worker.ts', events.url)));
  schedulers.push(...[1, 2, 3].map(i => worker(`scheduler-${i}`, 'scheduler-worker.ts')));
  await Promise.all([...workers, ...publishers, ...schedulers].map(child => child.waitFor(line => line['type'] === 'ready', 'ready')));
}, 90_000);

afterAll(async () => {
  await Promise.all(children().map(child => child.stop()));
  await Promise.all([commands, events].filter(Boolean).map(pair => pair.dispose()));
  client?.destroy();
  if (!orm) return;
  try {
    if (migrated) await orm.migrator.down({ to: 0 });
    await q(`DROP ROLE IF EXISTS ${role}`);
  } finally {
    await orm.close(true);
  }
}, 90_000);

const workerLines = (type: string) => workers.flatMap(worker => worker.of(type));
const send = (messages: ReturnType<typeof envelope>[]) =>
  Promise.all(messages.map(message => sendWagerTransactionRequested(client, commands.url, message)));


async function acknowledged(messageIds: readonly string[], timeoutMs = 45_000): Promise<Map<string, number>> {
  const wanted = new Set(messageIds);
  return waitUntil(async () => {
    const acks = new Map<string, number>();
    for (const line of workerLines('message')) {
      if (line['disposition'] === 'ACKNOWLEDGED' && wanted.has(line['messageId'] as string)) acks.set(line['messageId'] as string, line['pid'] as number);
    }
    return acks.size === wanted.size ? acks : undefined;
  }, `${wanted.size} SQS acknowledgements`, timeoutMs);
}

const countBy = <T>(values: readonly T[]) => values.reduce((map, value) => map.set(value, (map.get(value) ?? 0) + 1), new Map<T, number>());

test('three API instances, three SQS workers, two publishers and three schedulers run as distinct Bun processes', async () => {
  const all = children();
  expect(apis).toHaveLength(3);
  expect(new Set(all.map(child => child.pid)).size).toBe(all.length);
  expect(all.map(child => child.pid)).not.toContain(process.pid);
  for (const api of apis) expect(await call(api, 'GET', '/health/live')).toMatchObject({ status: 200, body: { status: 'ok' } });
  const backends = [...workers, ...publishers, ...schedulers].map(child => child.of('ready')[0]!['backend']);
  expect(new Set(backends).size).toBe(backends.length);
  proof('distinct-processes', {
    testRunner: process.pid,
    processes: all.map(child => ({ name: child.name, pid: child.pid, backend: child.of('ready')[0]?.['backend'] ?? null })),
  });
});

test('the same BET sent 50 times in parallel across three API processes, raced by SQS workers, debits once', async () => {
  const w = await openWallet(apis[0]!);
  const bet = wager(w, 'BET', '25.00');
  const messages = Array.from({ length: 10 }, () => envelope(bet));
  const sending = send(messages);
  const http = await Promise.all(Array.from({ length: 50 }, (_, i) => submit(apis[i % 3]!, bet)));
  await sending;
  const acks = await acknowledged(messages.map(m => m.messageId));

  expect(http.map(r => r.status)).toEqual(Array(50).fill(200));
  expect(countBy(http.map(r => r.instance)).size).toBe(3);
  const inbox = await q(`SELECT result_snapshot->'body'->>'transactionId' transaction_id,
      (result_snapshot->'body'->>'idempotentReplay')::boolean replay
    FROM wager_message_inbox WHERE message_id = ANY(?::text[])`, [pgArray(messages.map(m => m.messageId))]);
  expect(inbox).toHaveLength(10);
  const transactionIds = new Set([...http.map(r => r.body.transactionId as string), ...inbox.map(r => r['transaction_id'] as string)]);
  expect(transactionIds.size).toBe(1);
  const applied = http.filter(r => !r.body.idempotentReplay).length + inbox.filter(r => !r['replay']).length;
  expect(applied).toBe(1);
  expect(await walletState(orm, w.id)).toMatchObject({ balance: '75.00', version: '2', ledger: 2, debits: 1 });
  const reconciliation = await reconcile(orm, apis, [w.id]);
  proof('50-parallel-duplicates', {
    httpSubmissions: 50, sqsSubmissions: 10, applied, replays: 59, winner: http.some(r => !r.body.idempotentReplay) ? 'http' : 'sqs',
    httpByInstance: Object.fromEntries(countBy(http.map(r => r.instance))), sqsByWorkerPid: Object.fromEntries(countBy([...acks.values()])),
    reconciliation,
  });
}, 90_000);

test('100/80/80 on 20 wallets in parallel: HTTP x HTTP on different instances and HTTP x SQS each debit exactly once', async () => {
  const wallets = await Promise.all(Array.from({ length: 20 }, (_, i) => openWallet(apis[i % 3]!)));
  const bets = wallets.map(w => [wager(w, 'BET', '80.00'), wager(w, 'BET', '80.00')] as const);
  const messages = bets.slice(10).map(([, second]) => envelope(second));
  await send(messages);
  const http = await Promise.all(bets.flatMap(([first, second], i) =>
    i < 10 ? [submit(apis[i % 3]!, first), submit(apis[(i + 1) % 3]!, second)] : [submit(apis[i % 3]!, first)]));
  await acknowledged(messages.map(m => m.messageId));

  expect(http.every(r => r.status === 200 || (r.status === 422 && r.body.failureCode === 'INSUFFICIENT_FUNDS'))).toBe(true);
  const rows = await q(`SELECT wallet_id, status, failure_code, external_transaction_id FROM wager_transactions
    WHERE kind='BET' AND wallet_id = ANY(?::uuid[])`, [pgArray(wallets.map(w => w.id))]);
  const sqsExternalIds = new Set(messages.map(m => m.data.externalTransactionId));
  let sqsWins = 0;
  for (const w of wallets) {
    const own = rows.filter(r => r['wallet_id'] === w.id);
    expect(own.map(r => `${r['status']}:${r['failure_code'] ?? ''}`).sort()).toEqual(['PROCESSED:', 'REJECTED:INSUFFICIENT_FUNDS']);
    if (own.some(r => r['status'] === 'PROCESSED' && sqsExternalIds.has(r['external_transaction_id'] as string))) sqsWins++;
    expect(await walletState(orm, w.id)).toMatchObject({ balance: '20.00', version: '2', ledger: 2, debits: 1 });
  }
  const reconciliation = await reconcile(orm, apis, wallets.map(w => w.id));
  proof('100-80-80-parallel-wallets', { wallets: 20, httpVsHttp: 10, httpVsSqs: 10, sqsWins, httpWinsInMixed: 10 - sqsWins, reconciliation });
}, 90_000);

test('distinct wallets progress in parallel through HTTP and SQS with exact final balances and no lost update', async () => {
  const wallets = await Promise.all(Array.from({ length: 20 }, (_, i) => openWallet(apis[i % 3]!)));
  const operations = wallets.flatMap(w => [
    ...Array.from({ length: 6 }, () => wager(w, 'BET', '10.00')),
    ...Array.from({ length: 4 }, () => wager(w, 'WIN', '2.50')),
  ]);
  const viaSqs = operations.filter((_, i) => i % 2 === 1).map(op => envelope(op));
  const viaHttp = operations.filter((_, i) => i % 2 === 0);
  const [, http] = await Promise.all([send(viaSqs), Promise.all(viaHttp.map((op, i) => submit(apis[i % 3]!, op)))]);
  const acks = await acknowledged(viaSqs.map(m => m.messageId));

  expect(http.map(r => r.status)).toEqual(Array(viaHttp.length).fill(200));
  for (const w of wallets) expect(await walletState(orm, w.id)).toMatchObject({ balance: '50.00', version: '11', ledger: 11, debits: 6 });
  const reconciliation = await reconcile(orm, apis, wallets.map(w => w.id));
  proof('parallel-wallets', {
    wallets: 20, operations: operations.length, http: viaHttp.length, sqs: viaSqs.length,
    sqsByWorkerPid: Object.fromEntries(countBy([...acks.values()])), finalBalance: '50.00', reconciliation,
  });
}, 90_000);

test('REFUND/ROLLBACK delivered before the BET are applied once by three concurrent scheduler processes', async () => {
  const wallets = await Promise.all(Array.from({ length: 10 }, (_, i) => openWallet(apis[i % 3]!)));
  const bets = wallets.map(w => wager(w, 'BET', '25.00'));
  const reversals = wallets.map((w, i) => wager(w, i % 2 === 0 ? 'REFUND' : 'ROLLBACK', '25.00', bets[i]!.body.externalTransactionId));
  const pending = await Promise.all(reversals.map((r, i) => submit(apis[i % 3]!, r)));
  expect(pending.map(r => [r.status, r.body.status])).toEqual(Array(10).fill([202, 'PENDING_REFERENCE']));
  const reversalIds = pending.map(r => r.body.transactionId as string);
  const outcomesFor = (status: string) => schedulers.flatMap(s => s.of('outcome'))
    .filter(o => o['status'] === status && reversalIds.includes(o['transactionId'] as string));
  // A primeira tentativa vence após 1 s; esperar permite aos schedulers observar a referência ausente.
  await waitUntil(async () => new Set(outcomesFor('RESCHEDULED').map(o => o['transactionId'])).size === 10 || undefined, 'schedulers to reschedule missing references', 15_000);

  const messages = bets.slice(5).map(bet => envelope(bet));
  const [, http] = await Promise.all([send(messages), Promise.all(bets.slice(0, 5).map((bet, i) => submit(apis[i % 3]!, bet)))]);
  expect(http.map(r => r.status)).toEqual(Array(5).fill(200));
  await acknowledged(messages.map(m => m.messageId));
  const terminal = await waitUntil(async () => {
    const rows = await q(`SELECT id, status FROM wager_transactions WHERE id = ANY(?::uuid[])`, [pgArray(reversalIds)]);
    return rows.every(r => r['status'] !== 'PENDING_REFERENCE') ? rows : undefined;
  }, 'scheduler to resolve every pending reversal', 45_000);

  expect(terminal.map(r => r['status'])).toEqual(Array(10).fill('PROCESSED'));
  expect((await q(`SELECT count(*)::int n FROM pending_reference_jobs WHERE transaction_id = ANY(?::uuid[])`, [pgArray(reversalIds)]))[0]!['n']).toBe(0);
  for (const w of wallets) expect(await walletState(orm, w.id)).toMatchObject({ balance: '100.00', version: '3', ledger: 3, debits: 1 });
  const processed = outcomesFor('PROCESSED');
  expect(countBy(processed.map(o => o['transactionId'])).size).toBe(10);
  expect([...countBy(processed.map(o => o['transactionId'])).values()].every(n => n === 1)).toBe(true);
  expect(outcomesFor('REJECTED')).toEqual([]);
  const reconciliation = await reconcile(orm, apis, wallets.map(w => w.id));
  proof('reference-before-arrival-three-schedulers', {
    reversals: 10, processedByScheduler: Object.fromEntries(countBy(processed.map(o => o['pid'] as number))),
    rescheduledBySchedulerPid: Object.fromEntries(countBy(outcomesFor('RESCHEDULED').map(o => o['pid'] as number))),
    lostLease: outcomesFor('LOST_LEASE').length, reconciliation,
  });
}, 90_000);

test('two publisher processes drain one outbox once each and the demo consumer sees every eventId exactly once', async () => {
  await waitUntil(async () => (await q('SELECT count(*)::int n FROM outbox_events WHERE published_at IS NULL'))[0]!['n'] === 0,
    'outbox fully published', 45_000);
  const total = (await q('SELECT count(*)::int n FROM outbox_events'))[0]!['n'] as number;
  const published = publishers.flatMap(p => p.of('publication')).filter(line => line['status'] === 'PUBLISHED');
  const byPublisher = countBy(published.map(line => line['pid'] as number));
  expect(byPublisher.size).toBe(2);
  expect(published).toHaveLength(total);
  expect(new Set(published.map(line => line['eventId'])).size).toBe(total);
  expect(publishers.flatMap(p => p.of('publish-error'))).toEqual([]);

  const consumer = new SqsEventConsumer(client, events.url, new SqlEventDeduplicator(orm), { waitTimeSeconds: 1 });
  const consumed: EventConsumption[] = [];
  await waitUntil(async () => {
    consumed.push(...await consumer.pollOnce());
    return new Set(consumed.filter(c => c.disposition === 'ACKNOWLEDGED').map(c => c.eventId)).size === total ? true : undefined;
  }, `${total} events on wager-events`, 60_000);
  expect(consumed.filter(c => c.disposition !== 'ACKNOWLEDGED')).toEqual([]);
  const [inbox] = await q(`SELECT count(*)::int n,
      count(*) FILTER (WHERE event_id NOT IN (SELECT event_id FROM outbox_events))::int unknown
    FROM wager_event_inbox`);
  expect(inbox).toEqual({ n: total, unknown: 0 });

  expect(workerLines('message').filter(line => line['disposition'] === 'PERMANENT')).toEqual([]);
  const [lag] = await q(`SELECT
      round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM published_at-created_at)*1000)::numeric,1)::text p50_ms,
      round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM published_at-created_at)*1000)::numeric,1)::text p95_ms,
      round(max(extract(epoch FROM published_at-created_at)*1000)::numeric,1)::text max_ms
    FROM outbox_events`);
  proof('two-publishers', {
    outboxEvents: total, publishedByPid: Object.fromEntries(byPublisher),
    consumerDuplicates: consumed.filter(c => c.disposition === 'ACKNOWLEDGED' && c.duplicate).length, outboxLagMs: lag,
  });
}, 120_000);

test('final reconciliation of every wallet, then SIGTERM stops every process cleanly', async () => {
  const wallets = (await q('SELECT id FROM wallets')).map(r => r['id'] as string);
  const reconciliation = await reconcile(orm, apis, wallets);
  const [totals] = await q(`SELECT
      (SELECT count(*)::int FROM wallets WHERE balance < 0) negative,
      (SELECT count(*)::int FROM (SELECT transaction_id FROM ledger_entries GROUP BY transaction_id HAVING count(*) > 1) d) duplicated_entries,
      (SELECT count(*)::int FROM wager_message_inbox WHERE processed_at IS NULL) unfinished_inbox`);
  expect(totals).toEqual({ negative: 0, duplicated_entries: 0, unfinished_inbox: 0 });
  const acks = countBy(workerLines('message').filter(l => l['disposition'] === 'ACKNOWLEDGED').map(l => l['pid'] as number));
  expect(acks.size).toBe(3);

  const stopped = await Promise.all(children().map(child => child.stop()));
  for (const result of stopped) {
    const graceful = result.exitCode === 0 || (result.name.startsWith('api-') && result.signal === 'SIGTERM');
    expect({ name: result.name, graceful }).toEqual({ name: result.name, graceful: true });
  }
  for (const worker of workers) expect(worker.of('stopped')).toHaveLength(1);
  for (const child of [...publishers, ...schedulers]) expect(child.of('stopped')).toHaveLength(1);
  expect(children().filter(child => child.stderr !== '').map(child => ({ name: child.name, stderr: child.stderr }))).toEqual([]);
  const errors = children().flatMap(child => child.lines.filter((line: Line) => String(line['type']).endsWith('-error')).map(line => ({ name: child.name, ...line })));
  proof('final-reconciliation-and-shutdown', {
    reconciliation, totals, sqsAcksByWorkerPid: Object.fromEntries(acks), http: httpStats, transientErrors: errors,
    sqsTransientRetries: workerLines('message').filter(l => l['disposition'] === 'RETRY').length, stopped,
  });
}, 90_000);
