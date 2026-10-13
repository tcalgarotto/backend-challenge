








import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { GetQueueUrlCommand, ReceiveMessageCommand, SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { SqlEventDeduplicator, SqsEventConsumer, type EventConsumption } from '../../../src/messaging/outbox/sqs-event-consumer.js';
import { sendWagerTransactionRequested } from '../../../src/messaging/wager-message.js';
import { canonicalJson } from '../../../src/wagering/application/process-wager.js';
import { isolatedPair, type QueuePair } from '../../integration/sqs/support/sqs.js';
import {
  Child, PROVIDER, call, countBy, envelope, httpStats, openWallet, reconcile, requireSqs, requiredInfrastructure, sqsClient,
  startApi, submit, wager, waitUntil, walletState, type ApiInstance, type Orm, type Wager, type WalletRef,
} from './support.js';

const role = `jwp_matrix_${randomUUID().replaceAll('-', '')}`;
let orm: Orm, client: SQSClient, commands: QueuePair, events: QueuePair;
let ownerUrl: string, sqsEndpoint: string, runtimeUrl: string, migrated = false;
let apiEnv: Record<string, string>;
const apis: ApiInstance[] = [], workers: Child[] = [], publishers: Child[] = [], schedulers: Child[] = [];
const wallets: string[] = [];
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<Record<string, any>[]>(sql, args);
const pgArray = (values: readonly string[]) => `{${values.join(',')}}`;
const proof = (name: string, data: Record<string, unknown>) => console.log(JSON.stringify({ proof: name, ...data }));
const children = () => [...apis.map(api => api.child), ...workers, ...publishers, ...schedulers];
const api = (i: number) => apis[i % apis.length]!;
const track = (w: WalletRef) => { wallets.push(w.id); return w; };
const newWallet = async (amount = '100.00', i = 0) => track(await openWallet(api(i), amount));
const spawnMigrate = async () => {
  const migration = Bun.spawn(['bun', 'run', 'db:migrate'], { env: { ...process.env, JWP_MIGRATION_DATABASE_URL: ownerUrl }, stdout: 'pipe', stderr: 'pipe' });
  const exit = await migration.exited;
  if (exit !== 0) throw new Error(`db:migrate exited ${exit}: ${await new Response(migration.stderr).text()}`);
};

beforeAll(async () => {
  ({ ownerUrl, sqsEndpoint } = requiredInfrastructure());
  client = sqsClient(sqsEndpoint);
  const queueUrls = await requireSqs(client, sqsEndpoint);
  orm = await openFinancialOrm(ownerUrl);
  const [server] = await q('SELECT version() version, current_setting(\'server_version\') server_version');
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]!['relation']).toBeNull();


  await spawnMigrate();
  const firstUp = (await orm.migrator.getExecutedMigrations()).map(m => m.name);
  await orm.migrator.down({ to: 0 });
  const afterDown = (await q("SELECT to_regclass('public.wallets') w, to_regclass('public.ledger_entries') l, to_regclass('public.outbox_events') o"))[0]!;
  expect(afterDown).toEqual({ w: null, l: null, o: null });
  await spawnMigrate();
  migrated = true;
  const executed = (await orm.migrator.getExecutedMigrations()).map(m => m.name);
  expect(executed).toEqual(firstUp);
  expect(executed).toContain('Migration202610120005');
  const [reversalIndex] = await q("SELECT indexdef FROM pg_indexes WHERE indexname='wager_one_reversal'");
  expect(reversalIndex!['indexdef']).toContain('(reference_transaction_id)');
  expect(reversalIndex!['indexdef']).not.toContain('kind)');


  await q(`CREATE ROLE ${role} LOGIN PASSWORD 'matrix_test_only'`);
  await q(`GRANT jwp_runtime TO ${role}`);
  const [runtimeRole] = await q('SELECT rolsuper, rolcreaterole, rolcreatedb, rolbypassrls FROM pg_roles WHERE rolname=?', [role]);
  expect(runtimeRole).toEqual({ rolsuper: false, rolcreaterole: false, rolcreatedb: false, rolbypassrls: false });
  const url = new URL(ownerUrl);
  url.username = role;
  url.password = 'matrix_test_only';
  runtimeUrl = url.toString();

  const contractUrls = await Promise.all(['wager-transactions.fifo', 'wager-transactions-dlq.fifo', 'wager-events.fifo', 'wager-events-dlq.fifo']
    .map(async name => (await client.send(new GetQueueUrlCommand({ QueueName: name }))).QueueUrl!));
  for (const contractUrl of contractUrls) expect(queueUrls).toContain(contractUrl);
  commands = await isolatedPair(client, 'wager-transactions.fifo');
  events = await isolatedPair(client, 'wager-events.fifo');

  apiEnv = {
    JWP_DATABASE_URL: runtimeUrl, JWP_SQS_ENDPOINT: sqsEndpoint, JWP_SQS_QUEUE_URLS: contractUrls.join(','),
    AWS_REGION: 'us-east-1', AWS_ACCESS_KEY_ID: 'test', AWS_SECRET_ACCESS_KEY: 'test',
  };
  const worker = (name: string, script: string, queueUrl?: string) => Child.spawn(name, ['bun', `tests/matrix/delivery/${script}`], {
    JWP_WORKER_DATABASE_URL: runtimeUrl, JWP_WORKER_SQS_ENDPOINT: sqsEndpoint, ...(queueUrl ? { JWP_WORKER_QUEUE_URL: queueUrl } : {}),
  });
  apis.push(...await Promise.all([1, 2, 3].map(i => startApi(`api-${i}`, apiEnv))));
  workers.push(...[1, 2, 3].map(i => worker(`sqs-worker-${i}`, 'wager-worker.ts', commands.url)));
  publishers.push(...[1, 2].map(i => worker(`publisher-${i}`, 'publisher-worker.ts', events.url)));
  schedulers.push(...[1, 2].map(i => worker(`scheduler-${i}`, 'scheduler-worker.ts')));
  await Promise.all([...workers, ...publishers, ...schedulers].map(child => child.waitFor(line => line['type'] === 'ready', 'ready')));
  proof('clean-setup', {
    postgres: server!['server_version'], postgresVersion: server!['version'], migrations: executed,
    reversibleMigrations: { firstUp, afterDown, secondUp: executed }, reversalIndex: reversalIndex!['indexdef'],
    runtimeRole: { superuser: false, member: 'jwp_runtime' }, contractQueues: contractUrls.map(u => u.split('/').pop()),
  });
}, 120_000);

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
}, 120_000);

const workerLines = (type: string) => workers.flatMap(worker => worker.of(type));
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
async function transaction(id: string) {
  const result = await call(api(0), 'GET', `/wagering/transactions/${id}`);
  expect(result.status).toBe(200);
  return result.body;
}

test('M01 processes: three compiled API instances, three SQS workers, two publishers, two schedulers; health and metrics', async () => {
  const all = children();
  expect(new Set(all.map(child => child.pid)).size).toBe(all.length);
  expect(all.map(child => child.pid)).not.toContain(process.pid);
  const health = [];
  for (const instance of apis) {
    expect(await call(instance, 'GET', '/health/live')).toMatchObject({ status: 200, body: { status: 'ok' } });
    const ready = await call(instance, 'GET', '/health/ready');
    expect(ready.status).toBe(200);
    health.push(ready.body);
  }
  const metrics = await fetch(`${api(0).base}/metrics`);
  expect(metrics.status).toBe(200);
  const exposition = await metrics.text();
  for (const name of ['jwp_wager_transactions_total', 'jwp_duplicates_total', 'jwp_retries_total', 'jwp_outbox_pending_events',
    'jwp_wager_processing_duration_seconds', 'jwp_postgres_lock_waiters', 'jwp_sqs_queue_messages']) expect(exposition).toContain(name);
  const backends = [...workers, ...publishers, ...schedulers].map(child => child.of('ready')[0]!['backend']);
  expect(new Set(backends).size).toBe(backends.length);
  proof('processes', { testRunner: process.pid, processes: all.map(c => ({ name: c.name, pid: c.pid })), ready: health[0] });
}, 30_000);

test('M02 exact money: decimal strings at scale 2, no float drift, invalid inputs rejected before acceptance, numeric columns', async () => {
  const w = await newWallet('1000.00');
  const balances: string[] = [];
  for (let i = 0; i < 3; i++) {
    const r = await submit(api(i), wager(w, 'BET', '0.10'));
    expect(r.status).toBe(200);
    balances.push(r.body.balance.amount);
  }
  expect(balances).toEqual(['999.90', '999.80', '999.70']);
  const win = await submit(api(0), wager(w, 'WIN', '0.30'));
  expect(win.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '1000.00', currency: 'BRL' } });

  const invalid: unknown[] = ['1e2', '10.001', '-1.00', '', ' 1.00', '01.00', 'NaN', 'Infinity', '1.0', '1,00', 25, 25.5, null];
  const rejected: string[] = [];
  for (const amount of invalid) {
    const bad = wager(w, 'BET', '1.00');
    const body = { ...bad.body, money: { amount, currency: 'BRL' } };
    const r = await call(api(1), 'POST', '/wagering/transactions', body, { 'Idempotency-Key': bad.key });
    expect({ amount, status: r.status, code: r.body?.error?.code }).toEqual({ amount, status: 400, code: 'INVALID_PAYLOAD' });
    rejected.push(bad.body.externalTransactionId);
  }
  expect((await q('SELECT count(*)::int n FROM wager_transactions WHERE external_transaction_id = ANY(?::text[])', [pgArray(rejected)]))[0]!['n']).toBe(0);
  for (const amount of ['1e2', '10.5', '-5.00', 100]) {
    const r = await call(api(2), 'POST', '/wallets', { playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' } });
    expect({ amount, status: r.status }).toEqual({ amount, status: 400 });
  }
  const usdBet = wager(w, 'BET', '1.00');
  const usd = await submit(api(0), { ...usdBet, body: { ...usdBet.body, money: { amount: '1.00', currency: 'USD' } } });

  expect({ status: usd.status, code: usd.body.error?.code }).toEqual({ status: 400, code: 'INVALID_PAYLOAD' });

  const usdWallet = { id: randomUUID(), playerId: randomUUID() };
  await q("INSERT INTO wallets (id, player_id, currency, balance, version) VALUES (?, ?, 'USD', 0, 1)", [usdWallet.id, usdWallet.playerId]);
  track(usdWallet);
  const mismatch = wager(usdWallet, 'WIN', '1.00');
  const conflict = await submit(api(1), mismatch);
  expect({ status: conflict.status, code: conflict.body.error?.code }).toEqual({ status: 400, code: 'CURRENCY_MISMATCH' });
  expect((await q('SELECT count(*)::int n FROM wager_transactions WHERE wallet_id=?', [usdWallet.id]))[0]!['n']).toBe(0);
  const columns = await q(`SELECT table_name, column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns
    WHERE table_schema='public' AND (table_name, column_name) IN (('wallets','balance'),('wager_transactions','amount'),
      ('ledger_entries','amount'),('ledger_entries','balance_before'),('ledger_entries','balance_after')) ORDER BY 1,2`);
  expect(columns).toHaveLength(5);
  for (const column of columns) expect(column).toMatchObject({ data_type: 'numeric', numeric_precision: 20, numeric_scale: 2 });
  expect((await walletState(orm, w.id))['balance']).toBe('1000.00');
  proof('exact-money', { balances, afterWin: win.body.balance, invalidAmountsRejected: invalid.length, columns: columns.length,
    nonBrlOnBrlWallet: '400 INVALID_PAYLOAD', brlOnUsdWallet: '400 CURRENCY_MISMATCH' });
}, 30_000);

test('M03 challenge §8: 100.00 and two simultaneous 80.00 BETs on three instances, 10 wallets; retries never debit twice', async () => {
  const results = await Promise.all(Array.from({ length: 10 }, async (_, round) => {
    const w = await newWallet('100.00', round);
    const bets = [wager(w, 'BET', '80.00'), wager(w, 'BET', '80.00')];
    const race = await Promise.all(bets.map((bet, i) => submit(api(round + i), bet)));
    const retries = await Promise.all(bets.flatMap((bet, i) => [0, 1, 2].map(k => submit(api(round + i + k + 1), bet))));
    return { w, race, retries, state: await walletState(orm, w.id) };
  }));
  for (const { race, retries, state } of results) {
    const winner = race.find(r => r.status === 200)!;
    const loser = race.find(r => r.status === 422)!;
    expect(race.map(r => r.status).sort()).toEqual([200, 422]);
    expect(winner.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '20.00' }, idempotentReplay: false });
    expect(loser.body).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', idempotentReplay: false });
    for (const retry of retries) {
      const original = retry.body.transactionId === winner.body.transactionId ? winner : loser;
      expect(retry.status).toBe(original.status);
      expect(retry.body).toEqual({ ...original.body, idempotentReplay: true });
    }
    expect(state).toMatchObject({ balance: '20.00', debits: 1, ledger: 2 });
  }
  const ids = results.map(r => r.w.id);
  expect((await q(`SELECT status, failure_code, count(*)::int n FROM wager_transactions WHERE wallet_id = ANY(?::uuid[]) AND kind='BET'
    GROUP BY 1,2 ORDER BY 1`, [pgArray(ids)]))).toEqual([
    { status: 'PROCESSED', failure_code: null, n: 10 }, { status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS', n: 10 }]);
  expect((await q("SELECT count(*)::int n FROM wallets WHERE balance < 0"))[0]!['n']).toBe(0);
  proof('section-8', { wallets: 10, processed: 10, rejected: 10, finalBalance: '20.00', debitsPerWallet: 1, retriesPerBet: 3 });
}, 60_000);

test('M04 the same BET 50 times in parallel on three instances plus 10 SQS copies debits exactly once', async () => {
  const w = await newWallet('100.00');
  const bet = wager(w, 'BET', '25.00');
  const messages = Array.from({ length: 10 }, () => envelope(bet));
  const sending = Promise.all(messages.map(m => sendWagerTransactionRequested(client, commands.url, m)));
  const http = await Promise.all(Array.from({ length: 50 }, (_, i) => submit(api(i), bet)));
  await sending;
  await acknowledged(messages.map(m => m.messageId));
  expect(http.map(r => r.status)).toEqual(Array(50).fill(200));
  expect(countBy(http.map(r => r.instance)).size).toBe(3);
  const inbox = await q(`SELECT result_snapshot->'body'->>'transactionId' transaction_id,
      (result_snapshot->'body'->>'idempotentReplay')::boolean replay FROM wager_message_inbox WHERE message_id = ANY(?::text[])`,
  [pgArray(messages.map(m => m.messageId))]);
  expect(inbox).toHaveLength(10);
  const transactionIds = new Set([...http.map(r => r.body.transactionId as string), ...inbox.map(r => r['transaction_id'] as string)]);
  expect(transactionIds.size).toBe(1);
  expect(http.filter(r => !r.body.idempotentReplay).length + inbox.filter(r => !r['replay']).length).toBe(1);
  expect(await walletState(orm, w.id)).toMatchObject({ balance: '75.00', debits: 1 });
  proof('same-bet-50x', { http: 50, sqs: 10, transactions: transactionIds.size, applied: 1, debits: 1, balance: '75.00' });
}, 60_000);

test('M05 distinct wallets in parallel: 10 wallets x 5 BETs of 10.00 interleaved over three instances', async () => {
  const ws = await Promise.all(Array.from({ length: 10 }, (_, i) => newWallet('100.00', i)));
  const bets = ws.flatMap(w => Array.from({ length: 5 }, () => wager(w, 'BET', '10.00')));
  const results = await Promise.all(bets.map((bet, i) => submit(api(i), bet)));
  expect(results.map(r => r.status)).toEqual(Array(50).fill(200));
  for (const w of ws) expect(await walletState(orm, w.id)).toMatchObject({ balance: '50.00', debits: 5, ledger: 6, version: '6' });
  proof('distinct-wallets', { wallets: 10, betsPerWallet: 5, finalBalance: '50.00' });
}, 60_000);

test('M06 persistent idempotency: replay keeps the original result across instances; conflicts and missing key are distinct', async () => {
  const w = await newWallet('100.00');
  const bet = wager(w, 'BET', '25.00');
  const first = await submit(api(0), bet);
  expect(first).toMatchObject({ status: 200, body: { status: 'PROCESSED', balance: { amount: '75.00' }, idempotentReplay: false } });
  expect((await submit(api(1), wager(w, 'WIN', '20.00'))).body.balance.amount).toBe('95.00');
  const replay = await submit(api(2), bet);
  expect(replay).toMatchObject({ status: 200, body: { ...first.body, idempotentReplay: true } });

  const changed = await call(api(1), 'POST', '/wagering/transactions', { ...bet.body, money: { amount: '26.00', currency: 'BRL' } }, { 'Idempotency-Key': bet.key });
  expect({ status: changed.status, code: changed.body.error?.code }).toEqual({ status: 409, code: 'IDEMPOTENCY_CONFLICT' });
  const otherKey = await call(api(2), 'POST', '/wagering/transactions', bet.body, { 'Idempotency-Key': `${bet.key}-other` });
  expect({ status: otherKey.status, code: otherKey.body.error?.code }).toEqual({ status: 409, code: 'EXTERNAL_IDENTITY_CONFLICT' });
  const noKey = await call(api(0), 'POST', '/wagering/transactions', wager(w, 'BET', '1.00').body);
  expect({ status: noKey.status, code: noKey.body.error?.code }).toEqual({ status: 400, code: 'IDEMPOTENCY_KEY_REQUIRED' });
  const opening = wager(w, 'OPENING', '1.00');
  expect((await submit(api(1), opening)).status).toBe(400);

  const big = wager(w, 'BET', '500.00');
  const rejected = await submit(api(0), big);
  expect(rejected).toMatchObject({ status: 422, body: { status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', balance: { amount: '95.00' } } });
  expect((await submit(api(1), wager(w, 'WIN', '1000.00'))).body.balance.amount).toBe('1095.00');
  const rejectedReplay = await submit(api(2), big);
  expect(rejectedReplay).toMatchObject({ status: 422, body: { ...rejected.body, idempotentReplay: true } });
  expect(await walletState(orm, w.id)).toMatchObject({ balance: '1095.00', debits: 1 });
  proof('idempotency', {
    replayBalance: replay.body.balance.amount, conflict: 409, externalConflict: 409, missingKey: 400,
    rejectedReplay: { status: 422, balance: rejectedReplay.body.balance.amount },
  });
}, 30_000);

test('M07 auditable append-only ledger: cursor pages, chained balances, no ledger for LOSS/REJECTED, UPDATE/DELETE refused', async () => {
  const w = await newWallet('100.00');
  for (const [kind, amount] of [['BET', '10.00'], ['WIN', '30.00'], ['LOSS', '0.00'], ['BET', '500.00'], ['BET', '5.00']] as const) await submit(api(0), wager(w, kind, amount));
  const items: any[] = [];
  let cursor: string | null = null;
  let pages = 0;
  do {
    const page = await call(api(pages), 'GET', `/wallets/${w.id}/ledger?limit=2${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`);
    expect(page.status).toBe(200);
    items.push(...page.body.items);
    cursor = page.body.nextCursor;
    pages++;
  } while (cursor);
  expect(items.map(i => [i.direction, i.money.amount, i.balanceBefore.amount, i.balanceAfter.amount])).toEqual([
    ['CREDIT', '100.00', '0.00', '100.00'], ['DEBIT', '10.00', '100.00', '90.00'], ['CREDIT', '30.00', '90.00', '120.00'], ['DEBIT', '5.00', '120.00', '115.00']]);
  expect(items.map(i => i.walletVersion)).toEqual([1, 2, 3, 4]);
  expect(pages).toBe(2);
  const badCursor = await call(api(1), 'GET', `/wallets/${w.id}/ledger?cursor=not-a-cursor`);
  expect({ status: badCursor.status, code: badCursor.body.error?.code }).toEqual({ status: 400, code: 'INVALID_CURSOR' });
  await expect(q('UPDATE ledger_entries SET amount=amount WHERE wallet_id=?', [w.id])).rejects.toThrow(/append-only/);
  await expect(q('DELETE FROM ledger_entries WHERE wallet_id=?', [w.id])).rejects.toThrow(/append-only/);
  const runtime = await openFinancialOrm(runtimeUrl);
  try {
    await expect(runtime.em.fork().getConnection().execute('DELETE FROM ledger_entries WHERE wallet_id=?', [w.id])).rejects.toThrow();
    await expect(runtime.em.fork().getConnection().execute('UPDATE wallets SET balance=balance+1 WHERE id=?', [w.id])).rejects.toThrow();
  } finally { await runtime.close(true); }
  const rec = await call(api(2), 'POST', `/wallets/${w.id}/reconciliation`);
  expect(rec.body).toMatchObject({ consistent: true, checkedEntries: 4, storedBalance: { amount: '115.00' }, calculatedBalance: { amount: '115.00' } });
  proof('ledger', { entries: items.length, pages, lossAndRejectedEntries: 0, appendOnly: true, reconciliation: rec.body });
}, 30_000);

test('M08 reversal addendum: one applied reversal per referenced transaction, any kind; ROLLBACK of REFUND allowed', async () => {
  const states: Record<string, unknown> = {};

  const a = await newWallet('100.00');
  const betA = wager(a, 'BET', '25.00');
  expect((await submit(api(0), betA)).body.balance.amount).toBe('75.00');
  const refundA = wager(a, 'REFUND', '25.00', betA.body.externalTransactionId);
  expect((await submit(api(1), refundA)).body).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' } });
  const rollbackBetA = await submit(api(2), wager(a, 'ROLLBACK', '25.00', betA.body.externalTransactionId));
  expect(rollbackBetA).toMatchObject({ status: 422, body: { status: 'REJECTED', failureCode: 'ALREADY_REVERSED', balance: { amount: '100.00' } } });
  const rollbackRefund = wager(a, 'ROLLBACK', '25.00', refundA.body.externalTransactionId);
  expect((await submit(api(0), rollbackRefund)).body).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00' } });
  const secondRollbackRefund = await submit(api(1), wager(a, 'ROLLBACK', '25.00', refundA.body.externalTransactionId));
  expect(secondRollbackRefund.body).toMatchObject({ status: 'REJECTED', failureCode: 'ALREADY_REVERSED' });
  const rollbackOfRollback = await submit(api(2), wager(a, 'ROLLBACK', '25.00', rollbackRefund.body.externalTransactionId));
  expect(rollbackOfRollback.body).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_KIND_INVALID' });
  states['refundThenRollback'] = await walletState(orm, a.id);
  expect(states['refundThenRollback']).toMatchObject({ balance: '75.00', ledger: 4 });


  const b = await newWallet('100.00');
  const betB = wager(b, 'BET', '25.00');
  await submit(api(0), betB);
  expect((await submit(api(1), wager(b, 'ROLLBACK', '25.00', betB.body.externalTransactionId))).body.balance.amount).toBe('100.00');
  expect((await submit(api(2), wager(b, 'REFUND', '25.00', betB.body.externalTransactionId))).body)
    .toMatchObject({ status: 'REJECTED', failureCode: 'ALREADY_REVERSED', balance: { amount: '100.00' } });


  const c = await newWallet('100.00');
  const betC = wager(c, 'BET', '25.00');
  await submit(api(0), betC);
  const racers = [wager(c, 'REFUND', '25.00', betC.body.externalTransactionId), wager(c, 'ROLLBACK', '25.00', betC.body.externalTransactionId),
    ...Array.from({ length: 4 }, () => wager(c, 'REFUND', '25.00', betC.body.externalTransactionId))];
  const raced = await Promise.all(racers.map((r, i) => submit(api(i), r)));
  expect(raced.filter(r => r.status === 200)).toHaveLength(1);
  expect(raced.filter(r => r.status === 422 && r.body.failureCode === 'ALREADY_REVERSED')).toHaveLength(5);
  expect(await walletState(orm, c.id)).toMatchObject({ balance: '100.00', credits: 2, debits: 1 });


  const d = await newWallet('100.00');
  const betD = wager(d, 'BET', '25.00');
  await submit(api(0), betD);
  const winD = wager(d, 'WIN', '10.00', betD.body.externalTransactionId);
  expect((await submit(api(1), winD)).body.status).toBe('PROCESSED');
  expect((await submit(api(2), wager(d, 'REFUND', '10.00', winD.body.externalTransactionId))).body.failureCode).toBe('REFERENCE_KIND_INVALID');
  expect((await submit(api(0), wager(d, 'REFUND', '20.00', betD.body.externalTransactionId))).body.failureCode).toBe('REVERSAL_AMOUNT_MISMATCH');
  expect((await submit(api(1), wager(d, 'REFUND', '25.00', betD.body.externalTransactionId, 'other-round'))).body.failureCode).toBe('REFERENCE_CONTEXT_MISMATCH');
  expect((await submit(api(2), wager(d, 'REFUND', '25.00', betD.body.externalTransactionId))).body).toMatchObject({ status: 'PROCESSED', balance: { amount: '110.00' } });

  const e = await newWallet('100.00');
  const winE = wager(e, 'WIN', '50.00');
  await submit(api(0), winE);
  await submit(api(1), wager(e, 'BET', '140.00'));
  const negative = await submit(api(2), wager(e, 'ROLLBACK', '50.00', winE.body.externalTransactionId));
  expect(negative).toMatchObject({ status: 422, body: { status: 'REJECTED', failureCode: 'REVERSAL_INSUFFICIENT_FUNDS', balance: { amount: '10.00' } } });

  const applied = await q(`SELECT reference_transaction_id, count(*)::int n FROM wager_transactions
    WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK') GROUP BY 1 HAVING count(*) > 1`);
  expect(applied).toEqual([]);
  const rejectedLedger = await q(`SELECT count(*)::int n FROM ledger_entries l JOIN wager_transactions t ON t.id=l.transaction_id WHERE t.status<>'PROCESSED'`);
  expect(rejectedLedger[0]!['n']).toBe(0);
  proof('reversal-once-per-reference', {
    refundThenRollback: '422 ALREADY_REVERSED', rollbackOfRefund: 'PROCESSED once', rollbackOfRollback: 'REFERENCE_KIND_INVALID',
    inverseOrder: '422 ALREADY_REVERSED', concurrentReversals: { racers: racers.length, applied: 1 },
    reversalNegative: 'REVERSAL_INSUFFICIENT_FUNDS', duplicateAppliedReversals: 0,
  });
}, 60_000);

test('M09 HTTP contract: happy paths and distinct error classes', async () => {
  const playerId = randomUUID();
  const created = await call(api(0), 'POST', '/wallets', { playerId, initialBalance: { amount: '50.00', currency: 'BRL' } });
  expect(created).toMatchObject({ status: 201, body: { playerId, balance: { amount: '50.00', currency: 'BRL' }, version: 1 } });
  const w = track({ id: created.body.id, playerId });
  const duplicate = await call(api(1), 'POST', '/wallets', { playerId, initialBalance: { amount: '10.00', currency: 'BRL' } });
  expect({ status: duplicate.status, code: duplicate.body.error?.code }).toEqual({ status: 409, code: 'WALLET_ALREADY_EXISTS' });
  const zero = await call(api(2), 'POST', '/wallets', { playerId: randomUUID(), initialBalance: { amount: '0.00', currency: 'BRL' } });
  expect(zero).toMatchObject({ status: 201, body: { balance: { amount: '0.00' }, version: 1 } });
  track({ id: zero.body.id, playerId: zero.body.playerId });
  expect((await walletState(orm, zero.body.id))['ledger']).toBe(0);
  const got = await call(api(2), 'GET', `/wallets/${w.id}`);
  expect(got).toMatchObject({ status: 200, body: { id: w.id, playerId, balance: { amount: '50.00' }, version: 1 } });

  const loss = wager(w, 'LOSS', '0.00');
  const lossResult = await submit(api(0), loss);
  expect(lossResult).toMatchObject({ status: 200, body: { status: 'PROCESSED', balance: { amount: '50.00' } } });
  const byId = await transaction(lossResult.body.transactionId);
  const byProvider = await call(api(1), 'GET', `/providers/${PROVIDER}/wagering/transactions/${loss.body.externalTransactionId}`);
  expect(byProvider).toMatchObject({ status: 200, body: byId });
  expect(byId).toMatchObject({ transactionId: lossResult.body.transactionId, kind: 'LOSS', status: 'PROCESSED' });
  expect(byId).not.toHaveProperty('idempotentReplay');

  const errors = {
    invalidJson: await call(api(0), 'POST', '/wagering/transactions', '{"providerId":', { 'Idempotency-Key': 'k' }),
    unknownWallet: await call(api(1), 'GET', `/wallets/${randomUUID()}`),
    unknownTransaction: await call(api(2), 'GET', `/wagering/transactions/${randomUUID()}`),
    walletNotFound: await submit(api(0), wager({ id: randomUUID(), playerId }, 'BET', '1.00')),
    playerMismatch: await submit(api(1), wager({ id: w.id, playerId: randomUUID() }, 'BET', '1.00')),
    referenceRequired: await submit(api(2), wager(w, 'REFUND', '1.00')),
    unknownField: await call(api(0), 'POST', '/wagering/transactions', { ...wager(w, 'BET', '1.00').body, extra: 1 }, { 'Idempotency-Key': 'x:y' }),
  };
  const summary = Object.fromEntries(Object.entries(errors).map(([name, r]) => [name, `${r.status} ${r.body?.error?.code ?? r.body?.failureCode}`]));
  expect(summary).toEqual({
    invalidJson: '400 INVALID_PAYLOAD', unknownWallet: '404 WALLET_NOT_FOUND', unknownTransaction: '404 TRANSACTION_NOT_FOUND',
    walletNotFound: '404 WALLET_NOT_FOUND', playerMismatch: '400 PLAYER_MISMATCH', referenceRequired: '400 REFERENCE_REQUIRED',
    unknownField: '400 INVALID_PAYLOAD',
  });
  for (const r of Object.values(errors)) expect(r.body.error).toMatchObject({ retryable: false });
  const pending = await submit(api(1), wager(w, 'REFUND', '5.00', `never-arrives-${randomUUID()}`));
  expect(pending).toMatchObject({ status: 202, body: { status: 'PENDING_REFERENCE', idempotentReplay: false } });
  expect(pending.body).not.toHaveProperty('balance');
  proof('http-contract', { happy: ['201 wallet', '200 GET wallet', '200 LOSS', '200 GET by id', '200 GET by provider', '202 pending'], errors: summary });
}, 30_000);

test('M10 out-of-order references: ROLLBACK/REFUND before the BET (HTTP and SQS) resolved by the scheduler processes', async () => {
  const w = await newWallet('100.00');
  const bet = wager(w, 'BET', '30.00');
  const early = wager(w, 'ROLLBACK', '30.00', bet.body.externalTransactionId);
  const pending = await submit(api(0), early);
  expect(pending).toMatchObject({ status: 202, body: { status: 'PENDING_REFERENCE' } });
  const betMessage = envelope(bet);
  await sendWagerTransactionRequested(client, commands.url, betMessage);
  await acknowledged([betMessage.messageId]);
  const resolved = await waitUntil(async () => {
    const t = await transaction(pending.body.transactionId);
    return t.status === 'PROCESSED' ? t : undefined;
  }, 'scheduler resolves the early ROLLBACK', 30_000);
  expect(resolved.referenceTransactionId).toBeDefined();
  const replay = await submit(api(1), early);
  expect(replay).toMatchObject({ status: 200, body: { transactionId: pending.body.transactionId, status: 'PROCESSED', balance: { amount: '100.00' }, idempotentReplay: true } });

  const v = await newWallet('100.00');
  const betV = wager(v, 'BET', '40.00');
  const refundMessage = envelope(wager(v, 'REFUND', '40.00', betV.body.externalTransactionId));
  await sendWagerTransactionRequested(client, commands.url, refundMessage);
  await acknowledged([refundMessage.messageId]);
  const [inboxRow] = await q("SELECT result_snapshot->'body'->>'status' status, result_snapshot->'body'->>'transactionId' id FROM wager_message_inbox WHERE message_id=?", [refundMessage.messageId]);
  expect(inboxRow!['status']).toBe('PENDING_REFERENCE');
  expect((await submit(api(2), betV)).body.balance.amount).toBe('60.00');
  await waitUntil(async () => (await transaction(inboxRow!['id'])).status === 'PROCESSED', 'scheduler resolves the early REFUND', 30_000);
  expect(await walletState(orm, w.id)).toMatchObject({ balance: '100.00', debits: 1, credits: 2 });
  expect(await walletState(orm, v.id)).toMatchObject({ balance: '100.00', debits: 1, credits: 2 });
  const outcomes = schedulers.flatMap(s => s.of('outcome')).filter(o => o['status'] === 'PROCESSED').map(o => o['transactionId']);
  expect(outcomes).toEqual(expect.arrayContaining([pending.body.transactionId, inboxRow!['id']]));
  proof('pending-reference', { httpEarlyRollback: 'PROCESSED by scheduler', sqsEarlyRefund: 'PROCESSED by scheduler', schedulerProcesses: schedulers.length });
}, 60_000);

test('M11 SQS inbox: forced redelivery of one messageId is a no-op; ack only after commit; business rejection acked; poison to DLQ', async () => {
  const w = await newWallet('100.00');
  const bet = wager(w, 'BET', '15.00');
  const message = envelope(bet);
  // Dedup ID distinto força cinco entregas do mesmo messageId além da deduplicação FIFO.
  for (let i = 0; i < 5; i++) {
    await client.send(new SendMessageCommand({ QueueUrl: commands.url, MessageBody: canonicalJson(message), MessageGroupId: w.id, MessageDeduplicationId: `${message.messageId}-${i}` }));
  }
  const tooBig = envelope(wager(w, 'BET', '900.00'));
  await sendWagerTransactionRequested(client, commands.url, tooBig);
  await waitUntil(async () => workerLines('message').filter(l => l['messageId'] === message.messageId && l['disposition'] === 'ACKNOWLEDGED').length >= 5 || undefined,
    'five acknowledgements of one messageId', 45_000);
  await acknowledged([tooBig.messageId]);
  const rows = await q('SELECT message_id, processed_at IS NOT NULL processed, result_snapshot->\'body\'->>\'status\' status, result_snapshot->\'body\'->>\'failureCode\' code FROM wager_message_inbox WHERE message_id = ANY(?::text[]) ORDER BY 1',
    [pgArray([message.messageId, tooBig.messageId])]);
  expect(rows).toHaveLength(2);
  for (const row of rows) expect(row['processed']).toBe(true);
  expect(rows.find(r => r['message_id'] === tooBig.messageId)).toMatchObject({ status: 'REJECTED', code: 'INSUFFICIENT_FUNDS' });
  expect(await walletState(orm, w.id)).toMatchObject({ balance: '85.00', debits: 1 });
  // Todo messageId com ack deve ter linha de inbox confirmada.
  const ackedIds = [...new Set(workerLines('message').filter(l => l['disposition'] === 'ACKNOWLEDGED').map(l => l['messageId'] as string))];
  const committed = await q('SELECT count(*)::int n FROM wager_message_inbox WHERE processed_at IS NOT NULL AND message_id = ANY(?::text[])', [pgArray(ackedIds)]);
  expect(committed[0]!['n']).toBe(ackedIds.length);

  const poisonGroup = `matrix-poison-${randomUUID()}`;
  const poisonBody = JSON.stringify({ messageId: `matrix-poison-${randomUUID()}`, type: 'NotAWager', occurredAt: new Date().toISOString(), data: {} });
  await client.send(new SendMessageCommand({ QueueUrl: commands.url, MessageBody: poisonBody, MessageGroupId: poisonGroup, MessageDeduplicationId: randomUUID() }));
  const dead = await waitUntil(async () => {
    const { Messages } = await client.send(new ReceiveMessageCommand({ QueueUrl: commands.dlqUrl, WaitTimeSeconds: 1, MaxNumberOfMessages: 1 }));
    return Messages?.find(m => m.Body === poisonBody);
  }, 'poison message redriven to the DLQ', 60_000);
  await Bun.sleep(500);


  const permanent = workerLines('message').filter(l => l['disposition'] === 'PERMANENT');
  const receiveCounts = permanent.map(l => Number(l['receiveCount'])).sort((x, y) => x - y);
  expect(permanent.length).toBeGreaterThanOrEqual(1);
  expect(Math.max(...receiveCounts)).toBeLessThanOrEqual(5);
  expect(permanent.every(l => l['messageId'] === undefined)).toBe(true);
  proof('sqs-inbox', {
    forcedDeliveries: 5, inboxRows: 1, debits: 1, businessRejection: 'acked REJECTED INSUFFICIENT_FUNDS',
    ackedMessages: ackedIds.length, committedBeforeAck: committed[0]!['n'], dlq: { redriven: dead.Body === poisonBody, receiveCounts, maxReceiveCount: 5 },
  });
}, 120_000);

test('M12 transactional outbox: two publishers drain every event once by eventId; required event types; duplicate consumption is safe', async () => {
  await waitUntil(async () => (await q('SELECT count(*)::int n FROM outbox_events WHERE published_at IS NULL'))[0]!['n'] === 0 || undefined,
    'outbox drained by two publishers', 60_000);
  const outbox = await q('SELECT event_id::text id, event_type, envelope FROM outbox_events');
  const consumer = new SqsEventConsumer(client, events.url, new SqlEventDeduplicator(orm), { waitTimeSeconds: 1, maxMessages: 10 });
  const consumed: EventConsumption[] = [];
  const wanted = new Set(outbox.map(e => e['id'] as string));
  const seen = new Set<string>();
  await waitUntil(async () => {
    for (const c of await consumer.pollOnce()) {
      consumed.push(c);
      if (c.disposition === 'ACKNOWLEDGED') seen.add(c.eventId);
    }
    return [...wanted].every(id => seen.has(id)) || undefined;
  }, 'every outbox event consumed from SQS', 90_000);
  expect(consumed.filter(c => c.disposition !== 'ACKNOWLEDGED')).toEqual([]);
  const types = countBy(outbox.map(e => e['event_type'] as string));
  for (const type of ['WagerTransactionProcessed', 'WagerTransactionRejected', 'WalletBalanceChanged', 'WagerTransactionPendingReference']) {
    expect(types.get(type) ?? 0).toBeGreaterThan(0);
  }
  const [counts] = await q(`SELECT (SELECT count(*)::int FROM ledger_entries) ledger,
    (SELECT count(*)::int FROM wager_transactions WHERE status='PROCESSED') processed,
    (SELECT count(*)::int FROM wager_transactions WHERE status='REJECTED') rejected`);
  expect(types.get('WalletBalanceChanged')).toBe(counts!['ledger']);
  expect(types.get('WagerTransactionProcessed')).toBe(counts!['processed']);
  expect(types.get('WagerTransactionRejected')).toBe(counts!['rejected']);
  const publications = publishers.flatMap(p => p.of('publication').filter(l => l['status'] === 'PUBLISHED').map(l => ({ id: l['eventId'], pid: l['pid'] })));
  expect(new Set(publications.map(p => p.pid)).size).toBe(2);
  // A inbox durável reconhece a entrega duplicada de evento já consumido.
  const sample = outbox[0]!;
  await client.send(new SendMessageCommand({ QueueUrl: events.url, MessageBody: canonicalJson(sample['envelope']),
    MessageGroupId: (sample['envelope'] as any).data.walletId, MessageDeduplicationId: randomUUID() }));
  const duplicate = await waitUntil(async () => (await consumer.pollOnce()).find(c => c.disposition === 'ACKNOWLEDGED' && c.eventId === sample['id']), 'duplicate consumption', 30_000);
  expect(duplicate).toMatchObject({ duplicate: true });
  proof('outbox', {
    events: outbox.length, published: publications.length, publishersUsed: 2, consumedDistinct: seen.size,
    types: Object.fromEntries(types), duplicateConsumption: 'duplicate=true', ...counts,
  });
}, 150_000);

test('M13 restart: all API instances replaced; replays, balances and reconciliation unchanged', async () => {
  const w = await newWallet('100.00');
  const bet = wager(w, 'BET', '45.00');
  const before = await submit(api(0), bet);
  const balancesBefore = await q('SELECT id::text, balance::text, version::text FROM wallets WHERE id = ANY(?::uuid[]) ORDER BY id', [pgArray(wallets)]);
  const stopped = await Promise.all(apis.splice(0).map(instance => instance.child.stop()));
  for (const s of stopped) expect(s.signal).not.toBe('SIGKILL');
  apis.push(...await Promise.all([4, 5, 6].map(i => startApi(`api-${i}`, apiEnv))));
  const replays = await Promise.all([0, 1, 2].map(i => submit(api(i), bet)));
  for (const r of replays) expect(r).toMatchObject({ status: 200, body: { ...before.body, idempotentReplay: true } });
  const balancesAfter = await q('SELECT id::text, balance::text, version::text FROM wallets WHERE id = ANY(?::uuid[]) ORDER BY id', [pgArray(wallets)]);
  expect(balancesAfter).toEqual(balancesBefore);
  expect(await walletState(orm, w.id)).toMatchObject({ balance: '55.00', debits: 1 });
  proof('restart', { stopped: stopped.map(s => ({ name: s.name, exitCode: s.exitCode, signal: s.signal })), replays: replays.length, wallets: wallets.length });
}, 90_000);

test('M14 final invariant: wallet.balance == balance rebuilt from the ledger for every wallet; workers stop cleanly on SIGTERM', async () => {
  const result = await reconcile(orm, apis, wallets);
  const [global] = await q(`SELECT count(*)::int wallets,
    count(*) FILTER (WHERE w.balance <> coalesce((SELECT sum(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END) FROM ledger_entries l WHERE l.wallet_id=w.id),0))::int divergent,
    count(*) FILTER (WHERE w.balance < 0)::int negative FROM wallets w`);
  expect(global).toMatchObject({ divergent: 0, negative: 0 });
  const [duplicates] = await q(`SELECT
    (SELECT count(*)::int FROM (SELECT transaction_id FROM ledger_entries GROUP BY wallet_id, transaction_id HAVING count(*)>1) d) ledger_per_tx,
    (SELECT count(*)::int FROM (SELECT provider_id, idempotency_key FROM wager_transactions GROUP BY 1,2 HAVING count(*)>1) k) keys`);
  expect(duplicates).toEqual({ ledger_per_tx: 0, keys: 0 });
  const stops = await Promise.all([...workers, ...publishers, ...schedulers].map(child => child.stop()));
  for (const s of stops) expect({ name: s.name, exitCode: s.exitCode, signal: s.signal }).toEqual({ name: s.name, exitCode: 0, signal: null });
  proof('final-invariant', { ...result, global, duplicates, httpRequests: httpStats.requests, retried503: httpStats.retried503, workerStops: stops });
}, 60_000);
