import 'reflect-metadata';
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { NestFactory } from '@nestjs/core';
import type { DynamicModule, INestApplication } from '@nestjs/common';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { WageringModule } from '../../../src/wagering/http/wagering.module.js';
import { ReconciliationSignals } from '../../../src/wallet/infrastructure/reconciliation-signals.js';
import { WalletModule, type WalletModuleOptions } from '../../../src/wallet/http/wallet.module.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let app: INestApplication, base: string, migrated = false;
const apps: INestApplication[] = [];
const logLines: string[] = [];
const signals = new ReconciliationSignals(line => { logLines.push(line); });
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<Record<string, unknown>[]>(sql, args);
type Reply = { status: number; body: any };
async function call(path: string, init: RequestInit = {}, at = base): Promise<Reply> {
  const response = await fetch(at + path, init);
  return { status: response.status, body: await response.json() };
}
const postJson = (path: string, body?: unknown, headers: Record<string, string> = {}, at = base) => call(path, { method: 'POST',
  headers: { 'Content-Type': 'application/json', ...headers }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) }, at);
async function openWallet(amount = '100.00') {
  const reply = await postJson('/wallets', { playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' } });
  expect(reply.status).toBe(201);
  return reply.body as { id: string; playerId: string };
}
function wager(w: { id: string; playerId: string }, kind: string, amount: string, reference?: string, provider = 'query-provider') {
  const externalTransactionId = randomUUID();
  return { key: 'key:' + externalTransactionId, body: { providerId: provider, externalTransactionId, walletId: w.id, playerId: w.playerId,
    roundId: 'round-q', gameId: 'game-q', kind, money: { amount, currency: 'BRL' },
    ...(reference === undefined ? {} : { referenceExternalTransactionId: reference }) } };
}
async function submit(c: ReturnType<typeof wager>) { return postJson('/wagering/transactions', c.body, { 'Idempotency-Key': c.key }); }
async function storedState(walletId: string) {
  const [row] = await q(`SELECT balance::text balance,version::text version,(SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger,
    (SELECT coalesce(sum(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END),0)::text FROM ledger_entries WHERE wallet_id=w.id) calculated
    FROM wallets w WHERE id=?`, [walletId]);
  return row!;
}
async function listen(options: WalletModuleOptions) {
  const root: DynamicModule = { module: class QueryTestRoot {}, imports: [WalletModule.register(orm, options), WageringModule.register(orm)] };
  const created = await NestFactory.create(root, { logger: false });
  apps.push(created);
  await created.listen(0, '127.0.0.1');
  return { app: created, base: await created.getUrl() };
}

beforeAll(async () => {
  const url = process.env['JWP_QUERY_DATABASE_URL'];
  if (!url) throw new Error('JWP_QUERY_DATABASE_URL required: empty disposable PostgreSQL. Integration never skips infrastructure.');
  orm = await openFinancialOrm(url);
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]!['relation']).toBeNull();
  await orm.migrator.up();
  migrated = true;
  ({ app, base } = await listen({ reconciliationMonitor: signals }));
}, 30000);
afterAll(async () => {
  for (const created of apps) await created.close();
  if (orm) { try { if (migrated) await orm.migrator.down({ to: 0 }); } finally { await orm.close(true); } }
});

test('POST/GET wallet over real HTTP: typed statuses, string money, updatedAt from the last ledger change', async () => {
  const playerId = randomUUID();
  const created = await postJson('/wallets', { playerId: playerId.toUpperCase(), initialBalance: { amount: '100.00', currency: 'BRL' } });
  expect(created).toMatchObject({ status: 201, body: { playerId, balance: { amount: '100.00', currency: 'BRL' }, version: 1 } });
  const w = created.body as { id: string; playerId: string };
  const first = await call(`/wallets/${w.id.toUpperCase()}`);
  expect(first).toMatchObject({ status: 200, body: { id: w.id, playerId, balance: { amount: '100.00', currency: 'BRL' }, version: 1 } });
  expect(first.body.updatedAt).toBe(first.body.createdAt);
  expect(Object.keys(first.body).sort()).toEqual(['balance', 'createdAt', 'id', 'playerId', 'updatedAt', 'version']);
  await Bun.sleep(5);
  expect((await submit(wager(w, 'BET', '25.10'))).status).toBe(200);
  const after = await call(`/wallets/${w.id}`);
  expect(after.body).toMatchObject({ balance: { amount: '74.90' }, version: 2, createdAt: first.body.createdAt });
  const [last] = await q('SELECT created_at FROM ledger_entries WHERE wallet_id=? ORDER BY wallet_version DESC LIMIT 1', [w.id]);
  expect(after.body.updatedAt).toBe(new Date(last!['created_at'] as string).toISOString());
  expect(Date.parse(after.body.updatedAt)).toBeGreaterThan(Date.parse(after.body.createdAt));

  expect((await submit(wager(w, 'LOSS', '0.00'))).status).toBe(200);
  expect((await call(`/wallets/${w.id}`)).body).toEqual(after.body);

  expect(await postJson('/wallets', { playerId, initialBalance: { amount: '1.00', currency: 'BRL' } }))
    .toMatchObject({ status: 409, body: { error: { code: 'WALLET_ALREADY_EXISTS', retryable: false } } });
  for (const invalid of [{ playerId, initialBalance: { amount: 1, currency: 'BRL' } }, { playerId: 'x', initialBalance: { amount: '1.00', currency: 'BRL' } },
    { playerId: randomUUID(), initialBalance: { amount: '1.0', currency: 'BRL' } }, { playerId: randomUUID(), initialBalance: { amount: '1.00', currency: 'BRL' }, extra: 1 }])
    expect(await postJson('/wallets', invalid)).toMatchObject({ status: 400, body: { error: { code: 'INVALID_PAYLOAD', retryable: false } } });
  expect(await call('/wallets/not-a-uuid')).toMatchObject({ status: 400, body: { error: { code: 'INVALID_PAYLOAD' } } });
  expect(await call(`/wallets/${randomUUID()}`)).toMatchObject({ status: 404, body: { error: { code: 'WALLET_NOT_FOUND', retryable: false } } });
  const zero = await openWallet('0.00');
  const zeroView = await call(`/wallets/${zero.id}`);
  expect(zeroView.body).toMatchObject({ balance: { amount: '0.00' }, version: 1 });
  expect(zeroView.body.updatedAt).toBe(zeroView.body.createdAt);
});

test('ledger cursor is stable under concurrent appends: no duplicate, no skip, deterministic order', async () => {
  const w = await openWallet('100.00');
  for (let i = 0; i < 5; i++) expect((await submit(wager(w, i % 2 ? 'WIN' : 'BET', '1.00'))).status).toBe(200);

  expect((await submit(wager(w, 'BET', '999.00'))).status).toBe(422);
  expect((await submit(wager(w, 'LOSS', '0.00'))).status).toBe(200);
  const seen: any[] = [];
  let page = await call(`/wallets/${w.id}/ledger?limit=2`);
  expect(page.status).toBe(200);
  expect(page.body.items).toHaveLength(2);
  seen.push(...page.body.items);
  for (let i = 0; i < 3; i++) expect((await submit(wager(w, 'WIN', '0.01'))).status).toBe(200);
  const cursors: string[] = [];
  while (page.body.nextCursor) {
    cursors.push(page.body.nextCursor);
    page = await call(`/wallets/${w.id}/ledger?limit=2&cursor=${encodeURIComponent(page.body.nextCursor)}`);
    expect(page.status).toBe(200);
    seen.push(...page.body.items);
    if (cursors.length === 1) for (let i = 0; i < 2; i++) expect((await submit(wager(w, 'BET', '0.01'))).status).toBe(200);
  }
  const rows = await q(`SELECT id::text id,transaction_id::text transaction_id,wallet_version::int v,direction,amount::text amount,
    balance_before::text before,balance_after::text after,created_at FROM ledger_entries WHERE wallet_id=? ORDER BY wallet_version,id`, [w.id]);
  expect(rows).toHaveLength(11);
  expect(seen.map(e => e.walletVersion)).toEqual(Array.from({ length: 11 }, (_, i) => i + 1));
  expect(new Set(seen.map(e => e.id)).size).toBe(11);
  seen.forEach((entry, i) => expect(entry).toEqual({ id: rows[i]!['id'], walletId: w.id, transactionId: rows[i]!['transaction_id'],
    walletVersion: rows[i]!['v'], direction: rows[i]!['direction'], money: { amount: rows[i]!['amount'], currency: 'BRL' },
    balanceBefore: { amount: rows[i]!['before'], currency: 'BRL' }, balanceAfter: { amount: rows[i]!['after'], currency: 'BRL' },
    createdAt: new Date(rows[i]!['created_at'] as string).toISOString() }));
  expect(seen[0]).toMatchObject({ walletVersion: 1, direction: 'CREDIT', balanceBefore: { amount: '0.00' }, money: { amount: '100.00' } });

  const again = await call(`/wallets/${w.id}/ledger?limit=2&cursor=${encodeURIComponent(cursors[0]!)}`);
  expect(again.body.items.map((e: any) => e.id)).toEqual(seen.slice(2, 4).map(e => e.id));
  const other = await openWallet('5.00');
  const otherCursor = (await call(`/wallets/${other.id}/ledger?limit=1`)).body.nextCursor;
  expect(otherCursor).toBeNull();
  expect(await call(`/wallets/${other.id}/ledger?cursor=${encodeURIComponent(cursors[0]!)}`))
    .toMatchObject({ status: 400, body: { error: { code: 'INVALID_CURSOR', retryable: false } } });
  for (const query of ['cursor=', 'cursor=%21%21', 'cursor=a&cursor=b'])
    expect((await call(`/wallets/${w.id}/ledger?${query}`)).body.error.code).toBe('INVALID_CURSOR');
  for (const limit of ['0', '101', 'x', '1.5'])
    expect(await call(`/wallets/${w.id}/ledger?limit=${limit}`)).toMatchObject({ status: 400, body: { error: { code: 'INVALID_PAYLOAD' } } });
  expect((await call(`/wallets/${randomUUID()}/ledger`)).status).toBe(404);
  expect(await call(`/wallets/${w.id}/ledger?limit=100`)).toMatchObject({ status: 200, body: { nextCursor: null } });
}, 30000);

test('default ledger limit is 50 and the continuation finishes the history', async () => {
  const w = await openWallet('1000.00');
  for (let i = 0; i < 54; i++) expect((await submit(wager(w, 'BET', '0.01'))).status).toBe(200);
  const first = await call(`/wallets/${w.id}/ledger`);
  expect(first.body.items).toHaveLength(50);
  expect(typeof first.body.nextCursor).toBe('string');
  const rest = await call(`/wallets/${w.id}/ledger?cursor=${encodeURIComponent(first.body.nextCursor)}`);
  expect(rest.body.items.map((e: any) => e.walletVersion)).toEqual([51, 52, 53, 54, 55]);
  expect(rest.body.nextCursor).toBeNull();
}, 30000);

test('internal and provider transaction lookups converge on the persisted representation', async () => {
  const w = await openWallet('20.00');
  const bet = wager(w, 'BET', '15.00'), rejected = wager(w, 'BET', '50.00'), pending = wager(w, 'REFUND', '1.00', 'not-yet');
  const processed = await submit(bet);
  const replay = await submit(bet);
  expect(replay.body.idempotentReplay).toBe(true);
  const rejection = await submit(rejected);
  expect(rejection.status).toBe(422);
  const waiting = await submit(pending);
  expect(waiting.status).toBe(202);
  const refund = wager(w, 'REFUND', '15.00', bet.body.externalTransactionId);
  const refunded = await submit(refund);
  expect((await submit(wager(w, 'WIN', '30.00'))).status).toBe(200);

  const internal = await call(`/wagering/transactions/${processed.body.transactionId}`);
  expect(internal.status).toBe(200);
  expect(internal.body).toMatchObject({ transactionId: processed.body.transactionId, providerId: 'query-provider',
    externalTransactionId: bet.body.externalTransactionId, walletId: w.id, playerId: w.playerId, roundId: 'round-q', gameId: 'game-q',
    kind: 'BET', money: { amount: '15.00', currency: 'BRL' }, status: 'PROCESSED', resultBalance: { amount: '5.00', currency: 'BRL' } });
  expect(Date.parse(internal.body.processedAt)).toBeGreaterThan(0);
  for (const hidden of ['idempotentReplay', 'payloadHash', 'idempotencyKey', 'failureCode', 'referenceTransactionId'])
    expect(internal.body).not.toHaveProperty(hidden);
  const external = await call(`/providers/query-provider/wagering/transactions/${encodeURIComponent(bet.body.externalTransactionId)}`);
  expect(external).toEqual(internal);
  expect(await call(`/wagering/transactions/${processed.body.transactionId.toUpperCase()}`)).toEqual(internal);


  const rejectedView = await call(`/wagering/transactions/${rejection.body.transactionId}`);
  expect(rejectedView).toMatchObject({ status: 200, body: { status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', resultBalance: { amount: '5.00' } } });
  expect(rejectedView.body).not.toHaveProperty('processedAt');
  const pendingView = await call(`/wagering/transactions/${waiting.body.transactionId}`);
  expect(pendingView.body).toMatchObject({ status: 'PENDING_REFERENCE', referenceExternalTransactionId: 'not-yet' });
  for (const absent of ['resultBalance', 'processedAt', 'failureCode', 'referenceTransactionId']) expect(pendingView.body).not.toHaveProperty(absent);
  const refundView = await call(`/providers/query-provider/wagering/transactions/${refund.body.externalTransactionId}`);
  expect(refundView.body).toMatchObject({ transactionId: refunded.body.transactionId, kind: 'REFUND', referenceTransactionId: processed.body.transactionId,
    referenceExternalTransactionId: bet.body.externalTransactionId, resultBalance: { amount: '20.00' } });


  const ledger = await call(`/wallets/${w.id}/ledger`);
  for (const entry of ledger.body.items) {
    const tx = await call(`/wagering/transactions/${entry.transactionId}`);
    expect(tx.status).toBe(200);
    expect(tx.body.money).toEqual(entry.money);
  }
  expect((await call(`/wagering/transactions/${ledger.body.items[0].transactionId}`)).body).toMatchObject({ kind: 'OPENING', status: 'PROCESSED', resultBalance: { amount: '20.00', currency: 'BRL' } });

  expect(await call(`/wagering/transactions/${randomUUID()}`)).toMatchObject({ status: 404, body: { error: { code: 'TRANSACTION_NOT_FOUND' } } });
  expect((await call(`/providers/QUERY-PROVIDER/wagering/transactions/${bet.body.externalTransactionId}`)).status).toBe(404);
  expect((await call(`/providers/query-provider/wagering/transactions/${bet.body.externalTransactionId.toUpperCase()}`)).status).toBe(404);
  expect(await call('/wagering/transactions/123')).toMatchObject({ status: 400, body: { error: { code: 'INVALID_PAYLOAD' } } });
  expect((await call('/providers/query-provider/wagering/transactions/%20')).status).toBe(400);
}, 30000);

test('reconciliation: OPENING included, LOSS/rejections excluded, exact beyond one balance range', async () => {
  const w = await openWallet('10.00');
  await submit(wager(w, 'BET', '0.01'));
  await submit(wager(w, 'WIN', '0.02'));
  await submit(wager(w, 'LOSS', '0.00'));
  expect((await submit(wager(w, 'BET', '500.00'))).status).toBe(422);
  expect(await postJson(`/wallets/${w.id}/reconciliation`)).toEqual({ status: 200, body: { walletId: w.id,
    storedBalance: { amount: '10.01', currency: 'BRL' }, calculatedBalance: { amount: '10.01', currency: 'BRL' },
    difference: { amount: '0.00', currency: 'BRL' }, consistent: true, checkedEntries: 3 } });
  expect(await storedState(w.id)).toMatchObject({ balance: '10.01', calculated: '10.01', ledger: 3 });


  const max = '999999999999999999.99', big = await openWallet(max);
  expect((await submit(wager(big, 'BET', max))).body.balance).toEqual({ amount: '0.00', currency: 'BRL' });
  expect((await submit(wager(big, 'WIN', max))).body.balance).toEqual({ amount: max, currency: 'BRL' });
  expect((await postJson(`/wallets/${big.id}/reconciliation`, {})).body).toMatchObject({
    storedBalance: { amount: max }, calculatedBalance: { amount: max }, difference: { amount: '0.00' }, consistent: true, checkedEntries: 3 });

  const zero = await openWallet('0.00');
  expect((await postJson(`/wallets/${zero.id}/reconciliation`)).body).toMatchObject({ calculatedBalance: { amount: '0.00' }, consistent: true, checkedEntries: 0 });
  expect(await postJson(`/wallets/${w.id}/reconciliation`, { repair: true })).toMatchObject({ status: 400, body: { error: { code: 'INVALID_PAYLOAD' } } });
  expect(await postJson(`/wallets/${randomUUID()}/reconciliation`)).toMatchObject({ status: 404, body: { error: { code: 'WALLET_NOT_FOUND' } } });
  expect((await postJson('/wallets/nope/reconciliation')).status).toBe(400);
  expect(logLines).toEqual([]);
}, 30000);

test('reconciliation during appends from three separate processes never reports a false divergence', async () => {
  const w = await openWallet('1000.00');
  const writers = [1, 2, 3].map(() => Bun.spawn(['bun', 'tests/integration/query/ledger-writer.ts'], {
    stdin: 'pipe', stdout: 'pipe', stderr: 'inherit',
    env: { ...process.env, JWP_LEDGER_WRITER: JSON.stringify({ walletId: w.id, playerId: w.playerId, count: 30 }) } }));
  const readers = writers.map(writer => writer.stdout.getReader());
  const ready = await Promise.all(readers.map(async reader => JSON.parse(new TextDecoder().decode((await reader.read()).value))));
  expect(new Set([process.pid, ...ready.map(r => r.pid)]).size).toBe(4);
  for (const writer of writers) { writer.stdin.write('go\n'); writer.stdin.end(); }
  let finished = false;
  const done = Promise.all(writers.map(async (writer, i) => {
    let output = '';
    for (;;) { const chunk = await readers[i]!.read(); if (chunk.done) break; output += new TextDecoder().decode(chunk.value); }
    expect(await writer.exited).toBe(0);
    return JSON.parse(output.trim()) as { statuses: number[] };
  })).finally(() => { finished = true; });
  const observed: any[] = [];
  while (!finished) observed.push((await postJson(`/wallets/${w.id}/reconciliation`)).body);
  const results = await done;
  expect(results.flatMap(r => r.statuses)).toEqual(Array(90).fill(200));
  expect(observed.length).toBeGreaterThan(3);
  expect(observed.filter(r => !r.consistent)).toEqual([]);
  expect(new Set(observed.map(r => r.checkedEntries)).size).toBeGreaterThan(1);
  const final = (await postJson(`/wallets/${w.id}/reconciliation`)).body;

  expect(final).toMatchObject({ storedBalance: { amount: '1045.45' }, calculatedBalance: { amount: '1045.45' }, consistent: true, checkedEntries: 91 });
  expect(logLines).toEqual([]);
  console.log(JSON.stringify({ proof: 'concurrent-reconciliation', pids: ready.map(r => r.pid), reconciliations: observed.length,
    checkedEntries: [...new Set(observed.map(r => r.checkedEntries))].length, final }));
}, 60000);

test('reconciliation holds the writers\' wallet lock; a held lock ends in typed retryable 503', async () => {
  const w = await openWallet('10.00');
  const limited = await listen({ sqlPolicy: { lockTimeoutMs: 60, statementTimeoutMs: 300, transactionTimeoutMs: 400, budgetMs: 1500, maxAttempts: 2, retryBaseMs: 5 } });
  const blocker = orm.em.fork(); await blocker.begin();
  try {
    await blocker.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE', [w.id], 'all', blocker.getTransactionContext());
    expect(await postJson(`/wallets/${w.id}/reconciliation`, undefined, {}, limited.base))
      .toMatchObject({ status: 503, body: { error: { code: 'INFRASTRUCTURE_UNAVAILABLE', retryable: true } } });

    expect((await call(`/wallets/${w.id}`, {}, limited.base)).status).toBe(200);
  } finally { await blocker.rollback(); }
  expect((await postJson(`/wallets/${w.id}/reconciliation`, undefined, {}, limited.base)).body.consistent).toBe(true);
}, 15000);

test('controlled divergence in the disposable DB is flagged in response, log and metric, and never repaired', async () => {
  const over = await openWallet('50.00'), under = await openWallet('50.00');
  await submit(wager(over, 'BET', '10.00'));
  const tamper = async (walletId: string, balance: string) => {
    // Somente o banco descartável permite ignorar os triggers diferidos como owner.
    const em = orm.em.fork(); await em.begin();
    try {
      await em.getConnection().execute('SET LOCAL session_replication_role = replica', [], 'all', em.getTransactionContext());
      await em.getConnection().execute('UPDATE wallets SET balance=? WHERE id=?', [balance, walletId], 'all', em.getTransactionContext());
      await em.commit();
    } catch (error) { await em.rollback(); throw error; }
  };
  await tamper(over.id, '45.50');
  await tamper(under.id, '20.00');
  const before = [await storedState(over.id), await storedState(under.id)];
  const metricBefore = signals.metrics().find(m => m.labels.outcome === 'divergent')?.value ?? 0;

  const a = await postJson(`/wallets/${over.id}/reconciliation`);
  expect(a).toEqual({ status: 200, body: { walletId: over.id, storedBalance: { amount: '45.50', currency: 'BRL' },
    calculatedBalance: { amount: '40.00', currency: 'BRL' }, difference: { amount: '5.50', currency: 'BRL' }, consistent: false, checkedEntries: 2 } });
  const b = await postJson(`/wallets/${under.id}/reconciliation`);
  expect(b.body).toMatchObject({ difference: { amount: '-30.00', currency: 'BRL' }, consistent: false, checkedEntries: 1 });

  expect((await postJson(`/wallets/${over.id}/reconciliation`)).body).toEqual(a.body);
  expect([await storedState(over.id), await storedState(under.id)]).toEqual(before);

  expect(logLines).toHaveLength(3);
  const logs = logLines.map(line => JSON.parse(line));
  expect(logs.map(l => l.walletId)).toEqual([over.id, under.id, over.id]);
  for (const log of logs) expect(log).toMatchObject({ level: 'error', type: 'wallet.reconciliation.divergent', repaired: false, currency: 'BRL' });
  expect(logLines.join('')).not.toContain('45.50');
  expect(signals.metrics().find(m => m.labels.outcome === 'divergent')!.value - metricBefore).toBe(3);
  expect(JSON.stringify(signals.metrics())).not.toContain(over.id);

  expect((await submit(wager(over, 'BET', '1.00'))).status).toBe(500);
  expect(await storedState(over.id)).toEqual(before[0]!);
  console.log(JSON.stringify({ proof: 'controlled-divergence', over: a.body, under: b.body, logs }));
}, 15000);
