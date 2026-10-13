import 'reflect-metadata';
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { NestFactory } from '@nestjs/core';
import type { DynamicModule, INestApplication } from '@nestjs/common';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { WageringModule } from '../../../src/wagering/http/wagering.module.js';
import { WalletModule } from '../../../src/wallet/http/wallet.module.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let app: INestApplication, base: string, container: string, migrated = false;
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<Record<string, unknown>[]>(sql, args);
async function call(method: string, path: string, body?: unknown) {
  const response = await fetch(base + path, { method, headers: { 'Content-Type': 'application/json' },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
  return { status: response.status, body: await response.json() as any };
}
async function restartDatabase() {
  Bun.spawnSync(['docker', 'start', container]);
  for (let i = 0; i < 100; i++) {
    if (Bun.spawnSync(['docker', 'exec', container, 'pg_isready', '-U', 'jwp']).exitCode === 0) return;
    await Bun.sleep(100);
  }
  throw new Error('Dedicated PostgreSQL did not restart.');
}
const financialState = async (walletId: string) => (await q(`SELECT balance::text balance,version::text version,
  (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger,(SELECT count(*)::int FROM wallets) wallets
  FROM wallets w WHERE id=?`, [walletId]))[0];

beforeAll(async () => {
  const url = process.env['JWP_QUERY_DATABASE_URL'];
  const name = process.env['JWP_QUERY_POSTGRES_CONTAINER'];
  if (!url || !name?.startsWith('jwp-query-'))
    throw new Error('Dedicated JWP_QUERY_DATABASE_URL and JWP_QUERY_POSTGRES_CONTAINER (jwp-query-*) required. Integration never skips infrastructure.');
  container = name;
  orm = await openFinancialOrm(url);
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]!['relation']).toBeNull();
  await orm.migrator.up();
  migrated = true;
  const root: DynamicModule = { module: class OutageTestRoot {}, imports: [WalletModule.register(orm, { reconciliationMonitor: { observe() {} } }), WageringModule.register(orm)] };
  app = await NestFactory.create(root, { logger: false });
  await app.listen(0, '127.0.0.1');
  base = await app.getUrl();
}, 30000);
afterAll(async () => {
  if (app) await app.close();
  if (orm) { try { if (migrated) await orm.migrator.down({ to: 0 }); } finally { await orm.close(true); } }
});

test('database loss with idle pooled sockets: every new route answers typed 503, never 500, then recovers', async () => {
  const opened = await call('POST', '/wallets', { playerId: randomUUID(), initialBalance: { amount: '30.00', currency: 'BRL' } });
  expect(opened.status).toBe(201);
  const walletId = opened.body.id as string;
  const [{ transaction_id: transactionId }] = await q('SELECT transaction_id::text FROM ledger_entries WHERE wallet_id=?', [walletId]) as [{ transaction_id: string }];
  const routes: [string, string, unknown?][] = [
    ['POST', '/wallets', { playerId: randomUUID(), initialBalance: { amount: '1.00', currency: 'BRL' } }],
    ['GET', `/wallets/${walletId}`],
    ['GET', `/wallets/${walletId}/ledger?limit=1`],
    ['GET', `/wagering/transactions/${transactionId}`],
    ['GET', `/providers/internal:wallet-opening/wagering/transactions/opening:${walletId}`],
    ['POST', `/wallets/${walletId}/reconciliation`],
  ];
  const observations = [];
  for (const [method, path, body] of routes) {
    const before = await financialState(walletId);
    // Aquece o pool para que BEGIN use um socket morto ainda não removido.
    await Promise.all([1, 2, 3, 4].map(() => q('SELECT pg_sleep(0.05)')));
    let loss: Awaited<ReturnType<typeof call>>;
    try {
      expect(Bun.spawnSync(['docker', 'kill', '--signal=KILL', container]).exitCode).toBe(0);
      loss = await call(method, path, body);
    } finally { await restartDatabase(); }
    const recovered = await call(method, path, body);
    observations.push({ method, path: path.replace(walletId, ':walletId').replace(transactionId, ':transactionId'), loss: loss!, recovered: recovered.status });
    expect(loss!).toMatchObject({ status: 503, body: { error: { code: 'INFRASTRUCTURE_UNAVAILABLE', retryable: true } } });
    expect(loss!.body.correlationId).toMatch(/^[0-9a-f-]{36}$/);
    expect(recovered.status).toBe(method === 'POST' && path === '/wallets' ? 201 : 200);
    if (path !== '/wallets') expect(await financialState(walletId)).toEqual(before);
  }
  console.log(JSON.stringify({ proof: 'idle-pool-postgres-loss-query-routes', observations }));
  const reconciled = await call('POST', `/wallets/${walletId}/reconciliation`);
  expect(reconciled.body).toMatchObject({ storedBalance: { amount: '30.00' }, calculatedBalance: { amount: '30.00' }, consistent: true, checkedEntries: 1 });
}, 120000);
