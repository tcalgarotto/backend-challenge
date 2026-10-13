import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { INestApplication } from '@nestjs/common';
import { Observability } from '../../../src/observability/observability.js';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { call, infrastructure, logCapture, migratedOrm, observedApp, sample, wager, type Orm } from './support.js';

let orm: Orm, app: INestApplication, base: string;
const logs = logCapture();
const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

const OPENING = '987654.32', BET = '1234.56', TOO_BIG = '99999999.99';

beforeAll(async () => {
  orm = await migratedOrm(infrastructure().databaseUrl);
  const observability = new Observability({ orm, logger: logs.logger, readinessTimeoutMs: 1000 });
  ({ app, base } = await observedApp(orm, observability));
}, 30000);

afterAll(async () => {
  if (app) await app.close();
  else if (orm) await orm.close(true);
  const cleanup = await openFinancialOrm(infrastructure().databaseUrl);
  try { await cleanup.migrator.down({ to: 0 }); } finally { await cleanup.close(true); }
});

test('O01/O02: success, replay, rejection and invalid input produce correlated JSON logs and moving metrics, without money', async () => {
  const before = (await call(base, 'GET', '/metrics')).text;
  const playerId = randomUUID();
  const opened = await call(base, 'POST', '/wallets', { playerId, initialBalance: { amount: OPENING, currency: 'BRL' } });
  expect(opened.status).toBe(201);
  const wallet = { id: opened.body.id as string, playerId };

  const bet = wager(wallet, 'BET', BET);
  const key = `obs-key-${randomUUID()}`;
  const first = await call(base, 'POST', '/wagering/transactions', bet, { 'Idempotency-Key': key });
  const replay = await call(base, 'POST', '/wagering/transactions', bet, { 'Idempotency-Key': key });
  expect(first).toMatchObject({ status: 200, body: { status: 'PROCESSED', idempotentReplay: false } });
  expect(replay).toMatchObject({ status: 200, body: { status: 'PROCESSED', idempotentReplay: true, transactionId: first.body.transactionId } });

  const rejected = await call(base, 'POST', '/wagering/transactions', wager(wallet, 'BET', TOO_BIG), { 'Idempotency-Key': `obs-key-${randomUUID()}` });
  expect(rejected).toMatchObject({ status: 422, body: { status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS' } });
  const invalid = await call(base, 'POST', '/wagering/transactions', '{"money":{"amount":"5', { 'Idempotency-Key': 'k' });
  expect(invalid.status).toBe(400);
  const missingKey = await call(base, 'POST', '/wagering/transactions', wager(wallet, 'BET', '1.00'));
  expect(missingKey).toMatchObject({ status: 400, body: { error: { code: 'IDEMPOTENCY_KEY_REQUIRED' } } });
  const read = await call(base, 'GET', `/wallets/${wallet.id}`);
  expect(read.status).toBe(200);
  expect(read.text).toContain('balance');

  const entries = logs.entries();
  for (const line of logs.lines) {
    for (const leak of [OPENING, BET, TOO_BIG, '986419.76', 'amount', 'balance', 'money', playerId]) expect(line).not.toContain(leak);
  }
  const requests = entries.filter(entry => entry['type'] === 'http.request');
  for (const entry of requests) {
    // Matchers assimétricos do Bun podem alterar o objeto recebido; usar checks simples preserva a fixture.
    expect(['info', 'warn', 'error']).toContain(entry['level'] as string);
    expect([typeof entry['at'], typeof entry['method'], typeof entry['route'], typeof entry['httpStatus'], typeof entry['durationMs']])
      .toEqual(['string', 'string', 'string', 'number', 'number']);
    expect(String(entry['route'])).not.toMatch(UUID);
  }
  const wagers = requests.filter(entry => entry['route'] === '/wagering/transactions');
  expect(wagers.find(entry => entry['transactionId'] === first.body.transactionId && entry['idempotentReplay'] === false)).toMatchObject({
    level: 'info', httpStatus: 200, status: 'PROCESSED', kind: 'BET', walletId: wallet.id, providerId: 'obs-provider' });
  expect(wagers.find(entry => entry['idempotentReplay'] === true)).toMatchObject({ transactionId: first.body.transactionId, walletId: wallet.id });
  expect(wagers.find(entry => entry['httpStatus'] === 422)).toMatchObject({ level: 'warn', status: 'REJECTED',
    failureCode: 'INSUFFICIENT_FUNDS', transactionId: rejected.body.transactionId, walletId: wallet.id });
  const keyless = wagers.find(entry => entry['errorCode'] === 'IDEMPOTENCY_KEY_REQUIRED')!;
  expect(keyless).toMatchObject({ level: 'warn', httpStatus: 400, correlationId: missingKey.body.correlationId });
  expect(keyless['transactionId']).toBeUndefined();

  expect(requests.find(entry => entry['errorCode'] === 'INVALID_PAYLOAD')).toMatchObject({ level: 'warn', method: 'POST',
    route: 'unmatched', httpStatus: 400, correlationId: invalid.body.correlationId });
  expect(requests.find(entry => entry['route'] === '/wallets/:walletId')).toMatchObject({ walletId: wallet.id, httpStatus: 200 });

  const after = (await call(base, 'GET', '/metrics')).text;
  const delta = (series: string) => sample(after, series) - sample(before, series);
  expect(delta('jwp_wager_transactions_total{channel="http",status="PROCESSED",kind="BET"}')).toBe(2);
  expect(delta('jwp_wager_transactions_total{channel="http",status="REJECTED",kind="BET"}')).toBe(1);
  expect(delta('jwp_duplicates_total{layer="idempotency"}')).toBe(1);
  expect(delta('jwp_http_requests_total{method="POST",route="/wagering/transactions",status_code="400"}')).toBe(1);
  expect(delta('jwp_http_requests_total{method="POST",route="unmatched",status_code="400"}')).toBe(1);
  expect(delta('jwp_http_requests_total{method="GET",route="/wallets/:walletId",status_code="200"}')).toBe(1);
  expect(sample(after, 'jwp_http_request_duration_seconds_count{method="POST",route="/wallets"}')).toBeGreaterThanOrEqual(1);

  expect(sample(after, 'jwp_outbox_pending_events')).toBeGreaterThanOrEqual(2);
  expect(sample(after, 'jwp_metrics_collector_up{collector="postgres"}')).toBe(1);

  expect(after).not.toMatch(UUID);
  for (const leak of [OPENING, BET, TOO_BIG]) expect(after).not.toContain(leak);
}, 30000);

test('O05: successful opening, BET, replay and reconciliation log the correlationId the use case and its events used', async () => {
  const mark = logs.lines.length;
  const playerId = randomUUID();
  const opened = await call(base, 'POST', '/wallets', { playerId, initialBalance: { amount: '80.00', currency: 'BRL' } });
  expect(opened.status).toBe(201);
  const wallet = { id: opened.body.id as string, playerId };
  const bet = wager(wallet, 'BET', '12.00');
  const key = `obs-key-${randomUUID()}`;
  const first = await call(base, 'POST', '/wagering/transactions', bet, { 'Idempotency-Key': key });
  const replay = await call(base, 'POST', '/wagering/transactions', bet, { 'Idempotency-Key': key });
  expect(first).toMatchObject({ status: 200, body: { status: 'PROCESSED', idempotentReplay: false } });
  expect(replay).toMatchObject({ status: 200, body: { idempotentReplay: true, transactionId: first.body.transactionId } });
  const reconciled = await call(base, 'POST', `/wallets/${wallet.id}/reconciliation`);
  expect(reconciled).toMatchObject({ status: 200, body: { consistent: true } });
  const live = await call(base, 'GET', '/health/live');
  expect(live.body).toEqual({ status: 'ok' });

  const requests = logs.lines.slice(mark).map(line => JSON.parse(line) as Record<string, unknown>).filter(entry => entry['type'] === 'http.request');
  expect(requests.map(entry => [entry['method'], entry['route'], entry['httpStatus']])).toEqual([
    ['POST', '/wallets', 201], ['POST', '/wagering/transactions', 200], ['POST', '/wagering/transactions', 200],
    ['POST', '/wallets/:walletId/reconciliation', 200]]);
  for (const entry of requests) {
    expect(entry['level']).toBe('info');
    expect(String(entry['correlationId'])).toMatch(UUID);
    expect(entry['walletId']).toBe(wallet.id);
  }
  const [openLog, betLog, replayLog] = requests;
  expect(new Set(requests.map(entry => entry['correlationId'])).size).toBe(4);
  expect(replayLog).toMatchObject({ idempotentReplay: true, transactionId: first.body.transactionId });


  const events = await orm.em.fork().getConnection().execute<Record<string, unknown>[]>(
    "SELECT transaction_id::text tx, envelope->>'correlationId' correlation FROM outbox_events WHERE wallet_id=? ORDER BY created_at, event_id", [wallet.id]);
  const byTransaction = (tx: unknown) => [...new Set(events.filter(row => row['tx'] === tx).map(row => row['correlation']))];
  expect(byTransaction(first.body.transactionId)).toEqual([betLog!['correlationId']]);
  const opening = events.filter(row => row['tx'] !== first.body.transactionId);
  expect(opening.length).toBeGreaterThan(0);
  expect([...new Set(opening.map(row => row['correlation']))]).toEqual([openLog!['correlationId']]);
  expect(events.some(row => row['correlation'] === replayLog!['correlationId'])).toBe(false);
  for (const line of logs.lines.slice(mark)) for (const leak of ['80.00', '12.00', '68.00', 'amount', 'balance', playerId]) expect(line).not.toContain(leak);
}, 30000);

test('O04: controlled divergence is flagged in the response, logged, counted and never repaired', async () => {
  const playerId = randomUUID();
  const opened = await call(base, 'POST', '/wallets', { playerId, initialBalance: { amount: '50.00', currency: 'BRL' } });
  const walletId = opened.body.id as string;
  // Somente o banco descartável permite ignorar os triggers diferidos como owner.
  const em = orm.em.fork(); await em.begin();
  try {
    await em.getConnection().execute('SET LOCAL session_replication_role = replica', [], 'all', em.getTransactionContext());
    await em.getConnection().execute("UPDATE wallets SET balance='45.50' WHERE id=?", [walletId], 'all', em.getTransactionContext());
    await em.commit();
  } catch (error) { await em.rollback(); throw error; }
  const before = (await call(base, 'GET', '/metrics')).text;
  const mark = logs.lines.length;
  const first = await call(base, 'POST', `/wallets/${walletId}/reconciliation`);
  const second = await call(base, 'POST', `/wallets/${walletId}/reconciliation`);
  for (const result of [first, second]) {
    expect(result).toMatchObject({ status: 200, body: { consistent: false, difference: { amount: '-4.50', currency: 'BRL' } } });
  }
  const [row] = await orm.em.fork().getConnection().execute<Record<string, unknown>[]>('SELECT balance::text balance FROM wallets WHERE id=?', [walletId]);
  expect(row!['balance']).toBe('45.50');
  const divergent = logs.lines.slice(mark).map(line => JSON.parse(line)).filter(entry => entry.type === 'wallet.reconciliation.divergent');
  expect(divergent).toHaveLength(2);
  for (const entry of divergent) {
    expect(entry).toMatchObject({ level: 'error', walletId, currency: 'BRL', checkedEntries: 1, repaired: false });
    expect(entry.correlationId).toMatch(UUID);
  }
  const reconciliationLogs = logs.lines.slice(mark).map(line => JSON.parse(line)).filter(entry => entry.type === 'http.request');
  expect(reconciliationLogs.map(entry => entry.correlationId)).toEqual(divergent.map(entry => entry.correlationId));
  for (const line of logs.lines.slice(mark)) for (const leak of ['45.50', '50.00', '4.50', 'amount']) expect(line).not.toContain(leak);
  const after = (await call(base, 'GET', '/metrics')).text;
  expect(sample(after, 'jwp_wallet_reconciliations_total{outcome="divergent",currency="BRL"}')
    - sample(before, 'jwp_wallet_reconciliations_total{outcome="divergent",currency="BRL"}')).toBe(2);
  expect(after).not.toMatch(UUID);
}, 30000);
