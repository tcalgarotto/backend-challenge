import 'reflect-metadata';
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { NestFactory } from '@nestjs/core';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { WageringModule } from '../../../src/wagering/http/wagering.module.js';
import { ProcessWager } from '../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../src/wagering/infrastructure/sql-process-wager-store.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let open: OpenWallet;
let app: Awaited<ReturnType<typeof NestFactory.create>>;
let base: string;
let container: string;
let migrated = false;
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute(sql, args);
const command = (w: { id: string; playerId: string }) => ({
  providerId: 'review-provider', externalTransactionId: randomUUID(), walletId: w.id,
  playerId: w.playerId, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
});
const post = async (body: unknown, key: string = randomUUID()) => {
  const response = await fetch(base + '/wagering/transactions', { method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) });
  return { httpStatus: response.status, body: await response.json() };
};
const state = async (id: string) => (await q(`SELECT balance::text,version::text,
  (SELECT count(*)::int FROM wager_transactions WHERE wallet_id=w.id) wagers,
  (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger,
  (SELECT count(*)::int FROM outbox_events WHERE wallet_id=w.id) outbox,
  (SELECT coalesce(sum(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END),0)::numeric(20,2)::text
    FROM ledger_entries WHERE wallet_id=w.id) calculated
  FROM wallets w WHERE id=?`, [id]))[0];
async function restartDatabase() {
  Bun.spawnSync(['docker', 'start', container]);
  for (let i = 0; i < 100; i++) {
    if (Bun.spawnSync(['docker', 'exec', container, 'pg_isready', '-U', 'jwp']).exitCode === 0) return;
    await Bun.sleep(100);
  }
  throw new Error('Dedicated PostgreSQL did not restart.');
}
beforeAll(async () => {
  const url = process.env['JWP_WAGER_DATABASE_URL'];
  const name = process.env['JWP_WAGER_POSTGRES_CONTAINER'];
  if (!url || !name?.startsWith('jwp-wagerfix-')) throw new Error('Dedicated JWP_WAGER_DATABASE_URL and JWP_WAGER_POSTGRES_CONTAINER (jwp-wagerfix-*) required.');
  container = name;
  orm = await openFinancialOrm(url);
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]!['relation']).toBeNull();
  await orm.migrator.up();
  migrated = true;
  open = new OpenWallet(new SqlWalletOpeningStore(orm));
  app = await NestFactory.create(WageringModule.register(orm), { logger: false });
  await app.listen(0, '127.0.0.1');
  base = await app.getUrl();
});
afterAll(async () => {
  if (app) await app.close();
  if (orm) { try { if (migrated) await orm.migrator.down({ to: 0 }); } finally { await orm.close(true); } }
});

test('NUL/unpaired surrogates in all JSON identifiers return typed HTTP400 without financial effects', async () => {
  const w = await open.execute({ playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
  const before = await state(w.id);
  const observations = [];
  for (const invalid of ['invalid\u0000identifier', 'invalid\uD800identifier', 'invalid\uDC00identifier']) {
    for (const field of ['providerId', 'externalTransactionId', 'roundId', 'gameId', 'referenceExternalTransactionId']) {
      const result = await post({ ...command(w), kind:'WIN', [field]: invalid });
      observations.push({ field, ...result });
      expect(result).toMatchObject({httpStatus:400,body:{error:{code:'INVALID_PAYLOAD',retryable:false}}});
      expect(await state(w.id)).toEqual(before);
    }
  }
  console.log(JSON.stringify({ proof: 'NUL-input', observations, before, after: await state(w.id) }));
  expect(observations).toHaveLength(15);
});

test('existing idempotency identity with a different missing wallet should remain a conflict', async () => {
  const w = await open.execute({ playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
  const c = command(w), key = randomUUID();
  expect((await post(c, key)).httpStatus).toBe(200);
  const before = await state(w.id);
  const response = await post({ ...c, walletId: randomUUID() }, key);
  expect(await state(w.id)).toEqual(before);
  console.log(JSON.stringify({ proof: 'missing-wallet-conflict', response, before }));
  expect(response).toMatchObject({ httpStatus: 409, body: { error: { code: 'IDEMPOTENCY_CONFLICT' } } });
  const absentWallet = randomUUID();
  expect(await post({ ...command(w), walletId: absentWallet })).toMatchObject({httpStatus:404,body:{error:{code:'WALLET_NOT_FOUND'}}});
  expect(await post({ ...c, walletId: absentWallet })).toMatchObject({httpStatus:409,body:{error:{code:'EXTERNAL_IDENTITY_CONFLICT'}}});
  expect(await state(w.id)).toEqual(before);
});

test('uncoded application errors are not relabeled or retried as connection failures', async()=>{
  const w = await open.execute({ playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
  const before = await state(w.id), error = new Error('controlled application failure');
  let attempts = 0;
  class FaultyStore extends SqlProcessWagerStore {
    override async processInTransaction(): Promise<never> { attempts++; throw error; }
  }
  await expect(new ProcessWager(new FaultyStore(orm)).execute(command(w),'application-fault')).rejects.toBe(error);
  expect(attempts).toBe(1);
  expect(await state(w.id)).toEqual(before);
});

test('well-formed accented text and emoji persist unchanged and replay safely',async()=>{
  const w = await open.execute({ playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
  const c = {...command(w),providerId:'provedor-á-😀',externalTransactionId:'aposta-é-😀',roundId:'rodada-ç-😀',gameId:'jogo-ã-😀'};
  const key = 'opaque-unicode-key', first = await post(c,key);
  expect(first).toMatchObject({httpStatus:200,body:{idempotentReplay:false,balance:{amount:'75.00'}}});
  expect(await post(c,key)).toEqual({httpStatus:200,body:{...first.body,idempotentReplay:true}});
  const [row] = await q('SELECT provider_id,external_transaction_id,round_id,game_id FROM wager_transactions WHERE id=?',[first.body.transactionId]);
  expect(row).toEqual({provider_id:c.providerId,external_transaction_id:c.externalTransactionId,round_id:c.roundId,game_id:c.gameId});
  const persistedEvents = await q(`SELECT event_type,transaction_id,envelope->>'correlationId' AS correlation_id
    FROM outbox_events WHERE transaction_id=? ORDER BY seq`, [first.body.transactionId]) as
    { event_type: string; transaction_id: string; correlation_id: string }[];
  expect(persistedEvents.map(event => event.event_type)).toEqual([
    'WagerTransactionProcessed', 'WalletBalanceChanged',
  ]);
  expect(persistedEvents).toHaveLength(2);
  expect(new Set(persistedEvents.map(event => event.correlation_id)).size).toBe(1);
  expect(await state(w.id)).toMatchObject({balance:'75.00',calculated:'75.00',version:'2',wagers:2,ledger:2,outbox:4});
});

test('abrupt database loss during a blocked request should return503 and allow same-key recovery', async () => {
  const w = await open.execute({ playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
  const before = await state(w.id), c = command(w), key = randomUUID();
  const blocker = orm.em.fork();
  await blocker.begin();
  await blocker.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE', [w.id], 'all', blocker.getTransactionContext());
  const pending = post(c, key);
  let loss: Awaited<ReturnType<typeof post>>;
  try {
    let reached = false;
    for (let i = 0; i < 100; i++) {
      const [r] = await q("SELECT count(*)::int n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query ILIKE 'select%from \"wallets\"%for update%'");
      if (r!['n'] > 0) { reached = true; break; }
      await Bun.sleep(5);
    }
    expect(reached).toBe(true);
    const killed = Bun.spawnSync(['docker', 'kill', '--signal=KILL', container]);
    expect(killed.exitCode).toBe(0);
    loss = await pending;
  } finally {
    await restartDatabase();
    await blocker.rollback().catch(() => undefined);
  }
  expect(await state(w.id)).toEqual(before);
  const recovered = await post(c, key), replay = await post(c, key);
  expect(recovered).toMatchObject({ httpStatus: 200, body: { idempotentReplay: false, balance: { amount: '75.00' } } });
  expect(replay).toMatchObject({ httpStatus: 200, body: { idempotentReplay: true } });
  expect(replay.body.transactionId).toBe(recovered.body.transactionId);
  expect(await state(w.id)).toMatchObject({balance:'75.00',calculated:'75.00',version:'2',wagers:2,ledger:2,outbox:4});
  console.log(JSON.stringify({ proof: 'abrupt-postgres-loss', loss: loss!, recovered, replay, before, after: await state(w.id) }));
  expect(loss!).toMatchObject({ httpStatus: 503, body: { error: { code: 'INFRASTRUCTURE_UNAVAILABLE', retryable: true } } });
}, 30000);

test('database loss with idle pooled connections returns typed503, not500, and the same key recovers once', async () => {
  const w = await open.execute({ playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
  const observations = [];
  for (let round = 1; round <= 3; round++) {
    const before = await state(w.id), c = command(w), key = randomUUID();
    // Aquece o pool para que os sockets morram ociosos e o próximo BEGIN
    // os use antes de o pool detectar a queda.
    await Promise.all([1, 2, 3, 4].map(() => q('SELECT pg_sleep(0.05)')));
    let loss: Awaited<ReturnType<typeof post>>;
    try {
      expect(Bun.spawnSync(['docker', 'kill', '--signal=KILL', container]).exitCode).toBe(0);
      loss = await post(c, key);
    } finally {
      await restartDatabase();
    }
    expect(await state(w.id)).toEqual(before);
    const recovered = await post(c, key), replay = await post(c, key);
    observations.push({ round, loss: loss!, recovered, replay });
    expect(loss!).toMatchObject({ httpStatus: 503, body: { error: { code: 'INFRASTRUCTURE_UNAVAILABLE', retryable: true } } });
    expect(recovered).toMatchObject({ httpStatus: 200, body: { idempotentReplay: false } });
    expect(replay).toMatchObject({ httpStatus: 200, body: { idempotentReplay: true, transactionId: recovered.body.transactionId } });
  }
  console.log(JSON.stringify({ proof: 'idle-pool-postgres-loss', observations, after: await state(w.id) }));
  expect(await state(w.id)).toMatchObject({ balance: '25.00', calculated: '25.00', version: '4', wagers: 4, ledger: 4, outbox: 8 });
}, 60000);
