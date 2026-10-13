import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { OpenWallet, WalletOpeningError } from '../../../src/wallet/application/open-wallet.js';
import { openingProvider, SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let open: OpenWallet;
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<Record<string, unknown>[]>(sql,args);
const input = (amount = '100.00', playerId: string = randomUUID()) => ({ playerId, initialBalance: { amount, currency: 'BRL' } });
async function counts() {
  return (await q(`SELECT (SELECT count(*)::int FROM wallets) wallets,
    (SELECT count(*)::int FROM wager_transactions) wagers,
    (SELECT count(*)::int FROM ledger_entries) ledger,
    (SELECT count(*)::int FROM outbox_events) outbox`))[0];
}
beforeAll(async () => {
  const url = process.env['JWP_WALLET_DATABASE_URL'];
  if (!url) throw new Error('JWP_WALLET_DATABASE_URL is required; use an empty disposable PostgreSQL database. No infrastructure skip.');
  orm = await openFinancialOrm(url);
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]?.['relation']).toBeNull();
  await orm.migrator.up();
  await orm.migrator.down({ to: 0 });
  expect((await q("SELECT to_regclass('public.outbox_events') relation"))[0]?.['relation']).toBeNull();
  expect((await q("SELECT count(*)::int n FROM pg_proc WHERE proname LIKE 'jwp_%'"))[0]?.['n']).toBe(0);
  const pending = (await orm.migrator.getPendingMigrations()).map(migration => migration.name);
  expect(pending.length).toBeGreaterThan(0);
  await orm.migrator.up();
  expect((await orm.migrator.getExecutedMigrations()).map(migration => migration.name)).toEqual(pending);
  expect(await orm.migrator.getPendingMigrations()).toHaveLength(0);
  open = new OpenWallet(new SqlWalletOpeningStore(orm));
},30000);
afterAll(async () => {
  if (orm) { try { await orm.migrator.down({ to: 0 }); } finally { await orm.close(true); } }
});

test('zero opening persists only a wallet at version 1',async () => {
  const before = await counts();
  const request = input('0.00');
  const result = await open.execute(request);
  expect(result).toEqual({ id: result.id, playerId: request.playerId, balance: request.initialBalance, version: 1 });
  expect(await q('SELECT balance::text,version::text FROM wallets WHERE id=?',[result.id])).toEqual([{balance:'0.00',version:'1'}]);
  const after = await counts();
  expect(after).toEqual({ ...before, wallets: Number(before?.['wallets']) + 1 });
});

test('positive opening persists exact wallet, real OPENING, historical snapshot, ledger and concrete events',async () => {
  const request = input('9007199254740993.01');
  const result = await open.execute(request,'opening-proof');
  expect(result.balance).toEqual(request.initialBalance);
  expect(result.version).toBe(1);
  const [wallet] = await q('SELECT balance::text,version::text,created_at FROM wallets WHERE id=?',[result.id]);
  expect(wallet?.['balance']).toBe(request.initialBalance.amount);
  expect(wallet?.['version']).toBe('1');
  const [wager] = await q('SELECT * FROM wager_transactions WHERE wallet_id=?',[result.id]);
  expect(wager?.['kind']).toBe('OPENING');
  expect(wager?.['status']).toBe('PROCESSED');
  expect(wager?.['provider_id']).toBe(openingProvider);
  expect(wager?.['amount']).toBe(request.initialBalance.amount);
  expect(new Date(wager?.['processed_at'] as string).toISOString()).toBe(new Date(wallet?.['created_at'] as string).toISOString());
  expect(wager?.['response_snapshot']).toEqual({ transactionId: wager?.['id'], status:'PROCESSED', balance:request.initialBalance, idempotentReplay:false });
  const [ledger] = await q('SELECT * FROM ledger_entries WHERE wallet_id=?',[result.id]);
  expect(ledger?.['transaction_id']).toBe(wager?.['id']);
  expect(ledger?.['wallet_version']).toBe('1');
  expect(ledger?.['direction']).toBe('CREDIT');
  expect(ledger?.['balance_before']).toBe('0.00');
  expect(ledger?.['balance_after']).toBe(request.initialBalance.amount);
  const rows = await q('SELECT * FROM outbox_events WHERE wallet_id=? ORDER BY event_type',[result.id]);
  expect(rows).toHaveLength(2);
  expect(rows.map(row=>row['event_type'])).toEqual(['WagerTransactionProcessed','WalletBalanceChanged']);
  for (const row of rows) {
    const event = row['envelope'] as Record<string, unknown>;
    expect(event['eventId']).toBe(row['event_id']);
    expect(event['eventType']).toBe(row['event_type']);
    expect(event['version']).toBe(1);
    expect(event['correlationId']).toBe('opening-proof');
    expect(event['causationId']).toBe(wager?.['id']);
    expect(event['occurredAt']).toBe(new Date(wallet?.['created_at'] as string).toISOString());
    expect(row['published_at']).toBeNull();
    expect(row['attempts']).toBe(0);
    expect(row['claim_token']).toBeNull();
    expect((event['data'] as Record<string,unknown>)['money']).toEqual(request.initialBalance);
  }
});

test('duplicate creation is a typed conflict and preserves the original zero wallet',async () => {
  const request = input('0.00');
  const first = await open.execute(request);
  const before = await counts();
  await expect(open.execute(input('80.00',request.playerId.toUpperCase()))).rejects.toMatchObject({ code:'WALLET_ALREADY_EXISTS' });
  expect(await counts()).toEqual(before);
  expect((await q('SELECT balance::text,version::text FROM wallets WHERE id=?',[first.id]))[0]).toEqual({balance:'0.00',version:'1'});
});

test('invalid DTO, currency, money and correlation inputs write nothing',async () => {
  const before = await counts();
  const valid = input();
  const invalid: unknown[] = [null, [], {}, { ...valid, playerId:'not-a-uuid' }, { ...valid, unexpected:true },
    { ...valid, initialBalance:null }, { ...valid, initialBalance:{ ...valid.initialBalance, currency:'USD' } },
    ...[100,-1,'-1.00','1.001','1e2',' 1.00','01.00','NaN','Infinity','1000000000000000000.00'].map(amount=>({ ...valid,initialBalance:{amount,currency:'BRL'} })),
    { ...valid, initialBalance:{amount:'1.00',currency:'BRL',extra:true} }];
  for (const request of invalid) await expect(open.execute(request)).rejects.toBeInstanceOf(WalletOpeningError);
  await expect(open.execute(valid,'')).rejects.toMatchObject({code:'INVALID_PAYLOAD'});
  expect(await counts()).toEqual(before);
});

async function injectedFailure(table: 'ledger_entries' | 'outbox_events', deferred = false) {
  const request = input();
  const before = await counts();
  const predicate = table === 'outbox_events' ? "NEW.event_type='WalletBalanceChanged'" : 'true';
  await q(`CREATE FUNCTION test_opening_fault() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN IF ${predicate} THEN RAISE EXCEPTION 'controlled opening failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$`);
  await q(deferred
    ? `CREATE CONSTRAINT TRIGGER test_opening_fault AFTER INSERT ON ${table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_opening_fault()`
    : `CREATE TRIGGER test_opening_fault BEFORE INSERT ON ${table} FOR EACH ROW EXECUTE FUNCTION test_opening_fault()`);
  try {
    await expect(open.execute(request)).rejects.toMatchObject({code:'23514'});
    expect(await counts()).toEqual(before);
    expect(await q('SELECT id FROM wallets WHERE player_id=?',[request.playerId])).toHaveLength(0);
  } finally {
    await q(`DROP TRIGGER test_opening_fault ON ${table}`);
    await q('DROP FUNCTION test_opening_fault()');
  }
}
test('failure inserting ledger rolls back wallet and OPENING',()=>injectedFailure('ledger_entries'));
test('failure inserting second event rolls back wallet, OPENING, ledger and first event',()=>injectedFailure('outbox_events'));
test('deferred commit failure rolls back all financial and outbox writes',()=>injectedFailure('outbox_events',true));

test('runtime publisher may claim an event but cannot change its original envelope',async () => {
  const wallet = await open.execute(input());
  const [row] = await q('SELECT event_id,envelope FROM outbox_events WHERE wallet_id=? LIMIT 1',[wallet.id]);
  await orm.em.fork().transactional(async em => {
    const run = (sql:string,args:unknown[]=[])=>em.getConnection().execute(sql,args,'all',em.getTransactionContext());
    await run('SET LOCAL ROLE jwp_runtime');
    await run('UPDATE outbox_events SET claim_token=?,lease_until=now()+interval \'30 seconds\',attempts=attempts+1 WHERE event_id=?',[randomUUID(),row?.['event_id']]);
  });
  await expect(orm.em.fork().transactional(async em => {
    const run = (sql:string,args:unknown[]=[])=>em.getConnection().execute(sql,args,'all',em.getTransactionContext());
    await run('SET LOCAL ROLE jwp_runtime');
    await run("UPDATE outbox_events SET envelope=jsonb_set(envelope,'{correlationId}','\"changed\"') WHERE event_id=?",[row?.['event_id']]);
  })).rejects.toMatchObject({code:'23514'});
  expect((await q('SELECT envelope FROM outbox_events WHERE event_id=?',[row?.['event_id']]))[0]?.['envelope']).toEqual(row?.['envelope']);
});

test('eight separate Bun processes race: exactly one wallet, OPENING, ledger and pair of events',async () => {
  const playerId = randomUUID();
  const workers = Array.from({length:8},()=>Bun.spawn([process.execPath,resolve(import.meta.dir,'open-worker.ts')], {
    env:{...process.env,JWP_OPEN_PLAYER:playerId},stdin:'pipe',stdout:'pipe',stderr:'pipe',
  }));
  try {
    const readers = workers.map(worker=>worker.stdout.getReader());
    const ready = await Promise.all(readers.map(async reader=>new TextDecoder().decode((await reader.read()).value)));
    expect(ready).toEqual(Array(8).fill('ready\n'));
    for (const worker of workers) { worker.stdin.write('go\n'); worker.stdin.end(); }
    const results = await Promise.all(readers.map(async (reader,index)=> {
      let output='';
      for (;;) { const chunk=await reader.read(); if(chunk.done) break; output+=new TextDecoder().decode(chunk.value); }
      expect(await workers[index]!.exited).toBe(0);
      return JSON.parse(output) as {ok:boolean;pid:number;code?:string};
    }));
    expect(new Set(results.map(result=>result.pid)).size).toBe(8);
    expect(results.filter(result=>result.ok)).toHaveLength(1);
    expect(results.filter(result=>!result.ok).map(result=>result.code)).toEqual(Array(7).fill('WALLET_ALREADY_EXISTS'));
    const wallets=await q('SELECT id,balance::text,version::text FROM wallets WHERE player_id=?',[playerId]);
    expect(wallets).toHaveLength(1);
    expect(wallets[0]?.['balance']).toBe('100.00');
    expect(wallets[0]?.['version']).toBe('1');
    for (const [table,n] of [['wager_transactions',1],['ledger_entries',1],['outbox_events',2]] as const) {
      expect((await q(`SELECT count(*)::int n FROM ${table} WHERE wallet_id=?`,[wallets[0]?.['id']]))[0]?.['n']).toBe(n);
    }
    console.log(JSON.stringify({proof:'wallet-opening-race',processes:results.map(result=>result.pid),successes:1,conflicts:7}));
  } finally { for (const worker of workers) if(worker.exitCode===null) worker.kill(); }
},30000);


test('bounded transient lock retries roll back without partial wallet or events', async () => {
  const before = await counts();
  const blocker = orm.em.fork();
  await blocker.begin();
  await blocker.getConnection().execute('LOCK TABLE wallets IN ACCESS EXCLUSIVE MODE', [], 'all', blocker.getTransactionContext());
  // Atividade do banco prova retries sem depender de tempo fixo.
  const limitedOrm = await openFinancialOrm(process.env['JWP_WALLET_DATABASE_URL']!);
  let attempts = 0;
  const fork = limitedOrm.em.fork.bind(limitedOrm.em);
  limitedOrm.em.fork = ((...args: Parameters<typeof limitedOrm.em.fork>) => {
    attempts++;
    return fork(...args);
  }) as typeof limitedOrm.em.fork;
  const constrained = new OpenWallet(new SqlWalletOpeningStore(limitedOrm, {
    lockTimeoutMs: 50, statementTimeoutMs: 400, transactionTimeoutMs: 500,
    budgetMs: 2000, maxAttempts: 3, retryBaseMs: 10,
  }));
  const request = input();
  const pending = constrained.execute(request).catch(error => error as {code?: string});
  let observed = false;
  for (let i = 0; i < 100; i++) {
    const rows = await q(`SELECT count(*)::int AS n FROM pg_stat_activity
      WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO wallets%'`);
    if (Number(rows[0]?.['n']) > 0) { observed = true; break; }
    await Bun.sleep(5);
  }
  try {
    expect(observed).toBe(true);
    expect(await pending).toMatchObject({code:'55P03'});
    expect(attempts).toBe(3);
  } finally {
    await blocker.rollback();
  }
  expect(await counts()).toEqual(before);
  expect(await q('SELECT id FROM wallets WHERE player_id=?',[request.playerId])).toHaveLength(0);
  const result = await constrained.execute(request);
  expect(result.version).toBe(1);
  expect(attempts).toBe(4);
  await limitedOrm.close(true);
}, 10000);
