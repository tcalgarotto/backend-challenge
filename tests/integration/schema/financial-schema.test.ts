import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { EntityManager } from '@mikro-orm/postgresql';
import { FinancialDecimalType, openFinancialOrm, WalletRecord } from '../../../src/persistence/financial-orm.js';

type Q = (sql: string, args?: unknown[]) => Promise<Record<string, unknown>[]>;
let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
const id = () => randomUUID();
const query = (em: EntityManager): Q => (sql, args = []) =>
  em.getConnection().execute(sql, args, 'all', em.getTransactionContext());
async function runtime<T>(action: (q: Q) => Promise<T>): Promise<T> {
  return orm.em.fork().transactional(async em => {
    const q = query(em);
    await q('SET LOCAL ROLE jwp_runtime');
    await q("SET LOCAL lock_timeout='3s'");
    await q("SET LOCAL statement_timeout='10s'");
    expect((await q('SELECT current_user AS role'))[0]?.['role']).toBe('jwp_runtime');
    return action(q);
  });
}
async function violation(action: (q: Q) => Promise<unknown>, code = '23514') {
  let caught: unknown;
  try { await runtime(action); } catch (e) { caught = e; }
  expect(caught).toBeDefined();
  expect((caught as { code?: string }).code).toBe(code);
}
async function wallet(q: Q, amount = '0.00', currency = 'BRL') {
  const w = id();
  await q('INSERT INTO wallets(id,player_id,currency,balance,version) VALUES (?,?,?, ?,1)', [w,w,currency,amount]);
  return w;
}
type WagerOptions = { kind?: string; status?: string; amount?: string; provider?: string; external?: string;
  key?: string; reference?: string; referenceExternal?: string; round?: string; currency?: string; player?: string; hash?: string };
async function wager(q: Q, w: string, o: WagerOptions = {}) {
  const t = id();
  const kind = o.kind ?? 'WIN';
  const status = o.status ?? 'PROCESSED';
  await q(`INSERT INTO wager_transactions(id,wallet_id,player_id,currency,provider_id,external_transaction_id,
    idempotency_key,payload_hash,round_id,game_id,kind,status,amount,reference_transaction_id,
    reference_external_transaction_id,failure_code,response_snapshot) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::jsonb)`,
  [t,w,o.player ?? w,o.currency ?? 'BRL',o.provider ?? 'provider',o.external ?? t,o.key ?? t,o.hash ?? 'a'.repeat(64),
    o.round ?? 'round','game',kind,status,o.amount ?? (kind === 'LOSS' ? '0.00' : '10.00'),o.reference ?? null,
    o.referenceExternal ?? null,['REJECTED','FAILED'].includes(status) ? 'TEST_REJECTION' : null,
    status === 'PENDING_REFERENCE' ? JSON.stringify({ pendingReferenceAt: new Date().toISOString(), referenceExpiresAt: new Date(Date.now()+900_000).toISOString() }) : null]);
  return t;
}
async function ledger(q: Q,w: string,t: string,version: string,before: string,after: string,
  direction = 'CREDIT',amount = '10.00',currency = 'BRL') {
  const l = id();
  await q(`INSERT INTO ledger_entries(id,wallet_id,transaction_id,currency,wallet_version,direction,amount,balance_before,balance_after)
    VALUES (?,?,?,?,?,?,?,?,?)`,[l,w,t,currency,version,direction,amount,before,after]);
  return l;
}
async function opening(q: Q,amount = '100.00') {
  const w = await wallet(q,amount);
  const t = await wager(q,w,{kind:'OPENING',amount,external:w});
  await ledger(q,w,t,'1','0.00',amount,'CREDIT',amount);
  return {w,t};
}
async function movement(q: Q,w: string,version: string,before: string,after: string,o: WagerOptions = {},direction = 'CREDIT') {
  const t = await wager(q,w,o);
  await q('UPDATE wallets SET balance=?,version=? WHERE id=?',[after,version,w]);
  await ledger(q,w,t,version,before,after,direction,o.amount ?? '10.00');
  return t;
}

beforeAll(async () => {
  const url = process.env['JWP_SCHEMA_DATABASE_URL'];
  if (!url) throw new Error('JWP_SCHEMA_DATABASE_URL is required; use an empty disposable database and migration owner credentials.');
  orm = await openFinancialOrm(url);
  const q = query(orm.em.fork());
  expect((await q("SELECT to_regclass('public.wallets') AS relation"))[0]?.['relation']).toBeNull();
  await orm.migrator.up();

  const login = `jwp_schema_login_${randomUUID().replaceAll('-', '')}`;
  await q(`CREATE ROLE ${login} LOGIN; GRANT jwp_runtime TO ${login}`);
  try {
    await orm.migrator.down({to:0});
    expect((await q("SELECT to_regclass('public.ledger_entries') AS relation"))[0]?.['relation']).toBeNull();
    expect((await q("SELECT count(*)::int AS n FROM pg_proc WHERE proname LIKE 'jwp_%'"))[0]?.['n']).toBe(0);

    expect((await q("SELECT count(*)::int AS n FROM pg_roles WHERE rolname='jwp_runtime'"))[0]?.['n']).toBe(1);
    expect((await q(`SELECT count(*)::int AS n FROM pg_namespace, aclexplode(nspacl) acl
      WHERE nspname='public' AND acl.grantee=(SELECT oid FROM pg_roles WHERE rolname='jwp_runtime')`))[0]?.['n']).toBe(0);

    await q('ALTER ROLE jwp_runtime LOGIN');
    let rejected: unknown;
    try { await orm.migrator.up(); } catch (e) { rejected = e; }
    expect(String(rejected)).toContain('existing role jwp_runtime must be');
    expect(await orm.migrator.getExecutedMigrations()).toHaveLength(0);
    await q('ALTER ROLE jwp_runtime NOLOGIN');
    const pending = (await orm.migrator.getPendingMigrations()).map(m => m.name);
    expect(pending).toContain('Migration202610120008');
    await orm.migrator.up();
    expect((await orm.migrator.getExecutedMigrations()).map(m => m.name)).toEqual(pending);
    expect(await orm.migrator.getPendingMigrations()).toHaveLength(0);
    expect((await q('SELECT pg_has_role(?, ?, ?) AS member', [login, 'jwp_runtime', 'MEMBER']))[0]?.['member']).toBe(true);
  } finally {
    await q(`DROP ROLE IF EXISTS ${login}`);
  }
},30000);
afterAll(async () => {
  if (orm) {
    try { await orm.migrator.down({to:0}); } finally { await orm.close(true); }
  }
});

test('up/down/up restores functions, deferred triggers, unique reversal index and non-owner runtime privileges',async () => {
  const q=query(orm.em.fork());
  expect((await q("SELECT count(*)::int AS n FROM pg_trigger WHERE tgname IN ('wallet_consistency','wager_consistency','ledger_consistency') AND tgdeferrable AND tginitdeferred"))[0]?.['n']).toBe(3);
  expect((await q("SELECT indisunique FROM pg_index WHERE indexrelid='wager_one_reversal'::regclass"))[0]?.['indisunique']).toBe(true);
  expect((await q("SELECT pg_get_indexdef('wager_one_reversal'::regclass) AS def"))[0]?.['def']).toContain('(reference_transaction_id) WHERE');
  expect((await q("SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolcanlogin AS privileged FROM pg_roles WHERE rolname='jwp_runtime'"))[0]?.['privileged']).toBe(false);
  expect((await q("SELECT count(*)::int AS n FROM pg_class WHERE relowner=(SELECT oid FROM pg_roles WHERE rolname='jwp_runtime')"))[0]?.['n']).toBe(0);
  await runtime(async s => {
    for(const privilege of ['UPDATE','DELETE','TRUNCATE']) {
      expect((await s('SELECT has_table_privilege(current_user, ?, ?) AS allowed',['ledger_entries',privilege]))[0]?.['allowed']).toBe(false);
    }
  });
});

test('valid zero and positive opening, LOSS, BET/WIN, REFUND and rollback of REFUND',async () => {
  await runtime(async q => {
    const zero=await wallet(q);
    await wager(q,zero,{kind:'LOSS'});
    await movement(q,zero,'2','0.00','10.00');
    const {w}=await opening(q);
    const bet=await movement(q,w,'2','100.00','75.00',{kind:'BET',amount:'25.00',external:'bet-'+w},'DEBIT');
    const refund=await movement(q,w,'3','75.00','100.00',{kind:'REFUND',amount:'25.00',reference:bet,referenceExternal:'bet-'+w,external:'refund-'+w});
    await movement(q,w,'4','100.00','75.00',{kind:'ROLLBACK',amount:'25.00',reference:refund,referenceExternal:'refund-'+w},'DEBIT');
    expect((await q('SELECT balance::text AS balance,version::text AS version FROM wallets WHERE id=?',[w]))[0]).toEqual({balance:'75.00',version:'4'});
  });
});

test('wallet player/currency uniqueness and provider-scoped durable identities',async () => {
  await violation(async q=> {const w=await wallet(q);await q('INSERT INTO wallets(id,player_id,currency,balance,version) VALUES (?,?,?,0,1)',[id(),w,'BRL']);},'23505');
  for(const field of ['key','external'] as const) {
    await violation(async q=>{const w=await wallet(q); const o={kind:'LOSS',[field]:id()}; await wager(q,w,o);await wager(q,w,o);},'23505');
  }
  await runtime(async q=>{const w=await wallet(q);for(const provider of ['one','two']) await wager(q,w,{kind:'LOSS',provider,key:'shared',external:'shared'});});
});

test('wallet range, finitude, version, currency and hash checks reject raw SQL',async () => {
  for(const amount of ['-0.01','NaN','Infinity','-Infinity','1000000000000000000.00']) {
    await violation(q=>wallet(q,amount),amount.includes('Infinity') || amount.startsWith('100000') ? '22003':'23514');
  }
  await violation(q=>wallet(q,'0.00','INVALID'));
  await violation(async q=>{const w=await wallet(q);await q('UPDATE wallets SET version=0 WHERE id=?',[w]);});
  await violation(async q=>{const w=await wallet(q);await wager(q,w,{kind:'LOSS',hash:'invalid'});});
  await violation(async q=>{const w=await wallet(q);await q('UPDATE wallets SET player_id=? WHERE id=?',[id(),w]);});
});

test('wager finite amounts, kind/status policy and wallet context FK',async () => {
  for(const o of [{amount:'0.00'},{amount:'-1.00'},{amount:'NaN'},{kind:'LOSS',amount:'1.00'},{kind:'INVALID'},{status:'INVALID'}]) {
    await violation(async q=>{const w=await wallet(q);await wager(q,w,o);});
  }
  for(const o of [{player:id()},{currency:'USD'}]) await violation(async q=>{const w=await wallet(q);await wager(q,w,{...o,kind:'LOSS'});},'23503');
});

test('SQL balance/version change without ledger and monetary PROCESSED without ledger fail at commit',async () => {
  await violation(async q=>{const w=await wallet(q);await q('UPDATE wallets SET balance=1 WHERE id=?',[w]);});
  await violation(async q=>{const w=await wallet(q);await q('UPDATE wallets SET version=2 WHERE id=?',[w]);});
  await violation(async q=>{const w=await wallet(q);await wager(q,w);});
  await violation(async q=>{const w=await wallet(q,'10.00');await wager(q,w,{kind:'OPENING'});});
  await violation(async q=>{const w=await wallet(q);const t=await wager(q,w);await ledger(q,w,t,'2','0.00','10.00');});
});

test('ledger duplicate transaction and duplicate wallet version are rejected',async () => {
  await violation(async q=>{const {w,t}=await opening(q);await ledger(q,w,t,'2','100.00','200.00','CREDIT','100.00');},'23505');
  await violation(async q=>{const {w}=await opening(q);const a=await wager(q,w);const b=await wager(q,w);await ledger(q,w,a,'2','100.00','110.00');await ledger(q,w,b,'2','110.00','120.00');},'23505');
});

test('ledger CHECKs reject bad direction, zero/negative/NaN values, negative balances and wrong arithmetic',async () => {
  for(const [direction,amount,before,after] of [
    ['INVALID','10.00','0.00','10.00'], ['CREDIT','0.00','0.00','0.00'], ['CREDIT','-1.00','0.00','0.00'],
    ['CREDIT','NaN','0.00','NaN'], ['CREDIT','10.00','-1.00','9.00'], ['DEBIT','10.00','0.00','-10.00'],
    ['CREDIT','10.00','NaN','NaN'], ['CREDIT','10.00','0.00','11.00'], ['DEBIT','10.00','20.00','11.00'],
  ]) await violation(async q=>{const w=await wallet(q);const t=await wager(q,w);await ledger(q,w,t,'2',before!,after!,direction!,amount!);});
});

test('ledger FK requires a real wager, matching wallet and currency including OPENING',async () => {
  await violation(async q=>{const w=await wallet(q);await ledger(q,w,id(),'2','0.00','10.00');},'23503');
  await violation(async q=>{const w=await wallet(q);const other=await wallet(q);const t=await wager(q,other);await ledger(q,w,t,'2','0.00','10.00');},'23503');
  await violation(async q=>{const w=await wallet(q);const t=await wager(q,w);await ledger(q,w,t,'2','0.00','10.00','CREDIT','10.00','USD');},'23503');
  await runtime(async q=>{const {w,t}=await opening(q);expect((await q('SELECT t.kind FROM ledger_entries l JOIN wager_transactions t ON t.id=l.transaction_id WHERE l.wallet_id=? AND t.id=?',[w,t]))[0]?.['kind']).toBe('OPENING');});
});

test('LOSS and every non-PROCESSED status reject ledger; wager/ledger amount/direction must agree',async () => {
  for(const status of ['PENDING','PENDING_REFERENCE','REJECTED','FAILED']) {
    await violation(async q=>{const w=await wallet(q);const t=await wager(q,w,{status});await q('UPDATE wallets SET balance=10,version=2 WHERE id=?',[w]);await ledger(q,w,t,'2','0.00','10.00');});
  }
  await violation(async q=>{const w=await wallet(q);const t=await wager(q,w,{kind:'LOSS'});await ledger(q,w,t,'2','0.00','10.00');});
  await violation(async q=>{const w=await wallet(q);const t=await wager(q,w,{amount:'9.00'});await q('UPDATE wallets SET balance=10,version=2 WHERE id=?',[w]);await ledger(q,w,t,'2','0.00','10.00');});
  await violation(async q=>{const {w}=await opening(q);await movement(q,w,'2','100.00','110.00',{kind:'BET'});});
});

test('gaps, broken chain, wrong final balance/version and OPENING at wrong version fail',async () => {
  await violation(async q=>{const {w}=await opening(q);await movement(q,w,'3','100.00','110.00');});
  await violation(async q=>{const {w}=await opening(q);await movement(q,w,'2','99.00','109.00');});
  await violation(async q=>{const {w}=await opening(q);await movement(q,w,'2','100.00','110.00');await q('UPDATE wallets SET balance=120 WHERE id=?',[w]);});
  await violation(async q=>{const w=await wallet(q);await movement(q,w,'2','0.00','10.00',{kind:'OPENING'});});
  await violation(async q=>{const w=await wallet(q,'10.00');const t=await wager(q,w);await ledger(q,w,t,'1','0.00','10.00');});
});

test('only one processed reversal per reference, whatever its kind; WIN and rejected reversals do not reserve it',async () => {
  await violation(async q=>{
    const {w}=await opening(q);const bet=await movement(q,w,'2','100.00','90.00',{kind:'BET',external:'bet-'+w},'DEBIT');
    const o={kind:'REFUND',reference:bet,referenceExternal:'bet-'+w};
    await movement(q,w,'3','90.00','100.00',o);await movement(q,w,'4','100.00','110.00',o);
  },'23505');

  await violation(async q=>{
    const {w}=await opening(q);const bet=await movement(q,w,'2','100.00','90.00',{kind:'BET',external:'bet-'+w},'DEBIT');
    await movement(q,w,'3','90.00','100.00',{kind:'REFUND',reference:bet,referenceExternal:'bet-'+w});
    await movement(q,w,'4','100.00','110.00',{kind:'ROLLBACK',reference:bet,referenceExternal:'bet-'+w});
  },'23505');
  await runtime(async q=>{
    const {w}=await opening(q);const bet=await movement(q,w,'2','100.00','90.00',{kind:'BET',external:'bet-'+w},'DEBIT');
    const o={kind:'REFUND',reference:bet,referenceExternal:'bet-'+w};
    await wager(q,w,{...o,status:'REJECTED'});
    await movement(q,w,'3','90.00','100.00',{kind:'WIN',reference:bet,referenceExternal:'bet-'+w});
    await movement(q,w,'4','100.00','110.00',o);
  });
});

test('processed references enforce provider/round/type/full amount and inverse direction',async () => {
  for(const change of [{provider:'other'},{round:'other'},{amount:'9.00'},{referenceExternal:'wrong'}]) {
    await violation(async q=>{const {w}=await opening(q);const bet=await movement(q,w,'2','100.00','90.00',{kind:'BET',external:'bet-'+w},'DEBIT');await movement(q,w,'3','90.00',change.amount?'99.00':'100.00',{kind:'REFUND',reference:bet,referenceExternal:'bet-'+w,...change});});
  }
  await violation(async q=>{const {w,t}=await opening(q);await movement(q,w,'2','100.00','110.00',{kind:'ROLLBACK',reference:t,referenceExternal:w});});
  await violation(async q=>{const {w}=await opening(q);const win=await movement(q,w,'2','100.00','110.00',{external:'win-'+w});await movement(q,w,'3','110.00','120.00',{kind:'ROLLBACK',reference:win,referenceExternal:'win-'+w});});
  await violation(async q=>{const w=await wallet(q);await movement(q,w,'2','0.00','10.00',{referenceExternal:'missing'});});
});

test('runtime UPDATE/DELETE/TRUNCATE are denied; owner triggers independently forbid ledger mutation',async () => {
  let l='';await runtime(async q=>{const {w}=await opening(q);l=String((await q('SELECT id FROM ledger_entries WHERE wallet_id=?',[w]))[0]?.['id']);});
  for(const sql of ['UPDATE ledger_entries SET amount=amount WHERE id=?','DELETE FROM ledger_entries WHERE id=?','TRUNCATE ledger_entries']) {
    await violation(q=>q(sql,sql.includes('?')?[l]:[]),'42501');
    await expect(orm.em.fork().transactional(em=>query(em)(sql,sql.includes('?')?[l]:[]))).rejects.toThrow('ledger is append-only');
  }
});

test('wager terminal results and business identity are immutable',async () => {
  let t='';await runtime(async q=>{const {t:tx}=await opening(q);t=tx;});
  for(const sql of ["UPDATE wager_transactions SET amount=1 WHERE id=?","UPDATE wager_transactions SET status='FAILED',failure_code='ERROR' WHERE id=?","UPDATE wager_transactions SET payload_hash=repeat('b',64) WHERE id=?"]) await violation(q=>q(sql,[t]));
});

test('exact ORM string mapping above 2^53, cent dirty checking and input scale/range rejection',async () => {
  const exact='9007199254740993.01';let w='';
  await runtime(async q=>{w=(await opening(q,exact)).w;});
  const em=orm.em.fork();const row=await em.findOneOrFail(WalletRecord,{id:w});
  expect(typeof row.balance).toBe('string');expect(row.balance).toBe(exact);
  const type=new FinancialDecimalType();
  expect(type.compareValues(exact,'9007199254740993.02')).toBe(false);
  expect(type.convertToDatabaseValue(exact)).toBe(exact);
  for(const bad of ['1.001','NaN','Infinity','1000000000000000000.00','-0.01',' 1.00','1e1']) expect(()=>type.convertToDatabaseValue(bad)).toThrow();
  expect(()=>type.convertToDatabaseValue(1 as unknown as string)).toThrow();
  await runtime(async q=>{await movement(q,w,'2',exact,'9007199254740993.02',{amount:'0.01'});});
  expect((await orm.em.fork().findOneOrFail(WalletRecord,{id:w})).balance).toBe('9007199254740993.02');
});

test('two real runtime connections racing debits cannot produce a negative balance or duplicate effect',async () => {
  let w='';await runtime(async q=>{w=(await opening(q)).w;});
  const a=orm.em.fork(),b=orm.em.fork();
  await a.begin();await b.begin();
  const qa=query(a),qb=query(b),observer=query(orm.em.fork());
  let waiting: Promise<{code?:string}|null> | undefined;
  try {
    await qa('SET LOCAL ROLE jwp_runtime');await qb('SET LOCAL ROLE jwp_runtime');
    await qb("SET LOCAL lock_timeout='5s'");
    const pa=(await qa('SELECT pg_backend_pid() AS pid'))[0]?.['pid'];
    const pb=(await qb('SELECT pg_backend_pid() AS pid'))[0]?.['pid'];
    expect(pa).not.toBe(pb);
    await movement(qa,w,'2','100.00','20.00',{kind:'BET',amount:'80.00'},'DEBIT');
    waiting=qb('UPDATE wallets SET balance=balance-80,version=version+1 WHERE id=?',[w])
      .then(()=>null,e=>e as {code?:string});
    let blocked=false;
    for(let i=0;i<150;i++) {
      if((await observer('SELECT ? = ANY(pg_blocking_pids(?)) AS blocked',[pa,pb]))[0]?.['blocked']) {blocked=true;break;}
      await Bun.sleep(10);
    }
    expect(blocked).toBe(true);
    await a.commit();
    expect((await waiting)?.code).toBe('23514');
    await b.rollback();
    const state=(await observer(`SELECT balance::text AS balance,version::text AS version,
      (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=wallets.id) AS entries
      FROM wallets WHERE id=?`,[w]))[0];
    expect(state).toEqual({balance:'20.00',version:'2',entries:2});
    console.log(JSON.stringify({scenario:'schema-debit-race',connectionPids:[pa,pb],serverObservedBlocking:blocked,...state}));
  } finally {
    if(a.isInTransaction()) await a.rollback();
    if(waiting) await waiting;
    if(b.isInTransaction()) await b.rollback();
  }
},15000);

test('transactional ORM flush persists a one-cent change above 2^53 and matching wager/ledger',async () => {
  let w='';await runtime(async q=>{w=(await opening(q,'9007199254740993.01')).w;});
  await orm.em.fork().transactional(async em=>{
    const q=query(em);await q('SET LOCAL ROLE jwp_runtime');
    const row=await em.findOneOrFail(WalletRecord,{id:w});
    row.balance='9007199254740993.02';row.version=2n;
    await em.flush();
    const t=await wager(q,w,{amount:'0.01'});
    await ledger(q,w,t,'2','9007199254740993.01','9007199254740993.02','CREDIT','0.01');
  });
  expect((await orm.em.fork().findOneOrFail(WalletRecord,{id:w})).balance).toBe('9007199254740993.02');
});
