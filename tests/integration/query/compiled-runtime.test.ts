import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let child: ReturnType<typeof Bun.spawn>, base: string, runtimeUrl: string, migrated = false;
const logs: string[] = [];
const role = `jwp_audit_${randomUUID().replaceAll('-', '')}`;
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<any[]>(sql, args);
async function start() {
  child = Bun.spawn(['bun', 'dist/main.js'], { stdout: 'pipe', stderr: 'pipe',
    env: { ...Bun.env, JWP_DATABASE_URL: runtimeUrl, PORT: '0', HOST: '127.0.0.1' } });
  let ready!: (port: number) => void, fail!: (error: Error) => void;
  const address = new Promise<number>((resolve, reject) => { ready = resolve; fail = reject; });
  void (async () => {
    let buffer = '';
    for await (const chunk of child.stdout as ReadableStream<Uint8Array>) {
      buffer += new TextDecoder().decode(chunk);
      while (buffer.includes('\n')) {
        const end = buffer.indexOf('\n'), line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
        logs.push(line);
        try { const item = JSON.parse(line); if (item.type === 'listening') ready(item.port); } catch {}
      }
    }
    fail(new Error('Compiled server exited before listening'));
  })();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { base = `http://127.0.0.1:${await Promise.race([address,new Promise<never>((_,reject)=>{
    timer=setTimeout(()=>reject(new Error('Compiled bootstrap timed out')),8000);
  })])}`; }
  finally { if (timer) clearTimeout(timer); }
}
async function stop() { if (child) { child.kill('SIGTERM'); await child.exited; } }
async function call(method: string, path: string, body?: unknown, headers: Record<string,string> = {}) {
  const response = await fetch(base + path, { method, headers: { 'Content-Type': 'application/json', ...headers },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
  return { status: response.status, body: await response.json() as any };
}
async function open(amount = '100.00') {
  const result = await call('POST', '/wallets', { playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' } });
  expect(result.status).toBe(201); return result.body as { id: string; playerId: string };
}
function command(w: {id:string;playerId:string}, kind='BET', amount='25.00') {
  return { providerId: 'compiled-provider', externalTransactionId: randomUUID(), playerId: w.playerId, walletId: w.id,
    roundId: 'round', gameId: 'game', kind, money: { amount, currency: 'BRL' } };
}
const submit = (body: ReturnType<typeof command>) => call('POST','/wagering/transactions',body,{'Idempotency-Key':body.externalTransactionId});

beforeAll(async () => {
  const ownerUrl = Bun.env['JWP_QUERY_DATABASE_URL'];
  if (!ownerUrl) throw new Error('JWP_QUERY_DATABASE_URL required: empty disposable PostgreSQL; integration never skips.');
  orm = await openFinancialOrm(ownerUrl);
  expect((await q("SELECT to_regclass('wallets') relation"))[0].relation).toBeNull();

  const migration = Bun.spawn(['bun','run','db:migrate'], { env: { ...Bun.env, JWP_MIGRATION_DATABASE_URL: ownerUrl }, stdout:'pipe',stderr:'pipe' });
  const [status, output, errors] = await Promise.all([migration.exited,new Response(migration.stdout).text(),new Response(migration.stderr).text()]);
  expect(status).toBe(0); migrated = true;
  console.log(JSON.stringify({proof:'migration-command',status,output,errors}));
  await q(`CREATE ROLE ${role} LOGIN PASSWORD 'query_test_only'`);
  await q(`GRANT jwp_runtime TO ${role}`);
  const url = new URL(ownerUrl); url.username = role; url.password = 'query_test_only'; runtimeUrl = url.toString();
  const [permissions] = await q(`SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=?`,[role]);
  expect(permissions).toEqual({rolsuper:false,rolcreatedb:false,rolcreaterole:false});
  await start();
},30000);
afterAll(async () => {
  await stop();
  if (orm) {
    try { if (migrated) await orm.migrator.down({to:0}); await q(`DROP ROLE IF EXISTS ${role}`); }
    finally { await orm.close(true); }
  }
});

test('official compiled process exposes all financial routes under runtime credentials and survives restart',async()=>{
  expect(await call('GET','/health/live')).toEqual({status:200,body:{status:'ok'}});
  const w=await open(), bet=command(w), result=await submit(bet);
  expect(result.status).toBe(200);
  expect(result.body.balance).toEqual({amount:'75.00',currency:'BRL'});
  const wallet=await call('GET',`/wallets/${w.id}`);
  expect(wallet.body).toMatchObject({balance:{amount:'75.00'},version:2});
  const first=await call('GET',`/wallets/${w.id}/ledger?limit=1`);
  expect(first.status).toBe(200); expect(first.body.items).toHaveLength(1); expect(typeof first.body.nextCursor).toBe('string');
  const opening=await call('GET',`/wagering/transactions/${first.body.items[0].transactionId}`);
  expect(opening.body).toMatchObject({kind:'OPENING',resultBalance:{amount:'100.00',currency:'BRL'}});
  const internal=await call('GET',`/wagering/transactions/${result.body.transactionId}`);
  expect(await call('GET',`/providers/compiled-provider/wagering/transactions/${bet.externalTransactionId}`)).toEqual(internal);
  expect(internal.body).toMatchObject({status:'PROCESSED',resultBalance:{amount:'75.00'}});
  for(const hidden of ['idempotencyKey','payloadHash','idempotentReplay'])expect(internal.body).not.toHaveProperty(hidden);
  await stop(); await start();
  expect(await call('GET',`/wallets/${w.id}`)).toEqual(wallet);
  expect((await submit(bet)).body).toMatchObject({idempotentReplay:true,balance:{amount:'75.00'}});
  const rest=await call('GET',`/wallets/${w.id}/ledger?cursor=${first.body.nextCursor}`);
  expect(rest.body.items.map((x:any)=>x.walletVersion)).toEqual([2]); expect(rest.body.nextCursor).toBeNull();
  expect((await call('POST',`/wallets/${w.id}/reconciliation`)).body).toMatchObject({consistent:true,checkedEntries:2,
    storedBalance:{amount:'75.00'},calculatedBalance:{amount:'75.00'},difference:{amount:'0.00'}});
  console.log(JSON.stringify({proof:'compiled-runtime-all-routes-restart',wallet,opening:opening.body,transaction:internal.body}));
},30000);

test('compiled HTTP input errors are typed, including malformed JSON, without durable effects',async()=>{
  const w=await open('10.00');
  const state=()=>q('SELECT balance::text balance,version::text version,(SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger FROM wallets w WHERE id=?',[w.id]);
  const before=await state();
  const missing=await call('POST','/wagering/transactions',command(w));
  expect(missing).toMatchObject({status:400,body:{error:{code:'IDEMPOTENCY_KEY_REQUIRED'}}});
  const invalid=await call('GET',`/wallets/${w.id}/ledger?limit=1&limit=2`);
  expect(invalid).toMatchObject({status:400,body:{error:{code:'INVALID_PAYLOAD'}}});
  expect(await call('GET',`/wallets/${w.id}/ledger?cursor=garbage`)).toMatchObject({status:400,body:{error:{code:'INVALID_CURSOR'}}});
  expect(await call('GET',`/wallets/${randomUUID()}`)).toMatchObject({status:404,body:{error:{code:'WALLET_NOT_FOUND'}}});
  expect(await call('GET',`/wagering/transactions/${randomUUID()}`)).toMatchObject({status:404,body:{error:{code:'TRANSACTION_NOT_FOUND'}}});
  const jsonErrors=[];
  for(const path of ['/wallets','/wagering/transactions',`/wallets/${w.id}/reconciliation`]) {
    const response=await fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json'},body:'{"broken":'});
    const body=await response.json() as any;
    expect(response.status).toBe(400);expect(body.error).toEqual({code:'INVALID_PAYLOAD',message:'INVALID_PAYLOAD',retryable:false});
    expect(body.correlationId).toMatch(/^[0-9a-f-]{36}$/);jsonErrors.push({path,status:response.status,body});
  }
  expect(await call('POST',`/wallets/${w.id}/reconciliation`,{repair:true})).toMatchObject({status:400,body:{error:{code:'INVALID_PAYLOAD'}}});
  expect(await state()).toEqual(before);
  console.log(JSON.stringify({proof:'compiled-runtime-invalid-json',jsonErrors}));
});

test('compiled reconciliation flags controlled divergence in response and log without repairing state',async()=>{
  const w=await open('50.00');
  const em=orm.em.fork();await em.begin();
  try {
    await em.getConnection().execute('SET LOCAL session_replication_role=replica',[],'all',em.getTransactionContext());
    await em.getConnection().execute('UPDATE wallets SET balance=? WHERE id=?',['20.00',w.id],'all',em.getTransactionContext());
    await em.commit();
  }catch(error){await em.rollback();throw error;}
  const result=await call('POST',`/wallets/${w.id}/reconciliation`);
  expect(result).toMatchObject({status:200,body:{consistent:false,difference:{amount:'-30.00'},checkedEntries:1}});
  expect((await q('SELECT balance::text balance,version::text version FROM wallets WHERE id=?',[w.id]))[0]).toEqual({balance:'20.00',version:'1'});

  const divergence=()=>logs.map(x=>{try{return JSON.parse(x)}catch{return {}}}).find(x=>x.type==='wallet.reconciliation.divergent'&&x.walletId===w.id);
  for(let i=0;i<100&&!divergence();i++)await Bun.sleep(10);
  const event=divergence();
  expect(event).toMatchObject({level:'error',type:'wallet.reconciliation.divergent',repaired:false,checkedEntries:1});
  console.log(JSON.stringify({proof:'compiled-runtime-divergence',result,event}));
});

test('official compiled process returns typed 503 on real PostgreSQL loss and recovers every query route',async()=>{
  const container=Bun.env['JWP_QUERY_POSTGRES_CONTAINER'];
  if(!container?.startsWith('jwp-query-'))throw new Error('Dedicated JWP_QUERY_POSTGRES_CONTAINER (jwp-query-*) required; no infrastructure skip.');
  const w=await open('30.00'), ledger=await call('GET',`/wallets/${w.id}/ledger`);
  const tx=ledger.body.items[0].transactionId;
  const routes:[string,string,unknown?][]=[
    ['GET',`/wallets/${w.id}`],['GET',`/wallets/${w.id}/ledger`],['GET',`/wagering/transactions/${tx}`],
    ['GET',`/providers/internal:wallet-opening/wagering/transactions/opening:${w.id}`],['POST',`/wallets/${w.id}/reconciliation`],
    ['POST','/wallets',{playerId:randomUUID(),initialBalance:{amount:'1.00',currency:'BRL'}}],
  ];
  // Aquece pelo aplicativo e encerra somente o banco descartável deste teste.
  await Promise.all(Array(4).fill(0).map(()=>call('GET',`/wallets/${w.id}`)));
  const observations=[];
  try {
    expect(Bun.spawnSync(['docker','kill','--signal=KILL',container]).exitCode).toBe(0);
    for(const [method,path,body]of routes){
      const result=await call(method,path,body);observations.push({method,path,...result});
      expect(result).toMatchObject({status:503,body:{error:{code:'INFRASTRUCTURE_UNAVAILABLE',retryable:true}}});
    }
  }finally{
    expect(Bun.spawnSync(['docker','start',container]).exitCode).toBe(0);
    let ready=false;
    for(let i=0;i<100;i++){
      if(Bun.spawnSync(['docker','exec',container,'pg_isready','-U','jwp']).exitCode===0){ready=true;break;}
      await Bun.sleep(100);
    }
    expect(ready).toBe(true);
  }
  for(const [method,path,body]of routes)expect((await call(method,path,body)).status).toBe(path==='/wallets'?201:200);
  expect((await call('POST',`/wallets/${w.id}/reconciliation`)).body).toMatchObject({consistent:true,checkedEntries:1,storedBalance:{amount:'30.00'}});
  console.log(JSON.stringify({proof:'compiled-runtime-postgres-SIGKILL',observations}));
},60000);
