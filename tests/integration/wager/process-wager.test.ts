import { WalletRecord } from '../../../src/persistence/financial-orm.js';
import { LockMode } from '@mikro-orm/core';
import 'reflect-metadata';
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { Wallet } from '../../../src/wallet/domain/wallet.js';
import { Money } from '../../../src/shared/domain/money.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { ProcessWager, parseWager } from '../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../src/wagering/infrastructure/sql-process-wager-store.js';
import { configureReversalPolicy, reversalPolicy } from '../../../src/wagering/domain/reversal-policy.js';
import { downToZeroAfterReversals } from '../support/reversal-policy-teardown.js';
import { WageringModule } from '../../../src/wagering/http/wagering.module.js';

let orm:Awaited<ReturnType<typeof openFinancialOrm>>,open:OpenWallet,processWager:ProcessWager,app:INestApplication;
let base:string;
const q=(sql:string,args:unknown[]=[])=>orm.em.fork().getConnection().execute<Record<string,unknown>[]>(sql,args);
async function wallet(amount='100.00') { return open.execute({playerId:randomUUID(),initialBalance:{amount,currency:'BRL'}}); }
function command(w:{id:string;playerId:string},kind='BET',amount='25.00',reference?:string) {
  const id:string=randomUUID();
  return {key:'key:'+id,body:{providerId:'provider-a',externalTransactionId:id,walletId:w.id,playerId:w.playerId,
    roundId:'round-a',gameId:'game-a',kind,money:{amount,currency:'BRL'},...(reference===undefined ? {} : {referenceExternalTransactionId:reference})}};
}
const run=(c:ReturnType<typeof command>)=>processWager.execute(c.body,c.key);
async function state(walletId:string) {
  const [row]=await q(`SELECT balance::text,version::text,
    (SELECT count(*)::int FROM wager_transactions WHERE wallet_id=w.id AND kind<>'OPENING') wagers,
    (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger,
    (SELECT count(*)::int FROM outbox_events WHERE wallet_id=w.id) outbox,
    (SELECT coalesce(sum(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END),0)::numeric(20,2)::text
      FROM ledger_entries WHERE wallet_id=w.id) calculated FROM wallets w WHERE id=?`,[walletId]);
  expect(row!['balance']).toBe(row!['calculated']);
  return row!;
}
beforeAll(async()=>{
  const url=process.env['JWP_WAGER_DATABASE_URL'];
  if(!url) throw new Error('JWP_WAGER_DATABASE_URL required: empty disposable PostgreSQL. Integration never skips infrastructure.');
  orm=await openFinancialOrm(url);
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]!['relation']).toBeNull();
  await orm.migrator.up();
  open=new OpenWallet(new SqlWalletOpeningStore(orm));
  processWager=new ProcessWager(new SqlProcessWagerStore(orm));
  app=await NestFactory.create(WageringModule.register(orm),{logger:false});
  await app.listen(0,'127.0.0.1');
  base=await app.getUrl();
},30000);
afterAll(async()=>{
  if(app) await app.close();
  if(orm) { try {
    // Há REFUND e ROLLBACK aplicados à mesma BET (per_type): o down precisa recusar antes do descarte.
    await expect(orm.migrator.down({to:'Migration202610130009'})).rejects.toThrow('REVERSAL_POLICY_DOWN_BLOCKED');
    await downToZeroAfterReversals(orm,true);
  } finally { await orm.close(true); } }
});

test('BET, WIN, LOSS: exact amounts, historical snapshots and zero-effect LOSS',async()=>{
  const w=await wallet(),bet=command(w),win=command(w,'WIN','20.00'),loss=command(w,'LOSS','0.00');
  const result=await run(bet);
  expect(result).toMatchObject({httpStatus:200,body:{status:'PROCESSED',balance:{amount:'75.00',currency:'BRL'},idempotentReplay:false}});
  expect(await run(win)).toMatchObject({body:{balance:{amount:'95.00'}}});
  expect(await run(loss)).toMatchObject({body:{balance:{amount:'95.00'},status:'PROCESSED'}});
  expect(await run(bet)).toEqual({httpStatus:200,body:{...result.body,idempotentReplay:true}});
  expect(await state(w.id)).toMatchObject({balance:'95.00',version:'3',wagers:3,ledger:3,outbox:7});
  const events=await q('SELECT event_type,envelope FROM outbox_events WHERE transaction_id=?',[result.body.transactionId]);
  expect(events.map(e=>e['event_type']).sort()).toEqual(['WagerTransactionProcessed','WalletBalanceChanged']);
  expect(events[0]!['envelope']).toMatchObject({version:1,causationId:result.body.transactionId});
});
test('rejection persists one typed event and immutable replay after a later credit',async()=>{
  const w=await wallet('20.00'),bet=command(w,'BET','80.00');
  const first=await run(bet);
  expect(first).toMatchObject({httpStatus:422,body:{status:'REJECTED',failureCode:'INSUFFICIENT_FUNDS',balance:{amount:'20.00'}}});
  await run(command(w,'WIN','100.00'));
  expect(await run(bet)).toEqual({httpStatus:422,body:{...first.body,idempotentReplay:true}});
  expect(await state(w.id)).toMatchObject({balance:'120.00',version:'2',wagers:2,ledger:2,outbox:5});
  expect(await q('SELECT event_type FROM outbox_events WHERE transaction_id=?',[first.body.transactionId])).toEqual([{event_type:'WagerTransactionRejected'}]);
});
// Política padrão per_type (enunciado): um REFUND e um ROLLBACK por referência; a mesma BET pode ser creditada
// duas vezes quando o provedor envia os dois tipos. per_reference é a opção restritiva (JWP-ADR-005).
async function withPolicy<T>(policy:'per_type'|'per_reference',fn:()=>Promise<T>):Promise<T> {
  configureReversalPolicy({JWP_REVERSAL_POLICY:policy});
  try { return await fn(); } finally { configureReversalPolicy({}); }
}
test('per_type (default): REFUND then ROLLBACK of the same BET both apply (stake credited twice); same type twice is ALREADY_REVERSED',async()=>{
  expect(reversalPolicy()).toBe('per_type');
  const w=await wallet(),bet=command(w); await run(bet);
  const refund=command(w,'REFUND','25.00',bet.body.externalTransactionId);
  expect(await run(refund)).toMatchObject({httpStatus:200,body:{status:'PROCESSED',balance:{amount:'100.00'}}});
  const rollback=command(w,'ROLLBACK','25.00',bet.body.externalTransactionId);
  expect(await run(rollback)).toMatchObject({httpStatus:200,body:{status:'PROCESSED',balance:{amount:'125.00'}}});
  expect(await run(command(w,'REFUND','25.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:422,body:{status:'REJECTED',failureCode:'ALREADY_REVERSED',balance:{amount:'125.00'}}});
  expect(await run(command(w,'ROLLBACK','25.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:422,body:{failureCode:'ALREADY_REVERSED'}});
  // Replay idempotente devolve o snapshot original, sem novo efeito.
  expect(await run(rollback)).toMatchObject({httpStatus:200,body:{status:'PROCESSED',balance:{amount:'125.00'},idempotentReplay:true}});
  expect(await run(command(w,'ROLLBACK','25.00',refund.body.externalTransactionId))).toMatchObject({httpStatus:200,body:{balance:{amount:'100.00'}}});
  const win=command(w,'WIN','20.00');await run(win);
  expect(await run(command(w,'ROLLBACK','20.00',win.body.externalTransactionId))).toMatchObject({body:{balance:{amount:'100.00'}}});
  expect(await state(w.id)).toMatchObject({balance:'100.00',version:'7',wagers:8,ledger:7});
  const applied=await q(`SELECT t.kind FROM wager_transactions t JOIN wager_transactions b ON b.id=t.reference_transaction_id
    WHERE b.external_transaction_id=? AND t.status='PROCESSED' ORDER BY t.kind`,[bet.body.externalTransactionId]);
  expect(applied.map(r=>r['kind'])).toEqual(['REFUND','ROLLBACK']);
});
test('per_reference: REFUND then ROLLBACK of the same BET is ALREADY_REVERSED; rollback of REFUND and WIN debits',async()=>withPolicy('per_reference',async()=>{
  const w=await wallet(),bet=command(w); await run(bet);
  const refund=command(w,'REFUND','25.00',bet.body.externalTransactionId);
  expect(await run(refund)).toMatchObject({body:{balance:{amount:'100.00'}}});
  expect(await run(command(w,'ROLLBACK','25.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:422,body:{status:'REJECTED',failureCode:'ALREADY_REVERSED',balance:{amount:'100.00'}}});
  expect(await run(command(w,'REFUND','25.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:422,body:{failureCode:'ALREADY_REVERSED'}});
  expect(await run(command(w,'ROLLBACK','25.00',refund.body.externalTransactionId))).toMatchObject({httpStatus:200,body:{balance:{amount:'75.00'}}});
  const win=command(w,'WIN','20.00');await run(win);
  expect(await run(command(w,'ROLLBACK','20.00',win.body.externalTransactionId))).toMatchObject({body:{balance:{amount:'75.00'}}});
  expect(await state(w.id)).toMatchObject({balance:'75.00',version:'6',wagers:7,ledger:6,outbox:14});
}));
test('per_reference: ROLLBACK before REFUND of the same BET is also exclusive; a WIN referencing the BET does not consume its reversal',async()=>withPolicy('per_reference',async()=>{
  const w=await wallet(),bet=command(w); await run(bet);
  expect(await run(command(w,'WIN','30.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:200,body:{balance:{amount:'105.00'}}});
  expect(await run(command(w,'ROLLBACK','25.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:200,body:{balance:{amount:'130.00'}}});
  expect(await run(command(w,'REFUND','25.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:422,body:{status:'REJECTED',failureCode:'ALREADY_REVERSED',balance:{amount:'130.00'}}});
  expect(await state(w.id)).toMatchObject({balance:'130.00',version:'4',wagers:4,ledger:4,outbox:9});
}));
test('per_type: ROLLBACK before REFUND of the same BET both apply; WIN referencing the BET does not consume a reversal',async()=>{
  const w=await wallet(),bet=command(w); await run(bet);
  expect(await run(command(w,'WIN','30.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:200,body:{balance:{amount:'105.00'}}});
  expect(await run(command(w,'ROLLBACK','25.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:200,body:{balance:{amount:'130.00'}}});
  expect(await run(command(w,'REFUND','25.00',bet.body.externalTransactionId))).toMatchObject({httpStatus:200,body:{status:'PROCESSED',balance:{amount:'155.00'}}});
  expect(await state(w.id)).toMatchObject({balance:'155.00',version:'5',wagers:4,ledger:5});
});
test('mixed-type race: per_type applies one REFUND and one ROLLBACK; per_reference applies exactly one',async()=>{
  for (const [policy,applied,balance] of [['per_type',2,'125.00'],['per_reference',1,'100.00']] as const) await withPolicy(policy,async()=>{
    const w=await wallet(),bet=command(w);await run(bet);
    const ref=bet.body.externalTransactionId;
    const racers=Array.from({length:8},(_,i)=>command(w,i%2?'ROLLBACK':'REFUND','25.00',ref));
    const results=await Promise.all(racers.map(run));
    expect(results.filter(r=>r.httpStatus===200)).toHaveLength(applied);
    expect(results.filter(r=>r.body.failureCode==='ALREADY_REVERSED')).toHaveLength(8-applied);
    expect(await state(w.id)).toMatchObject({balance,ledger:2+applied});
    const kinds=await q(`SELECT t.kind FROM wager_transactions t JOIN wager_transactions b ON b.id=t.reference_transaction_id
      WHERE b.external_transaction_id=? AND t.status='PROCESSED' ORDER BY t.kind`,[ref]);
    if (policy==='per_type') expect(kinds.map(r=>r['kind'])).toEqual(['REFUND','ROLLBACK']);
    else expect(kinds).toHaveLength(1);
  });
});
test('database guard: unique (reference, kind) always; trigger rejects a second kind only when the TX publishes per_reference',async()=>{
  const w=await wallet(),bet=command(w);await run(bet);
  const refund=command(w,'REFUND','25.00',bet.body.externalTransactionId);await run(refund);
  const [refRow]=await q('SELECT id,reference_transaction_id FROM wager_transactions WHERE external_transaction_id=?',[refund.body.externalTransactionId]);
  const insert=async(kind:string,policy?:string)=>{
    const em=orm.em.fork();
    await em.transactional(async tx=>{
      const c=tx.getConnection(),ctx=tx.getTransactionContext();
      if(policy) await c.execute('SELECT set_config(?,?,true)',['jwp.reversal_policy',policy],'all',ctx);
      const id=randomUUID();
      await c.execute(`INSERT INTO wager_transactions(id,wallet_id,player_id,currency,provider_id,external_transaction_id,idempotency_key,
        payload_hash,round_id,game_id,kind,status,amount,reference_external_transaction_id,reference_transaction_id,response_snapshot,processed_at)
        VALUES (?,?,?,'BRL','provider-a',?,?,repeat('a',64),'round-a','game-a',?,'PROCESSED','25.00',?,?,'{}'::jsonb,now())`,
        [id,w.id,w.playerId,id,'key:'+id,kind,bet.body.externalTransactionId,refRow!['reference_transaction_id']],'all',ctx);
      // Desfaz sempre: o teste só observa se o banco aceitou a linha.
      throw Object.assign(new Error('rollback-probe'),{probe:true});
    }).catch((e:any)=>{ if(!e.probe) throw e; });
  };
  await expect(insert('REFUND')).rejects.toMatchObject({code:'23505'});
  await expect(insert('REFUND','per_type')).rejects.toMatchObject({code:'23505'});
  await expect(insert('ROLLBACK','per_type')).resolves.toBeUndefined();
  await expect(insert('ROLLBACK')).resolves.toBeUndefined();
  await expect(insert('ROLLBACK','per_reference')).rejects.toMatchObject({code:'23505'});
  expect(await state(w.id)).toMatchObject({balance:'100.00'});
});
test('reversal of credit without funds has its own rejection code and consumes no reversal',async()=>{
  const w=await wallet('0.00'),win=command(w,'WIN','20.00');await run(win);
  await run(command(w,'BET','20.00'));
  const rejected=command(w,'ROLLBACK','20.00',win.body.externalTransactionId);
  expect(await run(rejected)).toMatchObject({httpStatus:422,body:{failureCode:'REVERSAL_INSUFFICIENT_FUNDS'}});
  await run(command(w,'WIN','20.00'));
  expect(await run(command(w,'ROLLBACK','20.00',win.body.externalTransactionId))).toMatchObject({httpStatus:200,body:{balance:{amount:'0.00'}}});
  expect(await run(rejected)).toMatchObject({httpStatus:422,body:{failureCode:'REVERSAL_INSUFFICIENT_FUNDS',idempotentReplay:true}});
  expect(await state(w.id)).toMatchObject({balance:'0.00',version:'5',ledger:4});
});
test('reference validation: context, amount, kind, terminal failure and self reference',async()=>{
  const w=await wallet(),other=await wallet(),bet=command(w);await run(bet);
  for(const [request,code] of [
    [command(w,'REFUND','10.00',bet.body.externalTransactionId),'REVERSAL_AMOUNT_MISMATCH'],
    [command(other,'REFUND','25.00',bet.body.externalTransactionId),'REFERENCE_CONTEXT_MISMATCH'],
    [{...command(w,'REFUND','25.00',bet.body.externalTransactionId),body:{...command(w,'REFUND','25.00',bet.body.externalTransactionId).body,roundId:'other'}},'REFERENCE_CONTEXT_MISMATCH'],
  ] as const) expect(await run(request)).toMatchObject({httpStatus:422,body:{failureCode:code}});
  const rejected=command(w,'BET','200.00');await run(rejected);
  expect(await run(command(w,'REFUND','200.00',rejected.body.externalTransactionId))).toMatchObject({body:{failureCode:'REFERENCE_NOT_PROCESSED'}});
  const loss=command(w,'LOSS','0.00');await run(loss);
  expect(await run(command(w,'ROLLBACK','1.00',loss.body.externalTransactionId))).toMatchObject({body:{failureCode:'REFERENCE_KIND_INVALID'}});
  const self=command(w,'REFUND','1.00');self.body.referenceExternalTransactionId=self.body.externalTransactionId;
  expect(await run(self)).toMatchObject({body:{failureCode:'SELF_REFERENCE'}});
  const win=command(w,'WIN','10.00',bet.body.externalTransactionId);win.body.gameId='different-game';
  expect(await run(win)).toMatchObject({httpStatus:200});
  await state(w.id);await state(other.id);
});
test('missing/nonterminal references persist one pending event and fixed TTL; polling has no effects',async()=>{
  const w=await wallet(),pending=command(w,'REFUND','25.00','future-bet');
  const first=await run(pending);
  expect(first).toMatchObject({httpStatus:202,body:{status:'PENDING_REFERENCE',idempotentReplay:false}});
  expect(first.body).not.toHaveProperty('balance');
  const before=await state(w.id);
  for(let i=0;i<10;i++) expect(await run(pending)).toEqual({httpStatus:202,body:{...first.body,idempotentReplay:true}});
  const [row]=await q('SELECT response_snapshot FROM wager_transactions WHERE id=?',[first.body.transactionId]);
  const stored=row!['response_snapshot'] as {pendingReferenceAt:string;referenceExpiresAt:string};
  expect(Date.parse(stored.referenceExpiresAt)-Date.parse(stored.pendingReferenceAt)).toBe(900000);
  expect(await state(w.id)).toEqual(before);
  const nested=command(w,'ROLLBACK','25.00',pending.body.externalTransactionId);
  expect(await run(nested)).toMatchObject({httpStatus:202});
  const future=command(w);future.body.externalTransactionId='future-bet';await run(future);

  expect(await run(pending)).toMatchObject({httpStatus:202});
  await state(w.id);
});
test('idempotency/provider identities distinguish payload and external conflicts',async()=>{
  const w=await wallet(),c=command(w);await run(c);
  const before=await state(w.id);
  await expect(run({...c,body:{...c.body,money:{amount:'1.00',currency:'BRL'}}})).rejects.toMatchObject({code:'IDEMPOTENCY_CONFLICT',httpStatus:409});
  await expect(run({...c,key:'other-key'})).rejects.toMatchObject({code:'EXTERNAL_IDENTITY_CONFLICT',httpStatus:409});
  expect(await state(w.id)).toEqual(before);
  expect(await run({...c,body:{...c.body,providerId:'provider-b'}})).toMatchObject({httpStatus:200,body:{idempotentReplay:false}});
  expect(await state(w.id)).toMatchObject({balance:'50.00',version:'3'});
});
test('same logical wager with UUID case changed replays over real HTTP instead of 409 or a second debit',async()=>{
  type Body=ReturnType<typeof command>['body'];
  const post=(body:unknown,key:string)=>fetch(base+'/wagering/transactions',{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':key},body:JSON.stringify(body)});
  const lower=(b:Body)=>b,upper=(b:Body)=>({...b,playerId:b.playerId.toUpperCase(),walletId:b.walletId.toUpperCase()});
  for(const [first,retry] of [[lower,upper],[upper,lower]]){
    const w=await wallet(),c=command(w);
    const created=await post(first!(c.body),c.key);expect(created.status).toBe(200);const snapshot=await created.json();
    const replay=await post(retry!(c.body),c.key);expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual({...snapshot,idempotentReplay:true});
    expect((await q('SELECT player_id,wallet_id FROM wager_transactions WHERE id=?',[snapshot.transactionId]))[0]).toEqual({player_id:w.playerId,wallet_id:w.id});
    expect(await state(w.id)).toMatchObject({balance:'75.00',version:'2',wagers:1,ledger:2,outbox:4});
  }
});
test('exact values above 2^53 and range overflow rejection remain balanced',async()=>{
  const w=await wallet('9007199254740993.01');
  expect(await run(command(w,'BET','0.02'))).toMatchObject({body:{balance:{amount:'9007199254740992.99'}}});
  await state(w.id);
  const max=await wallet('999999999999999999.99');
  expect(await run(command(max,'WIN','0.01'))).toMatchObject({httpStatus:422,body:{failureCode:'MONETARY_RANGE_EXCEEDED',balance:{amount:'999999999999999999.99'}}});
  expect(await state(max.id)).toMatchObject({version:'1',ledger:1,outbox:3});
});
test('invalid wallet/player/currency context writes nothing',async()=>{
  const w=await wallet(),before=await state(w.id);
  const wrong=command(w);wrong.body.playerId=randomUUID();
  await expect(run(wrong)).rejects.toMatchObject({code:'PLAYER_MISMATCH'});
  const missing=command(w);missing.body.walletId=randomUUID();
  await expect(run(missing)).rejects.toMatchObject({code:'WALLET_NOT_FOUND'});
  expect(await state(w.id)).toEqual(before);
  const usd=Wallet.open({id:randomUUID(),playerId:randomUUID(),initialBalance:Money.zero('USD')});
  await new SqlWalletOpeningStore(orm).create(usd,{correlationId:'currency-test',occurredAt:new Date()});
  await expect(run(command(usd))).rejects.toMatchObject({code:'CURRENCY_MISMATCH',httpStatus:400});
  expect(await state(usd.id)).toMatchObject({balance:'0.00',version:'1',wagers:0,ledger:0,outbox:0});
});

async function fault(table:'ledger_entries'|'outbox_events',deferred=false) {
  const w=await wallet(),c=command(w),before=await state(w.id);
  await q(`CREATE FUNCTION test_wager_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled failure' USING ERRCODE='23514'; END $$`);
  await q(deferred
    ? `CREATE CONSTRAINT TRIGGER test_wager_fault AFTER INSERT ON ${table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_wager_fault()`
    : `CREATE TRIGGER test_wager_fault BEFORE INSERT ON ${table} FOR EACH ROW EXECUTE FUNCTION test_wager_fault()`);
  try {
    await expect(run(c)).rejects.toMatchObject({code:'23514'});
    expect(await state(w.id)).toEqual(before);
  } finally { await q(`DROP TRIGGER test_wager_fault ON ${table}`);await q('DROP FUNCTION test_wager_fault()'); }
  expect(await run(c)).toMatchObject({httpStatus:200,body:{balance:{amount:'75.00'}}});
  await state(w.id);
}
test('ledger insertion failure rolls back transaction, wallet, snapshot and events',()=>fault('ledger_entries'));
test('outbox insertion failure rolls back all financial writes',()=>fault('outbox_events'));
test('deferred commit failure returns no success and rolls back all writes',()=>fault('outbox_events',true));

async function startWorkers(commands:ReturnType<typeof command>[],count=1) {
  const workers=commands.map(c=>Bun.spawn([process.execPath,import.meta.dir+'/process-worker.ts'],{
    env:{...process.env,JWP_WAGER_COMMAND:JSON.stringify(c),JWP_WAGER_COUNT:String(count)},stdin:'pipe',stdout:'pipe',stderr:'pipe'}));
  const readers=workers.map(w=>w.stdout.getReader());
  const ready=await Promise.all(readers.map(async reader=>JSON.parse(new TextDecoder().decode((await reader.read()).value)) as {pid:number;backend:number;ready:boolean}));
  expect(new Set(ready.map(r=>r.pid)).size).toBe(workers.length);
  expect(new Set(ready.map(r=>r.backend)).size).toBe(workers.length);
  for(const w of workers) { w.stdin.write('go\n');w.stdin.end(); }
  return {ready,async results() {
    try {
      return await Promise.all(readers.map(async(reader,i)=>{
        let text='';for(;;) { const part=await reader.read();if(part.done)break;text+=new TextDecoder().decode(part.value); }
        const stderr=await new Response(workers[i]!.stderr).text();
        expect({exit:await workers[i]!.exited,stderr}).toEqual({exit:0,stderr:''});
        return JSON.parse(text).results as {httpStatus?:number;body?:{transactionId:string;idempotentReplay:boolean;failureCode?:string};error?:string}[];
      }));
    } finally { for(const w of workers) if(w.exitCode===null)w.kill(); }
  }};
}
test('50 simultaneous repeats across five real processes debit once',async()=>{
  const w=await wallet(),c=command(w),workers=await startWorkers(Array(5).fill(c),10);
  const results=(await workers.results()).flat();
  expect(results).toHaveLength(50);expect(results.filter(r=>r.httpStatus===200)).toHaveLength(50);
  expect(results.filter(r=>!r.body!.idempotentReplay)).toHaveLength(1);
  expect(new Set(results.map(r=>r.body!.transactionId)).size).toBe(1);
  expect(await state(w.id)).toMatchObject({balance:'75.00',version:'2',wagers:1,ledger:2,outbox:4});
  console.log(JSON.stringify({proof:'50-parallel-replays',processes:workers.ready,applied:1,replays:49}));
},30000);
test('100/80/80 with three real processes produces one debit and no lost update',async()=>{
  const w=await wallet(),workers=await startWorkers([command(w,'BET','80.00'),command(w,'BET','80.00'),command(w,'LOSS','0.00')]);
  const results=(await workers.results()).flat();
  expect(results.filter(r=>r.httpStatus===200)).toHaveLength(2);
  expect(results.filter(r=>r.body?.failureCode==='INSUFFICIENT_FUNDS')).toHaveLength(1);
  expect(await state(w.id)).toMatchObject({balance:'20.00',version:'2',wagers:3,ledger:2,outbox:6});
  console.log(JSON.stringify({proof:'100-80-80',processes:workers.ready,balance:'20.00'}));
},30000);

async function waitForLock(fragment:string,n=1) {
  for(let i=0;i<300;i++) {
    const rows=await q(`SELECT count(*)::int n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query ILIKE ?`,[fragment+'%']);
    if(Number(rows[0]!['n'])>=n)return;
    await Bun.sleep(5);
  }
  throw new Error('Controlled SQL lock barrier was not reached.');
}
test('cross-wallet UNIQUE losers recover in a new transaction for both identity constraints',async()=>{
  for(const identity of ['key','external'] as const) {
    const a=await wallet(),b=await wallet(),first=command(a),second=command(b);
    if(identity==='key')second.key=first.key;else second.body.externalTransactionId=first.body.externalTransactionId;
    const blocker=orm.em.fork();await blocker.begin();
    await blocker.getConnection().execute('LOCK TABLE wager_transactions IN SHARE MODE',[],'all',blocker.getTransactionContext());
    const pending=Promise.all([run(first).catch(e=>({error:e.code})),run(second).catch(e=>({error:e.code}))]);
    try { await waitForLock('INSERT INTO wager_transactions',2); } finally { await blocker.rollback(); }
    const results=await pending;
    expect(results.filter(r=>'httpStatus' in r && r.httpStatus===200)).toHaveLength(1);
    expect(results.filter(r=>'error' in r)).toEqual([{error:identity==='key'?'IDEMPOTENCY_CONFLICT':'EXTERNAL_IDENTITY_CONFLICT'}]);
    const states=await Promise.all([state(a.id),state(b.id)]);
    expect(states.map(s=>s['balance']).sort()).toEqual(['100.00','75.00']);
  }
},15000);
test('wallet B completes while wallet A is locked; bounded exhaustion returns typed 503',async()=>{
  const a=await wallet(),b=await wallet(),before=await state(a.id);
  const blocker=orm.em.fork();await blocker.begin();
  await blocker.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE',[a.id],'all',blocker.getTransactionContext());
  const limited=new ProcessWager(new SqlProcessWagerStore(orm,{lockTimeoutMs:80,statementTimeoutMs:400,transactionTimeoutMs:500,budgetMs:2000,maxAttempts:3,retryBaseMs:5}));
  const c=command(a);const pending=limited.execute(c.body,c.key).catch(e=>e);
  try {
    await waitForLock('select%from "wallets"%for update');
    expect(await run(command(b))).toMatchObject({httpStatus:200});
    expect(await pending).toMatchObject({code:'INFRASTRUCTURE_UNAVAILABLE',httpStatus:503,cause:{code:'55P03'}});
    expect(await state(a.id)).toEqual(before);
  } finally { await blocker.rollback(); }
  expect(await run(c)).toMatchObject({httpStatus:200});await state(a.id);await state(b.id);
},10000);
test('concurrent same-type reversals produce one effect and ALREADY_REVERSED (both policies)',async()=>{
  for (const policy of ['per_type','per_reference'] as const) await withPolicy(policy,async()=>{
  const w=await wallet(),bet=command(w);await run(bet);
  const results=await Promise.all(Array.from({length:8},()=>run(command(w,'REFUND','25.00',bet.body.externalTransactionId))));
  expect(results.filter(r=>r.httpStatus===200)).toHaveLength(1);
  expect(results.filter(r=>r.body.failureCode==='ALREADY_REVERSED')).toHaveLength(7);
  expect(await state(w.id)).toMatchObject({balance:'100.00',version:'3',ledger:3,wagers:9,outbox:13});
  });
});
test('real SQL deadlock aborts the attempt and whole-TX retry applies once',async()=>{
  const w=await wallet(),c=command(w);
  const limitedOrm=await openFinancialOrm(process.env['JWP_WAGER_DATABASE_URL']!);
  let attempts=0;
  const causes:string[]=[];
  const fork=limitedOrm.em.fork.bind(limitedOrm.em);
  limitedOrm.em.fork=((...args:Parameters<typeof limitedOrm.em.fork>)=>{
    attempts++;
    const em=fork(...args),transactional=em.transactional.bind(em);
    em.transactional=(async(...parameters:Parameters<typeof em.transactional>)=>{
      try { return await transactional(...parameters); }
      catch(error) { causes.push((error as {code:string}).code);throw error; }
    }) as typeof em.transactional;
    return em;
  }) as typeof limitedOrm.em.fork;
  const useCase=new ProcessWager(new SqlProcessWagerStore(limitedOrm,{lockTimeoutMs:2500}));
  const peer=orm.em.fork();await peer.begin();
  // A TX financeira segura wallet e espera outbox; a sessão concorrente segura
  // outbox e solicita a wallet. Esse ciclo provoca deadlock real no PostgreSQL.
  await peer.getConnection().execute('LOCK TABLE outbox_events IN SHARE MODE',[],'all',peer.getTransactionContext());
  const pending=useCase.execute(c.body,c.key);
  try {
    await waitForLock('INSERT INTO outbox_events');
    await peer.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE',[w.id],'all',peer.getTransactionContext());
  } finally { await peer.rollback(); }
  try {
    expect(await pending).toMatchObject({httpStatus:200,body:{idempotentReplay:false,balance:{amount:'75.00'}}});
    expect(causes).toContain('40P01');expect(attempts).toBe(2);
    expect(await state(w.id)).toMatchObject({balance:'75.00',version:'2',wagers:1,ledger:2,outbox:4});
  } finally { await limitedOrm.close(true); }
},15000);
test('real HTTP lock exhaustion returns retryable 503 and same-key retry succeeds',async()=>{
  const w=await wallet(),c=command(w),before=await state(w.id);
  const blocker=orm.em.fork();await blocker.begin();
  await blocker.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE',[w.id],'all',blocker.getTransactionContext());
  const post=()=>fetch(base+'/wagering/transactions',{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':c.key},body:JSON.stringify(c.body)});
  try {
    const response=await post();expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({error:{code:'INFRASTRUCTURE_UNAVAILABLE',retryable:true}});
    expect(await state(w.id)).toEqual(before);
  } finally { await blocker.rollback(); }
  const response=await post();expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({idempotentReplay:false,balance:{amount:'75.00'}});
  await state(w.id);
},15000);
test('real Nest HTTP preserves opaque header, status codes, historical replay and rejects body fallback',async()=>{
  const w=await wallet(),c=command(w);c.key='opaque-header-key';
  const post=(body:unknown,key?:string)=>fetch(base+'/wagering/transactions',{method:'POST',headers:{'Content-Type':'application/json',...(key===undefined?{}:{'Idempotency-Key':key})},body:JSON.stringify(body)});
  const absent=await post(c.body);expect(absent.status).toBe(400);expect(await absent.json()).toMatchObject({error:{code:'IDEMPOTENCY_KEY_REQUIRED',retryable:false}});
  expect((await post({...c.body,idempotencyKey:'body-fallback'})).status).toBe(400);
  expect((await post({...c.body,idempotencyKey:'body-key'},c.key)).status).toBe(400);
  const first=await post(c.body,c.key);expect(first.status).toBe(200);const snapshot=await first.json();
  expect((await q('SELECT idempotency_key FROM wager_transactions WHERE id=?',[snapshot.transactionId]))[0]!['idempotency_key']).toBe(c.key);
  await run(command(w,'WIN','20.00'));
  const replay=await post(c.body,c.key);expect(replay.status).toBe(200);expect(await replay.json()).toEqual({...snapshot,idempotentReplay:true});
  expect((await post({...c.body,money:{amount:'2.00',currency:'BRL'}},c.key)).status).toBe(409);
  expect((await post(c.body,'other-key')).status).toBe(409);
  const bad=command(w,'BET','999.00');expect((await post(bad.body,bad.key)).status).toBe(422);
  const pending=command(w,'REFUND','1.00','missing-reference');expect((await post(pending.body,pending.key)).status).toBe(202);
  await state(w.id);
},10000);


test('internal provider submissions are rejected before wallet, wager or outbox changes', async()=>{
  const w=await wallet(),before=await state(w.id),c=command(w);
  c.body.providerId='internal:wallet-opening';
  await expect(run(c)).rejects.toMatchObject({code:'INVALID_PAYLOAD',httpStatus:400});
  const response=await fetch(base+'/wagering/transactions',{method:'POST',headers:{'content-type':'application/json','idempotency-key':c.key},body:JSON.stringify(c.body)});
  expect(response.status).toBe(400);
  expect(await response.json()).toMatchObject({error:{code:'INVALID_PAYLOAD',retryable:false}});
  expect(await state(w.id)).toEqual(before);
});

test('pending TTL is anchored to persisted acceptance, not an earlier prepared command',async()=>{
  const w=await wallet(),c=command(w,'REFUND','25.00','delayed-future-bet');
  const prepared={...parseWager(c.body,c.key),createdAt:new Date(Date.now()-60000)};
  const started=Date.now();
  const result=await new SqlProcessWagerStore(orm).process(prepared,{correlationId:'delayed-acceptance'});
  expect(result.httpStatus).toBe(202);
  const [row]=await q('SELECT created_at,response_snapshot FROM wager_transactions WHERE id=?',[result.body.transactionId]);
  const accepted=new Date(row!['created_at'] as string).getTime();
  const snapshot=row!['response_snapshot'] as {pendingReferenceAt:string;referenceExpiresAt:string};
  expect(accepted).toBeGreaterThanOrEqual(started);
  expect(Date.parse(snapshot.referenceExpiresAt)-accepted).toBe(900000);
  expect(Date.parse(snapshot.pendingReferenceAt)).toBeGreaterThanOrEqual(accepted);
  const before=await state(w.id);
  for(let i=0;i<3;i++) expect(await run(c)).toMatchObject({httpStatus:202,body:{idempotentReplay:true}});
  expect((await q('SELECT response_snapshot FROM wager_transactions WHERE id=?',[result.body.transactionId]))[0]!['response_snapshot']).toEqual(snapshot);
  expect(await state(w.id)).toEqual(before);
});


test('real HTTP pool exhaustion returns typed 503 without writes and same-key recovery applies once',async()=>{
  const w=await wallet(),c=command(w),before=await state(w.id);
  const holders=[];
  const post=()=>fetch(base+'/wagering/transactions',{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':c.key},body:JSON.stringify(c.body)});
  try {

    for(let i=0;i<4;i++){const em=orm.em.fork();await em.begin();holders.push(em);}
    const response=await post();
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({error:{code:'INFRASTRUCTURE_UNAVAILABLE',retryable:true}});
  } finally {for(const em of holders)await em.rollback();}
  expect(await state(w.id)).toEqual(before);
  const response=await post();expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({idempotentReplay:false,balance:{amount:'75.00'}});
  expect(await state(w.id)).toMatchObject({balance:'75.00',version:'2',wagers:1,ledger:2,outbox:4});
  const replay=await post();expect(replay.status).toBe(200);
  expect(await replay.json()).toMatchObject({idempotentReplay:true,balance:{amount:'75.00'}});
  expect(await state(w.id)).toMatchObject({wagers:1,ledger:2,outbox:4});
},30000);

test('LockMode keeps row locking and fresh SQL state without retaining an identity map snapshot',async()=>{
  const w=await wallet();
  const store=new SqlProcessWagerStore(orm);
  await orm.em.fork().transactional(async em=>{
    await em.getConnection().execute('SET LOCAL ROLE jwp_runtime',[],'all',em.getTransactionContext());
    const first=command(w);
    await store.processInTransaction(em,parseWager(first.body,'lockmode-first'),{correlationId:'lockmode'});
    expect(em.getUnitOfWork().getIdentityMap().values()).toHaveLength(0);
    const record=await em.findOneOrFail(WalletRecord,w.id,{lockMode:LockMode.PESSIMISTIC_WRITE,disableIdentityMap:true});
    expect(record.balance).toBe('75.00');expect(String(record.version)).toBe('2');
    await store.processInTransaction(em,parseWager({...command(w).body,money:{amount:'5.00',currency:'BRL'}},'lockmode-second'),{correlationId:'lockmode'});
    const fresh=await em.findOneOrFail(WalletRecord,w.id,{lockMode:LockMode.PESSIMISTIC_WRITE,disableIdentityMap:true});
    expect(fresh.balance).toBe('70.00');expect(String(fresh.version)).toBe('3');
    expect(em.getUnitOfWork().getIdentityMap().values()).toHaveLength(0);
  });
  expect(await state(w.id)).toMatchObject({balance:'70.00',calculated:'70.00',version:'3',ledger:3});
});
