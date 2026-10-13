import { afterAll, beforeAll, beforeEach, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { openFinancialOrm } from '../../../../src/persistence/financial-orm.js';
import {
  SqlPendingReferenceScheduler,
  type PendingReferenceClaim,
} from '../../../../src/messaging/pending-reference-scheduler.js';
import { OpenWallet } from '../../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { ProcessWager } from '../../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../../src/wagering/infrastructure/sql-process-wager-store.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let openWallet: OpenWallet;
let processWager: ProcessWager;
let migrated = false;
const query = (sql: string, args: unknown[] = []) =>
  orm.em.fork().getConnection().execute<Record<string, unknown>[]>(sql, args);

beforeAll(async () => {
  const databaseUrl = process.env['JWP_SCHEDULER_DATABASE_URL'];
  const container = process.env['JWP_SCHEDULER_POSTGRES_CONTAINER'];
  if (!databaseUrl || !container?.startsWith('jwp-scheduler-')) {
    throw new Error(
      'Dedicated JWP_SCHEDULER_DATABASE_URL and JWP_SCHEDULER_POSTGRES_CONTAINER (jwp-scheduler-*) required.',
    );
  }
  orm = await openFinancialOrm(databaseUrl);
  expect((await query("SELECT to_regclass('public.wallets') relation"))[0]?.['relation']).toBeNull();
  await orm.migrator.up();
  migrated = true;
  openWallet = new OpenWallet(new SqlWalletOpeningStore(orm));
  processWager = new ProcessWager(new SqlProcessWagerStore(orm));
});

// Isola os jobs vencidos do cenário para impedir competição com pendências antigas.
beforeEach(async () => {
  await query("UPDATE pending_reference_jobs SET next_attempt_at=clock_timestamp()+interval '1 hour'");
});

afterAll(async () => {
  if (!orm) return;
  try {
    if (migrated) await orm.migrator.down({ to: 0 });
  } finally {
    await orm.close(true);
  }
});

async function wallet(initialBalance = '100.00') {
  return openWallet.execute({
    playerId: randomUUID(),
    initialBalance: { amount: initialBalance, currency: 'BRL' },
  });
}

function identities(target: { id: string; playerId: string }) {
  const suffix = randomUUID();
  return {
    providerId: `provider-${suffix}`,
    referenceExternalTransactionId: `bet-${suffix}`,
    refundExternalTransactionId: `refund-${suffix}`,
    walletId: target.id,
    playerId: target.playerId,
    roundId: `round-${suffix}`,
    gameId: `game-${suffix}`,
  };
}

async function pendingRefund(ids: ReturnType<typeof identities>, kind: 'REFUND' | 'ROLLBACK' = 'REFUND') {
  const input = {
    providerId: ids.providerId,
    externalTransactionId: ids.refundExternalTransactionId,
    walletId: ids.walletId,
    playerId: ids.playerId,
    roundId: ids.roundId,
    gameId: ids.gameId,
    kind,
    money: { amount: '25.00', currency: 'BRL' },
    referenceExternalTransactionId: ids.referenceExternalTransactionId,
  };
  const key = `refund-key-${randomUUID()}`;
  const result = await processWager.execute(input, key);
  expect(result).toMatchObject({ httpStatus: 202, body: { status: 'PENDING_REFERENCE' } });
  return { input, key, transactionId: result.body.transactionId };
}

async function referenceBet(ids: ReturnType<typeof identities>) {
  const result = await processWager.execute(
    {
      providerId: ids.providerId,
      externalTransactionId: ids.referenceExternalTransactionId,
      walletId: ids.walletId,
      playerId: ids.playerId,
      roundId: ids.roundId,
      gameId: ids.gameId,
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    },
    `bet-key-${randomUUID()}`,
  );
  expect(result).toMatchObject({ httpStatus: 200, body: { status: 'PROCESSED' } });
  return result.body.transactionId;
}

async function job(transactionId: string) {
  return (await query(
    `SELECT transaction_id,attempts,next_attempt_at,expires_at,claim_token,lease_until
      FROM pending_reference_jobs WHERE transaction_id=?`,
    [transactionId],
  ))[0];
}

async function financialState(walletId: string) {
  return (await query(
    `SELECT balance::text,version::text,
      (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=wallet.id) ledger,
      (SELECT count(*)::int FROM wager_transactions WHERE wallet_id=wallet.id) wagers
      FROM wallets wallet WHERE id=?`,
    [walletId],
  ))[0];
}

async function eventCount(transactionId: string, eventType: string) {
  return (await query(
    `SELECT count(*)::int count FROM outbox_events
      WHERE transaction_id=? AND event_type=?`,
    [transactionId, eventType],
  ))[0]?.['count'];
}


async function worker(options: object) {
  const child=Bun.spawn(['bun','tests/integration/messaging/scheduler/audit-worker.ts'],{
    stdin:'pipe',stdout:'pipe',stderr:'pipe',env:{...Bun.env,JWP_SCHEDULER_AUDIT_OPTIONS:JSON.stringify(options)}
  });
  const reader=child.stdout.getReader();let buffer='';
  const line=async()=>{
    while(!buffer.includes('\n')){const part=await reader.read();if(part.done)throw new Error(await new Response(child.stderr).text());buffer+=new TextDecoder().decode(part.value);}
    const end=buffer.indexOf('\n'),text=buffer.slice(0,end);buffer=buffer.slice(end+1);return JSON.parse(text);
  };
  const ready=await line();
  return {child,line,ready,go:()=>child.stdin.write('go\n'),close:async()=>{child.stdin.end();await child.exited;reader.releaseLock();}};
}
async function due(...transactionIds: string[]) {
 // Backfill pode recriar jobs antigos no mesmo cenário; também precisam ser isolados.
 await query("UPDATE pending_reference_jobs SET next_attempt_at=clock_timestamp()+interval '1 hour'");
 for (const transactionId of transactionIds) {
  // clock_timestamp tem microssegundos; Date só milissegundos, exigindo margem no relógio da fixture.
  await query("UPDATE pending_reference_jobs SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE transaction_id=?",[transactionId]);
 }
}
async function blockedBackend(backend:number){
 for(let i=0;i<200;i++){
  const rows=await query("SELECT wait_event_type FROM pg_stat_activity WHERE pid=?",[backend]);
  if(rows[0]?.['wait_event_type']==='Lock')return;
  await Bun.sleep(10);
 }
 throw new Error('Worker never reached controlled PostgreSQL lock barrier');
}

test('a scheduler clock behind the replica that persisted a pending wager does not reject its transition',async()=>{
 const target=await wallet(),ids=identities(target),systemDate=Date;
 const fastClock=systemDate.now()+3000;
 class AheadDate extends systemDate {
  constructor(value?: string|number){super(value===undefined?fastClock:value);}
  static override now(){return fastClock;}
 }
 let pending:Awaited<ReturnType<typeof pendingRefund>>,referenceTransactionId='';

 try{globalThis.Date=AheadDate as DateConstructor;pending=await pendingRefund(ids);referenceTransactionId=await referenceBet(ids);}
 finally{globalThis.Date=systemDate;}
 const slowSchedulerAt=new systemDate(systemDate.now()-2000);
 await due(pending.transactionId);
 // O job deve vencer no relógio simulado do worker atrasado, não no relógio do banco.
 await query('UPDATE pending_reference_jobs SET next_attempt_at=? WHERE transaction_id=?',
  [new systemDate(slowSchedulerAt.getTime()-1000),pending.transactionId]);
 const [stored]=await query('SELECT created_at,response_snapshot FROM wager_transactions WHERE id=?',[pending.transactionId]);
 const fastCreatedAt=new systemDate(stored!['created_at'] as string);
 const scheduler=new SqlPendingReferenceScheduler(orm,{leaseMs:30_000,jitter:()=>0});
 const claim=await scheduler.claimOne(slowSchedulerAt);expect(claim?.transactionId).toBe(pending.transactionId);
 let outcome:unknown;
 try{outcome=await scheduler.processClaim(claim!,slowSchedulerAt);}
 catch(error){outcome={rejected:error instanceof Error?error.message:String(error)};}
 expect(outcome).toEqual({status:'PROCESSED',transactionId:pending.transactionId});
 const state=await financialState(target.id);
 expect(state).toMatchObject({balance:'100.00',version:'3',ledger:3});
 const [wager]=await query('SELECT status,reference_transaction_id FROM wager_transactions WHERE id=?',[pending.transactionId]);
 expect(wager).toEqual({status:'PROCESSED',reference_transaction_id:referenceTransactionId});
 expect(await eventCount(pending.transactionId,'WagerTransactionProcessed')).toBe(1);
 const [financial]=await query(`SELECT wallet.balance::text balance,
    coalesce(sum(CASE WHEN ledger.direction='CREDIT' THEN ledger.amount ELSE -ledger.amount END),0)::text ledger_sum,
    count(*)::int ledger_rows,count(DISTINCT ledger.transaction_id)::int distinct_transactions,
    (wallet.balance<0) negative
   FROM wallets wallet LEFT JOIN ledger_entries ledger ON ledger.wallet_id=wallet.id
   WHERE wallet.id=? GROUP BY wallet.id`,[target.id]);
 expect(financial!['balance']).toBe(financial!['ledger_sum']);
 expect(financial).toMatchObject({ledger_rows:3,distinct_transactions:3,negative:false});
 const [persisted]=await query(`SELECT wager.created_at,wager.processed_at,ledger.created_at ledger_created_at,
    event.envelope->>'occurredAt' event_occurred_at
   FROM wager_transactions wager JOIN ledger_entries ledger ON ledger.transaction_id=wager.id
   JOIN outbox_events event ON event.transaction_id=wager.id AND event.event_type='WagerTransactionProcessed'
   WHERE wager.id=?`,[pending.transactionId]);
 expect(new Date(persisted!['processed_at'] as string).getTime()).toBeGreaterThanOrEqual(fastCreatedAt.getTime());
 expect(new Date(persisted!['ledger_created_at'] as string).getTime()).toBeGreaterThanOrEqual(fastCreatedAt.getTime());
 expect(new Date(persisted!['event_occurred_at'] as string).getTime()).toBeGreaterThanOrEqual(fastCreatedAt.getTime());
 console.log(JSON.stringify({proof:'scheduler-cross-replica-clock-skew',skewMs:fastCreatedAt.getTime()-slowSchedulerAt.getTime(),fastCreatedAt:fastCreatedAt.toISOString(),slowSchedulerAt:slowSchedulerAt.toISOString(),outcome:'PROCESSED',state}));
});

test('three separate OS workers race for a single durable claim',async()=>{
 const target=await wallet(),ids=identities(target),pending=await pendingRefund(ids);await referenceBet(ids);await due(pending.transactionId);
 const workers=await Promise.all([1,2,3].map(()=>worker({mode:'claim-only',leaseMs:5000})));
 try{
  expect(new Set([process.pid,...workers.map(w=>w.ready.pid)]).size).toBe(4);
  expect(new Set(workers.map(w=>w.ready.backend)).size).toBe(3);
  workers.forEach(w=>w.go());const results=await Promise.all(workers.map(w=>w.line()));
  const claims=results.map(r=>r.claimed).filter(Boolean);expect(claims).toHaveLength(1);
  const claim={...claims[0],expiresAt:new Date(claims[0].expiresAt),leaseUntil:new Date(claims[0].leaseUntil)};
  expect(await new SqlPendingReferenceScheduler(orm).processClaim(claim)).toMatchObject({status:'PROCESSED',transactionId:pending.transactionId});
  expect(await financialState(target.id)).toMatchObject({balance:'100.00',version:'3',ledger:3});
  console.log(JSON.stringify({proof:'scheduler-three-processes',pids:workers.map(w=>w.ready.pid),backends:workers.map(w=>w.ready.backend),claims:claims.length}));
 }finally{await Promise.all(workers.map(w=>w.close()));}
},15000);

test('real SIGKILL after durable claim and after terminal commit preserves one financial effect',async()=>{
 const target=await wallet(),ids=identities(target),pending=await pendingRefund(ids);await referenceBet(ids);await due(pending.transactionId);
 const crashed=await worker({mode:'claim-barrier',leaseMs:200});crashed.go();const {claimed}=await crashed.line();
 expect(claimed.transactionId).toBe(pending.transactionId);expect((await job(pending.transactionId))?.['claim_token']).toBe(claimed.token);
 crashed.child.kill('SIGKILL');await crashed.close();
 await Bun.sleep(Math.max(0,new Date(claimed.leaseUntil).getTime()-Date.now()+10));
 const restarted=await worker({mode:'commit-barrier',leaseMs:2000});restarted.go();const recovered=await restarted.line();const result=await restarted.line();
 expect(recovered.claimed.token).not.toBe(claimed.token);expect(recovered.claimed.attempts).toBe(2);
 expect(result.outcome).toEqual({status:'PROCESSED',transactionId:pending.transactionId});
 expect(await job(pending.transactionId)).toBeUndefined();const before=await financialState(target.id);
 restarted.child.kill('SIGKILL');await restarted.close();
 const next=new SqlPendingReferenceScheduler(orm);
 expect(await next.runOnce()).toEqual({status:'IDLE'});
 const old={...claimed,expiresAt:new Date(claimed.expiresAt),leaseUntil:new Date(claimed.leaseUntil)};
 expect(await next.processClaim(old)).toMatchObject({status:'LOST_LEASE'});
 expect(await financialState(target.id)).toEqual(before);
 expect(before).toMatchObject({balance:'100.00',version:'3',ledger:3});
 expect(await eventCount(pending.transactionId,'WagerTransactionProcessed')).toBe(1);
 expect((await processWager.execute(pending.input,pending.key)).body).toMatchObject({idempotentReplay:true,balance:{amount:'100.00'}});
 console.log(JSON.stringify({proof:'scheduler-real-SIGKILL',killedPids:[crashed.ready.pid,restarted.ready.pid],recoveredAttempts:recovered.claimed.attempts,before}));
},15000);

test('a lease expiring while waiting for the wallet lock invalidates the old worker',async()=>{
 const target=await wallet(),ids=identities(target),pending=await pendingRefund(ids);await referenceBet(ids);await due(pending.transactionId);
 const scheduler=new SqlPendingReferenceScheduler(orm,{leaseMs:500,jitter:()=>0});const claim=await scheduler.claimOne();expect(claim?.transactionId).toBe(pending.transactionId);
 const locker=orm.em.fork();await locker.begin();
 await locker.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE',[target.id],'all',locker.getTransactionContext());
 const old=await worker({claim,mode:'process'});old.go();await old.line();await blockedBackend(old.ready.backend);
 try{await Bun.sleep(Math.max(0,claim!.leaseUntil.getTime()-Date.now()+20));}finally{await locker.rollback();}
 const result=await old.line();await old.close();
 console.log(JSON.stringify({proof:'scheduler-expired-lock-lease',outcome:result.outcome}));
 expect(result.outcome).toEqual({status:'LOST_LEASE',transactionId:pending.transactionId});
 expect(await financialState(target.id)).toMatchObject({balance:'75.00',version:'2',ledger:2});
 expect(await new SqlPendingReferenceScheduler(orm).runOnce()).toMatchObject({status:'PROCESSED',transactionId:pending.transactionId});
},15000);

test('TTL crossed under a held wallet lock uses the time of the actual reference recheck',async()=>{
 const target=await wallet(),ids=identities(target),pending=await pendingRefund(ids);await due(pending.transactionId);
 const expiry=new Date(Date.now()+600);
 await query('UPDATE pending_reference_jobs SET expires_at=? WHERE transaction_id=?',[expiry,pending.transactionId]);
 const scheduler=new SqlPendingReferenceScheduler(orm,{leaseMs:5000,jitter:()=>0});const claim=await scheduler.claimOne();expect(claim?.transactionId).toBe(pending.transactionId);
 const locker=orm.em.fork();await locker.begin();
 await locker.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE',[target.id],'all',locker.getTransactionContext());
 const waiting=await worker({claim,mode:'process'});waiting.go();await waiting.line();await blockedBackend(waiting.ready.backend);
 try{await Bun.sleep(Math.max(0,expiry.getTime()-Date.now()+20));}finally{await locker.rollback();}
 const {outcome}=await waiting.line();await waiting.close();
 expect(outcome).toEqual({status:'REJECTED',transactionId:pending.transactionId,failureCode:'REFERENCE_EXPIRED'});
 expect(await job(pending.transactionId)).toBeUndefined();expect(await eventCount(pending.transactionId,'WagerTransactionRejected')).toBe(1);
 expect(await financialState(target.id)).toMatchObject({balance:'100.00',version:'1',ledger:1});
 console.log(JSON.stringify({proof:'scheduler-TTL-after-lock',outcome}));
},15000);

test('replaying a pending transaction never increments attempts and migration backfill preserves its original expiry',async()=>{
 const target=await wallet(),ids=identities(target),pending=await pendingRefund(ids);
 // Estado financeiro real permanece no schema antigo; só os jobs precisam ser reconstruídos.
 await orm.migrator.down({to:'Migration202610110004'});
 const [stored]=await query('SELECT response_snapshot FROM wager_transactions WHERE id=?',[pending.transactionId]);
 const expected=(stored!['response_snapshot'] as any).referenceExpiresAt;
 await orm.migrator.up();
 const initial=await job(pending.transactionId);expect(new Date(initial!['expires_at'] as string).toISOString()).toBe(expected);
 for(let i=0;i<5;i++)expect((await processWager.execute(pending.input,pending.key)).body).toMatchObject({status:'PENDING_REFERENCE',idempotentReplay:true});
 expect((await job(pending.transactionId))!['attempts']).toBe(0);expect(await eventCount(pending.transactionId,'WagerTransactionPendingReference')).toBe(1);
 await orm.migrator.up();expect(await job(pending.transactionId)).toEqual(initial);
 await referenceBet(ids);await due(pending.transactionId);
 expect(await new SqlPendingReferenceScheduler(orm).runOnce()).toMatchObject({status:'PROCESSED',transactionId:pending.transactionId});
});

test('PostgreSQL restart retains a pending job and reclaims the killed process lease',async()=>{
 const target=await wallet(),ids=identities(target),pending=await pendingRefund(ids);await referenceBet(ids);await due(pending.transactionId);
 const old=await worker({mode:'claim-barrier',leaseMs:200});old.go();const {claimed}=await old.line();
 old.child.kill('SIGKILL');await old.close();
 const container=Bun.env['JWP_SCHEDULER_POSTGRES_CONTAINER']!;
 expect(container.startsWith('jwp-scheduler-')).toBe(true);
 try{expect(Bun.spawnSync(['docker','kill','--signal=KILL',container]).exitCode).toBe(0);}
 finally{
  expect(Bun.spawnSync(['docker','start',container]).exitCode).toBe(0);
  let ready=false;for(let i=0;i<100;i++){if(Bun.spawnSync(['docker','exec',container,'pg_isready','-U','jwp']).exitCode===0){ready=true;break;}await Bun.sleep(100);}expect(ready).toBe(true);
 }
 await Bun.sleep(Math.max(0,new Date(claimed.leaseUntil).getTime()-Date.now()+10));
 const fresh=await openFinancialOrm(Bun.env['JWP_SCHEDULER_DATABASE_URL']!);
 try{expect(await new SqlPendingReferenceScheduler(fresh).runOnce()).toMatchObject({status:'PROCESSED',transactionId:pending.transactionId});}
 finally{await fresh.close(true);}
 expect(await financialState(target.id)).toMatchObject({balance:'100.00',version:'3',ledger:3});
 expect(await eventCount(pending.transactionId,'WagerTransactionProcessed')).toBe(1);
 console.log(JSON.stringify({proof:'scheduler-PostgreSQL-restart',oldPid:old.ready.pid,transactionId:pending.transactionId}));
},20000);

test('lease expiry during terminal writes rolls back wallet, ledger, snapshot and outbox together',async()=>{
 const target=await wallet(),ids=identities(target),pending=await pendingRefund(ids);await referenceBet(ids);await due(pending.transactionId);
 const before=await financialState(target.id);
 // Barreira SQL em persistTerminal, após escrever wallet e ledger.
 await query(`CREATE FUNCTION jwp_audit_delay_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.transaction_id='${pending.transactionId}'::uuid AND NEW.event_type='WagerTransactionProcessed' THEN PERFORM pg_sleep(0.4); END IF; RETURN NEW; END $$;
 CREATE TRIGGER jwp_audit_delay_terminal BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION jwp_audit_delay_terminal();`);
 const scheduler=new SqlPendingReferenceScheduler(orm,{leaseMs:250,jitter:()=>0});
 try{
  expect(await scheduler.runOnce()).toMatchObject({status:'LOST_LEASE',transactionId:pending.transactionId});
  expect(await financialState(target.id)).toEqual(before);
  expect((await query('SELECT status FROM wager_transactions WHERE id=?',[pending.transactionId]))[0]).toEqual({status:'PENDING_REFERENCE'});
  expect(await eventCount(pending.transactionId,'WagerTransactionProcessed')).toBe(0);
  expect(await eventCount(pending.transactionId,'WalletBalanceChanged')).toBe(0);
  expect(await job(pending.transactionId)).toBeDefined();
 }finally{await query('DROP TRIGGER jwp_audit_delay_terminal ON outbox_events; DROP FUNCTION jwp_audit_delay_terminal()');}
 expect(await new SqlPendingReferenceScheduler(orm).runOnce()).toMatchObject({status:'PROCESSED',transactionId:pending.transactionId});
 expect(await financialState(target.id)).toMatchObject({balance:'100.00',version:'3',ledger:3});
 console.log(JSON.stringify({proof:'scheduler-expired-terminal-rollback',before,after:await financialState(target.id)}));
},15000);

test('backoff and attempts persist exponentially with bounded jitter and survive new scheduler instances',async()=>{
 const target=await wallet(),ids=identities(target),pending=await pendingRefund(ids);
 let at=new Date((await job(pending.transactionId))!['next_attempt_at'] as string);
 const observed:number[]=[];
 for(let i=1;i<=8;i++){
  const scheduler=new SqlPendingReferenceScheduler(orm,{jitter:()=>0.5});
  expect(await scheduler.runOnce(at)).toMatchObject({status:'RESCHEDULED',transactionId:pending.transactionId});
  const state=await job(pending.transactionId),next=new Date(state!['next_attempt_at'] as string);
  expect(state!['attempts']).toBe(i);
  const wait=next.getTime()-at.getTime();observed.push(wait);expect(wait).toBe(Math.min(60000,1000*2**(i-1)+500));at=next;
 }
 await referenceBet(ids);expect(await new SqlPendingReferenceScheduler(orm).runOnce(at)).toMatchObject({status:'PROCESSED',transactionId:pending.transactionId});
 console.log(JSON.stringify({proof:'scheduler-persisted-backoff',observed}));
});

test('competing pending reversals are rechecked under wallet lock and only one gets a financial effect',async()=>{
 const target=await wallet(),ids=identities(target),first=await pendingRefund(ids);
 const second=await pendingRefund({...ids,refundExternalTransactionId:'second-'+randomUUID()});
 await referenceBet(ids);await due(first.transactionId,second.transactionId);
 const results=await Promise.all([new SqlPendingReferenceScheduler(orm).runOnce(),new SqlPendingReferenceScheduler(orm).runOnce()]);
 expect(results.map(x=>'transactionId' in x ? x.transactionId : null).sort()).toEqual([first.transactionId,second.transactionId].sort());
 expect(results.filter(x=>x.status==='PROCESSED')).toHaveLength(1);
 expect(results.filter(x=>x.status==='REJECTED')).toEqual([expect.objectContaining({failureCode:'ALREADY_REVERSED'})]);
 expect(await financialState(target.id)).toMatchObject({balance:'100.00',version:'3',ledger:3});
 const final=await query("SELECT sum(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END)::text calculated FROM ledger_entries WHERE wallet_id=?",[target.id]);
 expect(final[0]!['calculated']).toBe('100.00');
});

test('pending REFUND and ROLLBACK of the same BET: one reversal applies, the other is ALREADY_REVERSED',async()=>{
 const target=await wallet(),ids=identities(target),refund=await pendingRefund(ids);
 const rollback=await pendingRefund({...ids,refundExternalTransactionId:'rollback-'+randomUUID()},'ROLLBACK');
 await referenceBet(ids);
 await due(refund.transactionId,rollback.transactionId);
 const results=await Promise.all([new SqlPendingReferenceScheduler(orm).runOnce(),new SqlPendingReferenceScheduler(orm).runOnce()]);
 expect(results.map(x=>'transactionId' in x ? x.transactionId : null).sort()).toEqual([refund.transactionId,rollback.transactionId].sort());
 expect(results.filter(x=>x.status==='PROCESSED')).toHaveLength(1);
 expect(results.filter(x=>x.status==='REJECTED')).toEqual([expect.objectContaining({failureCode:'ALREADY_REVERSED'})]);
 expect(await financialState(target.id)).toMatchObject({balance:'100.00',version:'3',ledger:3});
});
