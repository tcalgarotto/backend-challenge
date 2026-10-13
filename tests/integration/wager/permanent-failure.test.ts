import { assertWalletLedgerInvariant } from '../support/wallet-invariant.js';
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { GetQueueAttributesCommand, SetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { SqlPendingReferenceScheduler } from '../../../src/messaging/pending-reference-scheduler.js';
import { sendWagerTransactionRequested } from '../../../src/messaging/wager-message.js';
import { SqsEventConsumer, SqlEventDeduplicator } from '../../../src/messaging/outbox/sqs-event-consumer.js';
import { isolatedPair, requireSqs, sqsClient } from '../sqs/support/sqs.js';
import { Child, startApi, call, waitUntil, type ApiInstance } from '../../matrix/delivery/support.js';
let orm: Awaited<ReturnType<typeof openFinancialOrm>>, runtime: typeof orm, api: ApiInstance, runtimeUrl: string;
const role=`jwp_failure_${randomUUID().replaceAll('-','')}`;
const children: Child[]=[];
const client=sqsClient();
const q=(sql:string,args:unknown[]=[])=>orm.em.fork().getConnection().execute<any[]>(sql,args);
const command=(w:{id:string;playerId:string},kind='BET',reference?:string)=>({providerId:'permanent-provider',externalTransactionId:randomUUID(),
  walletId:w.id,playerId:w.playerId,roundId:'permanent-round',gameId:'game',kind,money:{amount:'10.00',currency:'BRL'},
  ...(reference?{referenceExternalTransactionId:reference}:{})});
const post=async(body:any,key=body.externalTransactionId)=>{
  const response=await fetch(api.base+'/wagering/transactions',{method:'POST',
    headers:{'Content-Type':'application/json','Idempotency-Key':key},body:JSON.stringify(body)});
  return {status:response.status,body:await response.json() as any};
};
async function wallet(){const result=await call(api,'POST','/wallets',{playerId:randomUUID(),initialBalance:{amount:'100.00',currency:'BRL'}});expect(result.status).toBe(201);return result.body;}
async function state(id:string){const [row]=await q(`SELECT balance::text,version::text,
  (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger FROM wallets w WHERE id=?`,[id]);return row;}
async function featureFailure(walletId:string,sqlState='0A000'){
  const name=`jwp_fault_${randomUUID().replaceAll('-','')}`;
  await q(`CREATE SEQUENCE ${name}; GRANT USAGE ON SEQUENCE ${name} TO jwp_runtime;
    CREATE FUNCTION ${name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      PERFORM nextval('${name}'); RAISE EXCEPTION 'controlled SQL feature failure' USING ERRCODE='${sqlState}'; END $$;
    CREATE TRIGGER ${name} BEFORE UPDATE ON wallets FOR EACH ROW WHEN (OLD.id='${walletId}'::uuid) EXECUTE FUNCTION ${name}()`);
  return {calls:async()=>Number((await q(`SELECT last_value FROM ${name}`))[0].last_value),
    close:()=>q(`DROP TRIGGER ${name} ON wallets; DROP FUNCTION ${name}(); DROP SEQUENCE ${name}`)};
}
async function failedRow(external:string){return (await q('SELECT * FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',['permanent-provider',external]))[0];}
async function failedEvents(id:string){return q("SELECT event_id,envelope,published_at FROM outbox_events WHERE transaction_id=? AND event_type='WagerTransactionFailed'",[id]);}
beforeAll(async()=>{
  const owner=process.env['JWP_WAGER_DATABASE_URL'],container=process.env['JWP_WAGER_POSTGRES_CONTAINER'];
  if(!owner||!container?.startsWith('jwp-wagerfix-'))throw new Error('Dedicated JWP_WAGER_DATABASE_URL and JWP_WAGER_POSTGRES_CONTAINER required; permanent failure tests never skip.');
  orm=await openFinancialOrm(owner);await orm.migrator.up();
  await q(`CREATE ROLE ${role} LOGIN PASSWORD 'audit_test_only'; GRANT jwp_runtime TO ${role}`);
  const url=new URL(owner);url.username=role;url.password='audit_test_only';runtimeUrl=url.toString();runtime=await openFinancialOrm(runtimeUrl);
  await requireSqs(client);
  api=await startApi('failed-http',{APP_ROLES:'http',JWP_DATABASE_URL:runtimeUrl,JWP_SQS_QUEUE_URLS:''});children.push(api.child);
},30000);
afterAll(async()=>{
  for(const child of children)await child.stop();client.destroy();await runtime?.close(true);
  if(orm){await orm.migrator.down({to:0});await q(`DROP ROLE IF EXISTS ${role}`);await orm.close(true);}
},30000);

test('compiled HTTP records FAILED only on final known attempt, with immutable replay after later movement',async()=>{
  const w=await wallet(),body=command(w),fault=await featureFailure(w.id);
  let result:any;
  try{
    result=await post(body);expect(result.status).toBe(500);expect(result.body).toMatchObject({status:'FAILED',failureCode:'PERMANENT_INFRASTRUCTURE_FAILURE',error:{retryable:false}});
    expect(await fault.calls()).toBe(3);expect(await state(w.id)).toEqual({balance:'100.00',version:'1',ledger:1});
    const row=await failedRow(body.externalTransactionId);expect(row.status).toBe('FAILED');expect(row.processed_at).toBeNull();
    expect((await failedEvents(row.id))).toHaveLength(1);
    const event=(await failedEvents(row.id))[0].envelope;expect(event.data).toMatchObject({attempts:3,sqlState:'0A000',diagnosticCode:'SQL_FEATURE_NOT_SUPPORTED'});
    expect((await call(api,'GET',`/wagering/transactions/${row.id}`)).status).toBe(200);
  }finally{await fault.close();}
  expect((await post(command(w,'WIN'))).status).toBe(200);
  const replay=await post(body);expect(replay.status).toBe(500);expect(replay.body).toEqual({...result.body,idempotentReplay:true});
  expect(replay.body.balance.amount).toBe('100.00');expect((await state(w.id)).balance).toBe('110.00');
  expect((await failedEvents(result.body.transactionId))).toHaveLength(1);
  expect((await post(command(w,'REFUND',body.externalTransactionId))).body).toMatchObject({status:'REJECTED',failureCode:'REFERENCE_NOT_PROCESSED'});
  console.log(JSON.stringify({proof:'HTTP-final-known-FAILED',transactionId:result.body.transactionId,replay:replay.body,status:await state(w.id)}));
},30000);

test('SQS reads actual redrive max=3, atomically audits last delivery then ACKs; Failed event is consumable',async()=>{
  const commands=await isolatedPair(client,'wager-transactions.fifo'),events=await isolatedPair(client,'wager-events.fifo');
  const policy=await client.send(new GetQueueAttributesCommand({QueueUrl:commands.url,AttributeNames:['RedrivePolicy']}));
  await client.send(new SetQueueAttributesCommand({QueueUrl:commands.url,Attributes:{RedrivePolicy:JSON.stringify({...JSON.parse(policy.Attributes!.RedrivePolicy!),maxReceiveCount:3})}}));
  const w=await wallet(),body=command(w),messageId=randomUUID(),fault=await featureFailure(w.id);
  const child=await startApi('failed-sqs',{APP_ROLES:'http,workers',JWP_DATABASE_URL:runtimeUrl,JWP_SQS_QUEUE_URLS:'',
    JWP_SQS_COMMAND_QUEUE_URL:commands.url,JWP_SQS_EVENT_QUEUE_URL:events.url,JWP_SQS_ENDPOINT:process.env['JWP_SQS_ENDPOINT']!,
    AWS_REGION:'us-east-1',AWS_ACCESS_KEY_ID:'test',AWS_SECRET_ACCESS_KEY:'test'});children.push(child.child);
  try{
    await child.child.waitFor(l=>l['type']==='workers.started','workers started');
    await sendWagerTransactionRequested(client,commands.url,{messageId,type:'WagerTransactionRequested',occurredAt:new Date().toISOString(),data:{...body,idempotencyKey:body.externalTransactionId}});
    const row=await waitUntil(async()=>{const found=await failedRow(body.externalTransactionId);return found?.status==='FAILED'?found:undefined;},'SQS durable FAILED',30000);
    await waitUntil(async()=>child.child.of('sqs.message').some(l=>l['messageId']===messageId && l['disposition']==='ACKNOWLEDGED'),'ACK after FAILED commit',10000);
    expect(await fault.calls()).toBe(3);expect(await state(w.id)).toEqual({balance:'100.00',version:'1',ledger:1});
    const [inbox]=await q('SELECT processed_at,result_snapshot FROM wager_message_inbox WHERE message_id=?',[messageId]);
    expect(inbox.processed_at).not.toBeNull();expect(inbox.result_snapshot).toMatchObject({httpStatus:500,body:{status:'FAILED',error:{retryable:false}}});
    const observations=child.child.of('sqs.message').filter(l=>l['messageId']===messageId);expect(observations.map(l=>l['receiveCount'])).toEqual([1,2,3]);
    expect(observations[2]!['disposition']).toBe('ACKNOWLEDGED');
    const depth=await client.send(new GetQueueAttributesCommand({QueueUrl:commands.dlqUrl,AttributeNames:['ApproximateNumberOfMessages']}));expect(Number(depth.Attributes!.ApproximateNumberOfMessages)).toBe(0);
    await waitUntil(async()=>{const rows=await failedEvents(row.id);return rows[0]?.published_at?rows:undefined;},'Failed event published',10000);
    const consumer=new SqsEventConsumer(client,events.url,new SqlEventDeduplicator(runtime),{waitTimeSeconds:0});
    await waitUntil(async()=>{await consumer.pollOnce();const [seen]=await q('SELECT count(*)::int n FROM wager_event_inbox WHERE event_id=?',[(await failedEvents(row.id))[0].event_id]);return seen.n===1;},'Failed event accepted by event inbox',10000);
    console.log(JSON.stringify({proof:'SQS-final-FAILED-atomic-inbox-ACK-event',observations,row:row.id,inbox}));
  }finally{await child.child.stop();await fault.close();await commands.dispose();await events.dispose();}
},60000);

test('scheduler audits a known feature failure on third recovered claim with no credit, then removes the job',async()=>{
  const w=await wallet(),bet=command(w),refund=command(w,'REFUND',bet.externalTransactionId);
  const pending=await post(refund);expect(pending.status).toBe(202);expect((await post(bet)).status).toBe(200);
  await q("UPDATE pending_reference_jobs SET next_attempt_at=now()-interval '1 second' WHERE transaction_id=?",[pending.body.transactionId]);
  const fault=await featureFailure(w.id),scheduler=new SqlPendingReferenceScheduler(runtime,{permanentFailureAttempts:3});
  try{
    for(let attempt=1;attempt<=3;attempt++){
      const claim=await scheduler.claimOne();expect(claim).not.toBeNull();expect(claim!.attempts).toBe(attempt);
      if(attempt<3){await expect(scheduler.processClaim(claim!)).rejects.toMatchObject({code:'0A000'});
        await q("UPDATE pending_reference_jobs SET lease_until=clock_timestamp()-interval '1 second',next_attempt_at=now()-interval '1 second' WHERE transaction_id=?",[claim!.transactionId]);}
      else expect(await scheduler.processClaim(claim!)).toMatchObject({status:'FAILED',failureCode:'PERMANENT_INFRASTRUCTURE_FAILURE'});
    }
    expect(await fault.calls()).toBe(3);const row=await failedRow(refund.externalTransactionId);expect(row.status).toBe('FAILED');
    expect(await q('SELECT * FROM pending_reference_jobs WHERE transaction_id=?',[row.id])).toHaveLength(0);
    expect(await state(w.id)).toEqual({balance:'90.00',version:'2',ledger:2});expect(await failedEvents(row.id)).toHaveLength(1);
    expect((await post(refund)).body).toMatchObject({status:'FAILED',idempotentReplay:true,balance:{amount:'90.00'},error:{retryable:false}});
    console.log(JSON.stringify({proof:'scheduler-final-known-FAILED',transactionId:row.id,state:await state(w.id)}));
  }finally{await fault.close();}
},30000);

test('unknown SQL error and exhausted transient lock error never become FAILED',async()=>{
  for(const code of ['P0001','55P03']){
    const w=await wallet(),body=command(w),fault=await featureFailure(w.id,code);
    try{
      const result=await post(body);expect(result.status).toBe(code==='55P03'?503:500);
      if(code==='55P03')expect(result.body).toMatchObject({error:{retryable:true}});
      expect(await failedRow(body.externalTransactionId)).toBeUndefined();expect(await state(w.id)).toEqual({balance:'100.00',version:'1',ledger:1});
      console.log(JSON.stringify({proof:'no-arbitrary-FAILED',sqlState:code,status:result.status,calls:await fault.calls()}));
    }finally{await fault.close();}
  }
},30000);

test('SQS never ACKs or invents FAILED when the final audit itself cannot commit',async()=>{
  const commands=await isolatedPair(client,'wager-transactions.fifo'),events=await isolatedPair(client,'wager-events.fifo');
  const policy=await client.send(new GetQueueAttributesCommand({QueueUrl:commands.url,AttributeNames:['RedrivePolicy']}));
  await client.send(new SetQueueAttributesCommand({QueueUrl:commands.url,Attributes:{RedrivePolicy:JSON.stringify({...JSON.parse(policy.Attributes!.RedrivePolicy!),maxReceiveCount:2})}}));
  const w=await wallet(),body=command(w),messageId=randomUUID(),fault=await featureFailure(w.id);
  const name=`jwp_audit_fault_${randomUUID().replaceAll('-','')}`;
  await q(`CREATE FUNCTION ${name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    RAISE EXCEPTION 'controlled failure to persist audit' USING ERRCODE='P0001'; END $$;
    CREATE TRIGGER ${name} BEFORE INSERT ON wager_transactions FOR EACH ROW
      WHEN (NEW.wallet_id='${w.id}'::uuid AND NEW.status='FAILED') EXECUTE FUNCTION ${name}()`);
  const child=await startApi('failed-audit-unavailable',{APP_ROLES:'http,workers',JWP_DATABASE_URL:runtimeUrl,JWP_SQS_QUEUE_URLS:'',
    JWP_SQS_COMMAND_QUEUE_URL:commands.url,JWP_SQS_EVENT_QUEUE_URL:events.url,JWP_SQS_ENDPOINT:process.env['JWP_SQS_ENDPOINT']!,
    AWS_REGION:'us-east-1',AWS_ACCESS_KEY_ID:'test',AWS_SECRET_ACCESS_KEY:'test'});children.push(child.child);
  try{
    await child.child.waitFor(l=>l['type']==='workers.started','workers started');
    await sendWagerTransactionRequested(client,commands.url,{messageId,type:'WagerTransactionRequested',occurredAt:new Date().toISOString(),data:{...body,idempotencyKey:body.externalTransactionId}});
    await waitUntil(async()=>{
      const depth=await client.send(new GetQueueAttributesCommand({QueueUrl:commands.dlqUrl,AttributeNames:['ApproximateNumberOfMessages']}));
      return Number(depth.Attributes!.ApproximateNumberOfMessages)===1;
    },'unconfirmed audit goes to DLQ without ACK',30000);
    const observations=child.child.of('sqs.message').filter(l=>l['messageId']===messageId);
    expect(observations).toHaveLength(2);expect(observations.every(l=>l['disposition']!=='ACKNOWLEDGED')).toBe(true);
    expect(await fault.calls()).toBe(2);expect(await failedRow(body.externalTransactionId)).toBeUndefined();
    expect(await q('SELECT * FROM wager_message_inbox WHERE message_id=?',[messageId])).toHaveLength(0);
    expect(await state(w.id)).toEqual({balance:'100.00',version:'1',ledger:1});
    console.log(JSON.stringify({proof:'SQS-unconfirmed-audit-no-ACK-no-FAILED',observations,dlq:1,state:await state(w.id)}));
  }finally{
    await child.child.stop();await q(`DROP TRIGGER ${name} ON wager_transactions; DROP FUNCTION ${name}()`);
    await fault.close();await commands.dispose();await events.dispose();
  }
},60000);

test('final invariant: every wallet balance equals its real PostgreSQL ledger and is nonnegative', async()=>{
  await assertWalletLedgerInvariant(orm);
});
