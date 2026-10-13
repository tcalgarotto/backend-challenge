import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
  SetQueueAttributesCommand,
  SQSClient,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
} from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../../src/persistence/financial-orm.js';
import {
  SqlEventDeduplicator,
  parseIntegrationEvent,
  SqsEventConsumer,
} from '../../../../src/messaging/outbox/sqs-event-consumer.js';
import { SqlOutboxStore } from '../../../../src/messaging/outbox/sql-outbox-store.js';
import { SqsOutboxPublisher } from '../../../../src/messaging/outbox/sqs-outbox-publisher.js';
import { canonicalJson } from '../../../../src/wagering/application/process-wager.js';
import { OpenWallet } from '../../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import {
  isolatedPair,
  requireSqs,
  sqsClient,
  type QueuePair,
} from '../../sqs/support/sqs.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let openWallet: OpenWallet;
let client: SQSClient;
let migrated = false;
const pairs: QueuePair[] = [];
const query = (sql: string, args: unknown[] = []) =>
  orm.em.fork().getConnection().execute<Record<string, unknown>[]>(sql, args);

beforeAll(async () => {
  const databaseUrl = process.env['JWP_OUTBOX_DATABASE_URL'];
  const container = process.env['JWP_OUTBOX_POSTGRES_CONTAINER'];
  if (!databaseUrl || !container?.startsWith('jwp-outbox-')) {
    throw new Error(
      'Dedicated JWP_OUTBOX_DATABASE_URL and JWP_OUTBOX_POSTGRES_CONTAINER (jwp-outbox-*) required.',
    );
  }
  client = sqsClient();
  await requireSqs(client);
  orm = await openFinancialOrm(databaseUrl);
  expect((await query("SELECT to_regclass('public.wallets') relation"))[0]?.['relation']).toBeNull();
  await orm.migrator.up();
  migrated = true;
  openWallet = new OpenWallet(new SqlWalletOpeningStore(orm));
});

afterAll(async () => {
  await Promise.all(pairs.map(pair => pair.dispose()));
  client?.destroy();
  if (!orm) return;
  try {
    if (migrated) await orm.migrator.down({ to: 0 });
  } finally {
    await orm.close(true);
  }
});

async function pair(source = 'wager-events.fifo'): Promise<QueuePair> {
  const created = await isolatedPair(client, source);
  pairs.push(created);
  return created;
}

async function wallet() {
  return openWallet.execute({
    playerId: randomUUID(),
    initialBalance: { amount: '100.00', currency: 'BRL' },
  });
}

async function receive(queueUrl: string, wait = 1): Promise<Message[]> {
  return (await client.send(new ReceiveMessageCommand({
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: wait,
    MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId', 'MessageDeduplicationId'],
  }))).Messages ?? [];
}

async function consume(
  consumer: SqsEventConsumer,
  count: number,
): Promise<Awaited<ReturnType<SqsEventConsumer['pollOnce']>>[number][]> {
  const observations: Awaited<ReturnType<SqsEventConsumer['pollOnce']>>[number][] = [];
  for (let attempt = 0; attempt < 30 && observations.length < count; attempt++) {
    observations.push(...await consumer.pollOnce());
  }
  expect(observations).toHaveLength(count);
  return observations;
}


test('expired outbox token cannot finalize or reschedule even before a replacement claim',async()=>{
 const target=await wallet();const at=new Date();const store=new SqlOutboxStore(orm,20);const [claim]=await store.claimBatch(1,at);
 expect(claim).toBeDefined();const expiredAt=new Date(at.getTime()+21);
 expect(await store.markPublished(claim!,expiredAt)).toBe(false);
 expect(await store.reschedule(claim!,new Date(expiredAt.getTime()+1000),expiredAt)).toBe(false);
});

test('event parser rejects malformed event data before durable inbox acceptance',async()=>{
 const target=await wallet();const [row]=await query("SELECT envelope FROM outbox_events WHERE wallet_id=? AND event_type='WalletBalanceChanged'",[target.id]);
 const valid=row!['envelope'] as any;expect(parseIntegrationEvent(valid)).toEqual(valid);
 const corrupt=structuredClone(valid);corrupt.data.money.amount=25;
 expect(()=>parseIntegrationEvent(corrupt)).toThrow();
 const missing=structuredClone(valid);delete missing.data.transactionId;expect(()=>parseIntegrationEvent(missing)).toThrow();
 const badInstant=structuredClone(valid);badInstant.occurredAt='2026-10-11';expect(()=>parseIntegrationEvent(badInstant)).toThrow();
});

async function worker(options:object){
 const child=Bun.spawn(['bun','tests/integration/messaging/outbox/audit-worker.ts'],{stdin:'pipe',stdout:'pipe',stderr:'pipe',env:{...Bun.env,JWP_OUTBOX_AUDIT_OPTIONS:JSON.stringify(options)}});
 const reader=child.stdout.getReader();let buffer='';
 const line=async()=>{while(!buffer.includes('\n')){const part=await reader.read();if(part.done)throw new Error(await new Response(child.stderr).text());buffer+=new TextDecoder().decode(part.value);}const i=buffer.indexOf('\n'),text=buffer.slice(0,i);buffer=buffer.slice(i+1);return JSON.parse(text);};
 const ready=await line();return{child,line,ready,go:()=>child.stdin.write('go\n'),close:async()=>{child.stdin.end();await child.exited;reader.releaseLock();}};
}
const isolate=async(walletId:string)=>query("UPDATE outbox_events SET next_attempt_at=clock_timestamp()+interval '1 day' WHERE wallet_id<>?",[walletId]);

test('two OS publishers hold disjoint committed leases and send six real SQS events',async()=>{
 const events=await pair(),commands=await pair('wager-transactions.fifo');
 const wallets=await Promise.all([wallet(),wallet(),wallet()]);
 await query("UPDATE outbox_events SET next_attempt_at=clock_timestamp()+interval '1 day' WHERE wallet_id NOT IN (?,?,?)",wallets.map(w=>w.id));
 const processes=await Promise.all([1,2].map(()=>worker({mode:'publish',queueUrl:events.url,batchSize:3,drain:true})));
 try{
  expect(new Set([process.pid,...processes.map(p=>p.ready.pid)]).size).toBe(3);
  processes.forEach(p=>p.go());
  const results=await Promise.all(processes.map(async p=>{const sent=[];for(;;){const line=await p.line();if(line.outcome)return {sent,outcome:line.outcome};sent.push(line.sent);}}));
  expect(results.flatMap(r=>r.outcome)).toHaveLength(6);
  expect(results.flatMap(r=>r.outcome).every(r=>r.status==='PUBLISHED')).toBe(true);
  expect(new Set(results.flatMap(r=>r.sent)).size).toBe(6);
  const observations=await consume(new SqsEventConsumer(client,events.url,new SqlEventDeduplicator(orm)),6);
  expect(new Set(observations.map(x=>x.eventId)).size).toBe(6);expect(await receive(commands.url)).toEqual([]);
  console.log(JSON.stringify({proof:'outbox-two-OS-publishers',pids:processes.map(p=>p.ready.pid),published:6}));
 }finally{await Promise.all(processes.map(p=>p.close()));}
},20000);

test('SIGKILL after confirmed SendMessage recovers stable identity and consumer crash before ack deduplicates durably',async()=>{
 const events=await pair(),target=await wallet();await isolate(target.id);
 const [row]=await query('SELECT event_id FROM outbox_events WHERE wallet_id=? ORDER BY seq LIMIT 1',[target.id]);
 const eventId=row!['event_id'] as string;
 await query("UPDATE outbox_events SET next_attempt_at=clock_timestamp()+interval '1 day' WHERE wallet_id=? AND event_id<>?",[target.id,eventId]);
 const crashed=await worker({mode:'send-barrier',queueUrl:events.url,batchSize:1,leaseMs:300});crashed.go();const sent=await crashed.line();
 expect(sent.sent).toBe(eventId);expect((await query('SELECT published_at FROM outbox_events WHERE event_id=?',[eventId]))[0]!['published_at']).toBeNull();
 crashed.child.kill('SIGKILL');await crashed.close();
 const [{lease_until}]=await query('SELECT lease_until FROM outbox_events WHERE event_id=?',[eventId]) as any[];
 await Bun.sleep(Math.max(0,new Date(lease_until).getTime()-Date.now()+10));
 expect(await new SqsOutboxPublisher(client,events.url,new SqlOutboxStore(orm),{batchSize:1}).publishBatch()).toEqual([{eventId,status:'PUBLISHED'}]);
 const beforeAck=await worker({mode:'ack-barrier',queueUrl:events.url});beforeAck.go();const ack=await beforeAck.line();
 expect(ack.ackBoundary).toBe(true);expect((await query('SELECT count(*)::int n FROM wager_event_inbox WHERE event_id=?',[eventId]))[0]!['n']).toBe(1);
 beforeAck.child.kill('SIGKILL');await beforeAck.close();
 await client.send(new ChangeMessageVisibilityCommand({QueueUrl:events.url,ReceiptHandle:ack.receipt,VisibilityTimeout:0}));
 const second=await worker({mode:'consume',queueUrl:events.url});second.go();const outcome=await second.line();await second.close();
 expect(outcome.outcome).toEqual([expect.objectContaining({eventId,disposition:'ACKNOWLEDGED',duplicate:true})]);
 expect((await query('SELECT attempts FROM outbox_events WHERE event_id=?',[eventId]))[0]!['attempts']).toBe(2);
 console.log(JSON.stringify({proof:'outbox-and-consumer-real-SIGKILL',killedPids:[crashed.ready.pid,beforeAck.ready.pid],consumerPid:second.ready.pid,eventId}));
},20000);

test('poison event bodies and divergent eventId go to DLQ without ack or an extra inbox row',async()=>{
 const events=await pair(),target=await wallet();
 await client.send(new SetQueueAttributesCommand({QueueUrl:events.url,Attributes:{VisibilityTimeout:'0'}}));
 const [row]=await query('SELECT envelope FROM outbox_events WHERE wallet_id=? ORDER BY event_id LIMIT 1',[target.id]);
 const original=row!['envelope'] as any;
 await new SqlEventDeduplicator(orm).accept(original);
 const invalid=structuredClone(original);invalid.eventId=randomUUID();invalid.data.money.amount=25;
 const divergent=structuredClone(original);divergent.correlationId='changed-correlation';
 for(const body of [invalid,divergent]){
  await client.send(new SendMessageCommand({QueueUrl:events.url,MessageBody:JSON.stringify(body),MessageGroupId:target.id,MessageDeduplicationId:randomUUID()}));
  const consumer=new SqsEventConsumer(client,events.url,new SqlEventDeduplicator(orm),{waitTimeSeconds:0,maxMessages:1});
  const observations=[];for(let i=0;i<7;i++)observations.push(...await consumer.pollOnce());
  expect(observations).toHaveLength(5);
  expect(observations.every(x=>x.disposition==='PERMANENT')).toBe(true);
  const dlq=await receive(events.dlqUrl);expect(dlq).toHaveLength(1);
  expect(JSON.parse(dlq[0]!.Body!).eventId).toBe(body.eventId);
  await client.send(new DeleteMessageCommand({QueueUrl:events.dlqUrl,ReceiptHandle:dlq[0]!.ReceiptHandle!}));
 }
 expect((await query('SELECT count(*)::int n FROM wager_event_inbox WHERE event_id IN (?,?)',[original.eventId,invalid.eventId]))[0]!['n']).toBe(1);
});

test('event inbox SQL lock exhaustion remains retryable with a bounded transaction policy',async()=>{
 const target=await wallet();const [row]=await query('SELECT envelope FROM outbox_events WHERE wallet_id=? LIMIT 1',[target.id]);const event=row!['envelope'] as any;
 const blocker=orm.em.fork();await blocker.begin();
 await blocker.getConnection().execute('INSERT INTO wager_event_inbox(consumer_name,event_id,envelope_hash) VALUES (?,?,?)',['wager-events-demo-consumer',event.eventId,'a'.repeat(64)],'all',blocker.getTransactionContext());
 const limited=new SqlEventDeduplicator(orm,{lockTimeoutMs:50,statementTimeoutMs:200,transactionTimeoutMs:300,budgetMs:1000,maxAttempts:2,retryBaseMs:5});
 try{await expect(limited.accept(event)).rejects.toMatchObject({code:'INFRASTRUCTURE_UNAVAILABLE'});}
 finally{await blocker.rollback();}
 expect(await limited.accept(event)).toEqual({duplicate:false});
});

test('real network refusal reschedules durable events and retry preserves the full envelope',async()=>{
 const events=await pair(),target=await wallet();await isolate(target.id);
 const before=await query('SELECT event_id,envelope FROM outbox_events WHERE wallet_id=? ORDER BY event_id',[target.id]);
 const broken=new SQSClient({endpoint:'http://127.0.0.1:1',useQueueUrlAsEndpoint:false,region:'us-east-1',credentials:{accessKeyId:'test',secretAccessKey:'test'},maxAttempts:1});
 try{
  const result=await new SqsOutboxPublisher(broken,events.url,new SqlOutboxStore(orm),{batchSize:2,retryBaseMs:50,retryMaxMs:100,sendTimeoutMs:500}).publishBatch();

  expect(result.map(x=>x.status)).toEqual(['RETRY','DEFERRED']);
 }finally{broken.destroy();}
 const failed=await query('SELECT event_id,envelope,attempts,published_at,claim_token FROM outbox_events WHERE wallet_id=? ORDER BY event_id',[target.id]);
 expect(failed.map(x=>({event_id:x['event_id'],envelope:x['envelope']}))).toEqual(before.map(x=>({event_id:x['event_id'],envelope:x['envelope']})));
 expect(failed.every(x=>x['published_at']===null&&x['claim_token']===null)).toBe(true);
 expect(failed.map(x=>x['attempts']).sort()).toEqual([0,1]);
 expect(new Set((await query('SELECT next_attempt_at FROM outbox_events WHERE wallet_id=?',[target.id])).map(x=>new Date(x['next_attempt_at'] as string).getTime())).size).toBe(1);
 await Bun.sleep(60);
 const recovered=await new SqsOutboxPublisher(client,events.url,new SqlOutboxStore(orm),{batchSize:2}).publishBatch();
 expect(recovered.every(x=>x.status==='PUBLISHED')).toBe(true);
 const consumed=await consume(new SqsEventConsumer(client,events.url,new SqlEventDeduplicator(orm)),2);
 expect(new Set(consumed.map(x=>x.eventId))).toEqual(new Set(before.map(x=>x['event_id'] as string)));
 console.log(JSON.stringify({proof:'outbox-real-network-refusal-retry',events:before.map(x=>x['event_id']),attempts:2}));
});

test('production event parser matches every versioned valid and invalid contract example',async()=>{
 const examples=await Bun.file('contracts/messaging/examples/events.json').json();
 for(const example of examples.valid)expect(parseIntegrationEvent(example.message)).toEqual(example.message);
 for(const example of examples.invalid){
  const original=examples.valid.find((v:any)=>v.name===example.base);const body=structuredClone(original.message);
  for(const [path,value]of Object.entries(example.patch)){const keys=path.split('.');const last=keys.pop()!;let target=body;for(const key of keys)target=target[key];if(value===null)delete target[last];else target[last]=value;}
  expect(()=>parseIntegrationEvent(body)).toThrow();
 }
 console.log(JSON.stringify({proof:'production-event-contract-matrix',valid:examples.valid.length,invalid:examples.invalid.length}));
});

test('a connected HTTP endpoint that never responds cannot hold publication beyond its send timeout',async()=>{
 const target=await wallet();await isolate(target.id);
 let requests=0;
 const hanging=Bun.serve({hostname:'127.0.0.1',port:0,fetch(){requests++;return new Promise<Response>(()=>{});}});
 const sqs=new SQSClient({endpoint:`http://127.0.0.1:${hanging.port}`,useQueueUrlAsEndpoint:false,region:'us-east-1',credentials:{accessKeyId:'test',secretAccessKey:'test'},maxAttempts:1});
 try{
  const started=Date.now();
  const results=await new SqsOutboxPublisher(sqs,`http://127.0.0.1:${hanging.port}/events`,new SqlOutboxStore(orm),{batchSize:2,sendTimeoutMs:100}).publishBatch();
  expect(results.map(r=>r.status)).toEqual(['RETRY','DEFERRED']);expect(requests).toBe(1);expect(Date.now()-started).toBeLessThan(2000);
  console.log(JSON.stringify({proof:'outbox-real-connected-send-timeout',requests,elapsedMs:Date.now()-started}));
 }finally{sqs.destroy();await hanging.stop(true);}
});

test('SIGKILL before send and after published commit respectively recovers and avoids another send',async()=>{
 const events=await pair(),target=await wallet();await isolate(target.id);
 const [{event_id:eventId}]=await query('SELECT event_id FROM outbox_events WHERE wallet_id=? ORDER BY seq LIMIT 1',[target.id]) as any[];
 await query("UPDATE outbox_events SET next_attempt_at=clock_timestamp()+interval '1 day' WHERE wallet_id=? AND event_id<>?",[target.id,eventId]);
 const beforeSend=await worker({mode:'claim-barrier',queueUrl:events.url,batchSize:1,leaseMs:2000});beforeSend.go();const {claimed}=await beforeSend.line();
 expect(claimed[0].eventId).toBe(eventId);expect(await receive(events.url)).toEqual([]);
 beforeSend.child.kill('SIGKILL');await beforeSend.close();
 await Bun.sleep(Math.max(0,new Date(claimed[0].leaseUntil).getTime()-Date.now()+10));
 const afterCommit=await worker({mode:'commit-barrier',queueUrl:events.url,batchSize:1});afterCommit.go();expect((await afterCommit.line()).sent).toBe(eventId);const {outcome}=await afterCommit.line();
 expect(outcome).toEqual([{eventId,status:'PUBLISHED'}]);
 afterCommit.child.kill('SIGKILL');await afterCommit.close();
 expect(await new SqsOutboxPublisher(client,events.url,new SqlOutboxStore(orm)).publishBatch()).toEqual([]);
 expect((await query('SELECT attempts,published_at IS NOT NULL published FROM outbox_events WHERE event_id=?',[eventId]))[0]).toEqual({attempts:2,published:true});
 console.log(JSON.stringify({proof:'outbox-SIGKILL-before-send-after-publication',killedPids:[beforeSend.ready.pid,afterCommit.ready.pid],eventId}));
},20000);
