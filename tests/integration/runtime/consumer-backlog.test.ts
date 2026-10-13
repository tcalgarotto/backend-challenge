import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { SendMessageBatchCommand, SendMessageCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { ApplicationWorkers } from '../../../src/runtime/workers.js';
import { Observability } from '../../../src/observability/observability.js';
import { JsonLogger } from '../../../src/observability/log.js';
import { isolatedPair, requireSqs, sqsClient, type QueuePair } from '../sqs/support/sqs.js';

const rounds = Number(process.env['JWP_CONSUMER_BENCH_ROUNDS'] ?? '1');
const count = Number(process.env['JWP_CONSUMER_BENCH_MESSAGES'] ?? '200');
if (!Number.isSafeInteger(rounds) || rounds < 1 || rounds > 10 || !Number.isSafeInteger(count) || count < 100 || count > 2000 || count % 20 !== 0)
  throw new Error('Diagnostic requires 1..10 rounds and 100..2000 messages divisible by 20.');


const workerModulePath = process.env['JWP_CONSUMER_WORKERS_MODULE'];
const Workers = workerModulePath
  ? (await import(workerModulePath)).ApplicationWorkers as typeof ApplicationWorkers
  : ApplicationWorkers;
const client = sqsClient();
let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
const pairs: QueuePair[] = [];
let workers: ApplicationWorkers | undefined;
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<Record<string, unknown>[]>(sql, args);
async function until(check: () => Promise<boolean> | boolean, timeoutMs = 120000) {
  const deadline = performance.now() + timeoutMs;
  while (!await check()) {
    if (performance.now() >= deadline) throw new Error('Consumer diagnostic deadline exceeded.');
    await Bun.sleep(10);
  }
}
beforeAll(async () => {
  const url = process.env['JWP_CONSUMER_DATABASE_URL'];
  if (!url) throw new Error('JWP_CONSUMER_DATABASE_URL requires a dedicated empty PG database; no skip.');
  await requireSqs(client);
  orm = await openFinancialOrm(url);
  expect((await q("SELECT to_regclass('public.wallets') AS relation"))[0]?.['relation']).toBeNull();
  await orm.migrator.up();
},30000);
afterAll(async () => {
  try { await workers?.stop(); } finally {
    try { for (const pair of pairs) await pair.dispose(); } finally {
      client.destroy();
      if (orm) { try { await orm.migrator.down({to:0}); } finally { await orm.close(true); } }
    }
  }
});

test('real worker loop preserves FIFO, durable inbox, ACK and duplicate safety while draining backlog',async () => {
  const results: {round:number;messages:number;elapsedMs:number;messagesPerSecond:number}[] = [];
  for (let round = 1; round <= rounds; round++) {
    const commands = await isolatedPair(client,'wager-transactions.fifo'); pairs.push(commands);
    const events = await isolatedPair(client,'wager-events.fifo'); pairs.push(events);
    const open = new OpenWallet(new SqlWalletOpeningStore(orm));
    const wallets: {id:string;playerId:string}[] = [];
    for (let i=0;i<count/20;i++) wallets.push(await open.execute({playerId:randomUUID(),initialBalance:{amount:'100.00',currency:'BRL'}}));
    const messages = Array.from({length:count},(_,i) => {
      const wallet = wallets[Math.floor(i/20)]!;
      const id = randomUUID();
      return {messageId:id,type:'WagerTransactionRequested',occurredAt:new Date().toISOString(),data:{
        providerId:'consumer-diagnostic',externalTransactionId:id,idempotencyKey:id,playerId:wallet.playerId,walletId:wallet.id,
        roundId:'diagnostic',gameId:'diagnostic',kind:i%2===0?'BET':'WIN',money:{amount:'1.00',currency:'BRL'},
      }};
    });
    for (let i=0;i<messages.length;i+=10) {
      const sent = await client.send(new SendMessageBatchCommand({QueueUrl:commands.url,Entries:messages.slice(i,i+10).map((message,j)=>({
        Id:String(j),MessageGroupId:message.data.walletId,MessageDeduplicationId:message.messageId,MessageBody:JSON.stringify(message),
      }))}));
      expect(sent.Failed??[]).toHaveLength(0);
    }
    let acknowledged=0,retries=0,permanent=0,lastAck=0;
    const observation = new Observability({logger:new JsonLogger(line=>{
      const record=JSON.parse(line);
      if(record.type==='sqs.message') {
        if(record.disposition==='ACKNOWLEDGED'){acknowledged++;lastAck=performance.now();}
        else if(record.disposition==='RETRY')retries++;
        else if(record.disposition==='PERMANENT')permanent++;
      }
    })});
    workers=new Workers(orm,client,{commandsUrl:commands.url,eventsUrl:events.url,intervalMs:250},observation);
    const start=performance.now();workers.start();
    await until(()=>acknowledged>=count);
    const elapsedMs=lastAck-start;
    results.push({round,messages:count,elapsedMs,messagesPerSecond:count/(elapsedMs/1000)});
    expect(acknowledged).toBe(count);expect(retries).toBe(0);expect(permanent).toBe(0);
    for(const wallet of wallets) {
      expect((await q('SELECT balance::text AS balance,version::text AS version FROM wallets WHERE id=?',[wallet.id]))[0])
        .toEqual({balance:'100.00',version:'21'});
      const ledger=await q(`SELECT t.external_transaction_id FROM ledger_entries l JOIN wager_transactions t ON t.id=l.transaction_id
        WHERE l.wallet_id=? AND t.kind<>'OPENING' ORDER BY l.wallet_version`,[wallet.id]);
      expect(ledger.map(row=>row['external_transaction_id'])).toEqual(messages.filter(m=>m.data.walletId===wallet.id).map(m=>m.data.externalTransactionId));
      expect((await q('SELECT count(*)::int AS n FROM ledger_entries WHERE wallet_id=?',[wallet.id]))[0]?.['n']).toBe(21);
    }
    const ids=messages.map(m=>m.messageId);
    expect((await q('SELECT count(*)::int AS n FROM wager_message_inbox WHERE message_id=ANY(?::text[]) AND processed_at IS NOT NULL',[`{${ids.join(',')}}`]))[0]?.['n']).toBe(count);
    // Dedup ID novo força redelivery lógica; a inbox, não a deduplicação FIFO, deve absorvê-la.
    const duplicate=messages[0]!;
    await client.send(new SendMessageCommand({QueueUrl:commands.url,MessageGroupId:duplicate.data.walletId,
      MessageDeduplicationId:randomUUID(),MessageBody:JSON.stringify(duplicate)}));
    await until(()=>acknowledged===count+1);
    expect((await q('SELECT count(*)::int AS n FROM ledger_entries WHERE wallet_id=?',[duplicate.data.walletId]))[0]?.['n']).toBe(21);
    expect((await q('SELECT count(*)::int AS n FROM wager_message_inbox WHERE message_id=?',[duplicate.messageId]))[0]?.['n']).toBe(1);
    const walletIds=wallets.map(w=>w.id);
    // Drena o publisher fora da janela medida do consumer, evitando misturar os tempos.
    await until(async()=>Number((await q('SELECT count(*)::int AS n FROM outbox_events WHERE wallet_id=ANY(?::uuid[]) AND published_at IS NULL',[`{${walletIds.join(',')}}`]))[0]?.['n'])===0);
    await workers.stop();workers=undefined;
    expect((await client.send(new ReceiveMessageCommand({QueueUrl:commands.url,WaitTimeSeconds:0}))).Messages??[]).toHaveLength(0);
    expect((await client.send(new ReceiveMessageCommand({QueueUrl:commands.dlqUrl,WaitTimeSeconds:0}))).Messages??[]).toHaveLength(0);
  }
  const ordered=results.map(r=>r.messagesPerSecond).sort((a,b)=>a-b);
  console.log(JSON.stringify({proof:'consumer-backlog-diagnostic',label:process.env['JWP_CONSUMER_BENCH_LABEL']??'regression',
    rounds:results,medianMessagesPerSecond:ordered[Math.floor(ordered.length/2)],intervalMs:250,
    producer:'same fixed 20 alternating BET/WIN per wallet; source scripts/load unchanged',
    workersSha256:createHash('sha256').update(readFileSync(workerModulePath ?? new URL('../../../src/runtime/workers.ts',import.meta.url))).digest('hex')}));
},360000);
