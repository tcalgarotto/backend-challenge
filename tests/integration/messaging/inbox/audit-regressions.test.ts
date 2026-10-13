import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SetQueueAttributesCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../../src/persistence/financial-orm.js';
import { ProcessInboxWager, SqlInboxProcessWagerStore } from '../../../../src/messaging/sql-inbox-process-wager-store.js';
import { SqsWagerConsumer } from '../../../../src/messaging/sqs-wager-consumer.js';
import { sendWagerTransactionRequested, type WagerTransactionRequested } from '../../../../src/messaging/wager-message.js';
import { OpenWallet } from '../../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { ProcessWager } from '../../../../src/wagering/application/process-wager.js';
import { WagerTransactionStatus } from '../../../../src/wagering/domain/wager-transaction.js';
import { SqlProcessWagerStore } from '../../../../src/wagering/infrastructure/sql-process-wager-store.js';
import { isolatedPair, requireSqs, sqsClient, type QueuePair } from '../../sqs/support/sqs.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let openWallet: OpenWallet;
let client: SQSClient;
let migrated = false;
const pairs: QueuePair[] = [];
const query = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute(sql, args);

beforeAll(async () => {
  const databaseUrl = process.env['JWP_MESSAGING_DATABASE_URL'];
  const container = process.env['JWP_MESSAGING_POSTGRES_CONTAINER'];
  if (!databaseUrl || !container?.startsWith('jwp-inbox-')) {
    throw new Error('Dedicated JWP_MESSAGING_DATABASE_URL and JWP_MESSAGING_POSTGRES_CONTAINER (jwp-inbox-*) required.');
  }
  client = sqsClient();
  await requireSqs(client);
  orm = await openFinancialOrm(databaseUrl);
  expect((await query("SELECT to_regclass('public.wallets') relation"))[0]!['relation']).toBeNull();
  await orm.migrator.up();
  migrated = true;
  openWallet = new OpenWallet(new SqlWalletOpeningStore(orm));
});

afterAll(async () => {
  await Promise.all(pairs.map(pair => pair.dispose()));
  client?.destroy();
  if (orm) {
    try {
      if (migrated) await orm.migrator.down({ to: 0 });
    } finally {
      await orm.close(true);
    }
  }
});

async function pair(): Promise<QueuePair> {
  const created = await isolatedPair(client, 'wager-transactions.fifo');
  pairs.push(created);
  return created;
}

async function wallet(balance = '100.00') {
  return openWallet.execute({ playerId: randomUUID(), initialBalance: { amount: balance, currency: 'BRL' } });
}

function envelope(target: { id: string; playerId: string }, id = randomUUID(), amount = '25.00'): WagerTransactionRequested {
  return {
    messageId: `msg-${id}`,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: {
      providerId: 'messaging-provider',
      externalTransactionId: `tx-${id}`,
      idempotencyKey: `key-${id}`,
      playerId: target.playerId,
      walletId: target.id,
      roundId: `round-${id}`,
      gameId: 'messaging-game',
      kind: 'BET',
      money: { amount, currency: 'BRL' },
    },
  };
}

const consumer = (sqs: SQSClient, queueUrl: string) => new SqsWagerConsumer(
  sqs,
  queueUrl,
  new ProcessInboxWager(new SqlInboxProcessWagerStore(orm)),
  { waitTimeSeconds: 1, retryVisibilitySeconds: 0 },
);

async function receive(queueUrl: string, wait = 1): Promise<Message[]> {
  return (await client.send(new ReceiveMessageCommand({
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: wait,
    MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
  }))).Messages ?? [];
}

async function state(walletId: string) {
  return (await query(`SELECT balance::text,version::text,
    (SELECT count(*)::int FROM wager_transactions WHERE wallet_id=w.id) wagers,
    (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger
    FROM wallets w WHERE id=?`, [walletId]))[0];
}
async function inboxCount(messageId: string) {
  return (await query(`SELECT count(*)::int count FROM wager_message_inbox
    WHERE consumer_name='wager-transactions-consumer' AND message_id=?`, [messageId]))[0]!['count'];
}

async function pollMessage(worker: SqsWagerConsumer) {
  for (let attempt = 0; attempt < 30; attempt++) {
    const [observation] = await worker.pollOnce();
    if (observation) return observation;
    await Bun.sleep(25);
  }
  throw new Error('SQS message was not observed.');
}

async function restartDatabase(container: string) {
  Bun.spawnSync(['docker', 'start', container]);
  for (let attempt = 0; attempt < 100; attempt++) {
    if (Bun.spawnSync(['docker', 'exec', container, 'pg_isready', '-U', 'jwp']).exitCode === 0) return;
    await Bun.sleep(100);
  }
  throw new Error('PostgreSQL did not restart.');
}


test('shutdown returns all fetched but unfinished batch receipts, not only the active one',async()=>{
 const queues=await pair();const first=await wallet(),second=await wallet();
 await sendWagerTransactionRequested(client,queues.url,envelope(first));await sendWagerTransactionRequested(client,queues.url,envelope(second));
 let started!:()=>void,release!:()=>void;const began=new Promise<void>(r=>{started=r});const gate=new Promise<void>(r=>{release=r});
 let fetched=0;const inspecting={send:async(c:unknown,o?:unknown)=>{const result=await client.send(c as never,o as never);if(c instanceof ReceiveMessageCommand)fetched=(result as any).Messages?.length??0;return result;}} as unknown as SQSClient;
 const actual=new ProcessInboxWager(new SqlInboxProcessWagerStore(orm));
 const worker=new SqsWagerConsumer(inspecting,queues.url,{execute:async(i,k,m)=>{started();await gate;return actual.execute(i,k,m);}},{waitTimeSeconds:1,maxMessages:10,shutdownGraceMs:10});
 const polling=worker.pollOnce();await began;expect(fetched).toBe(2);
 try{
  expect(await worker.stop()).toBe('VISIBILITY_RETURNED');
  const returned=await receive(queues.url,0);expect(returned).toHaveLength(2);
  console.log(JSON.stringify({proof:'inbox-shutdown-whole-batch',fetched,returned:returned.length}));
 }finally{release();await polling;}
});
