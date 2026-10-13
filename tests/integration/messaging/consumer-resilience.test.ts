import { assertWalletLedgerInvariant } from '../support/wallet-invariant.js';
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { ReceiveMessageCommand, SendMessageCommand, SetQueueAttributesCommand, type Message, type SQSClient } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { ProcessInboxWager, SqlInboxProcessWagerStore, type InboxMessageMetadata } from '../../../src/messaging/sql-inbox-process-wager-store.js';
import { SqsWagerConsumer, transientRetryVisibilitySeconds, type ConsumerObservation, type WagerMessageProcessor }
  from '../../../src/messaging/sqs-wager-consumer.js';
import type { WagerTransactionRequested } from '../../../src/messaging/wager-message.js';
import { canonicalJson } from '../../../src/wagering/application/process-wager.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { isolatedPair, requireSqs, sqsClient, type QueuePair } from '../sqs/support/sqs.js';



let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let openWallet: OpenWallet;
let client: SQSClient;
let container: string;
let migrated = false;
const pairs: QueuePair[] = [];
const query = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute(sql, args);

beforeAll(async () => {
  const databaseUrl = process.env['JWP_MESSAGING_DATABASE_URL'];
  container = process.env['JWP_MESSAGING_POSTGRES_CONTAINER'] ?? '';
  if (!databaseUrl || !container.startsWith('jwp-')) {
    throw new Error('Dedicated JWP_MESSAGING_DATABASE_URL and JWP_MESSAGING_POSTGRES_CONTAINER (jwp-*) required; no skip.');
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

const wallet = () => openWallet.execute({ playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });

function envelope(target: { id: string; playerId: string }, amount: string): WagerTransactionRequested {
  const id = randomUUID();
  return {
    messageId: `msg-${id}`, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
    data: { providerId: 'resilience-provider', externalTransactionId: `tx-${id}`, idempotencyKey: `key-${id}`,
      playerId: target.playerId, walletId: target.id, roundId: `round-${id}`, gameId: 'resilience-game', kind: 'BET',
      money: { amount, currency: 'BRL' } },
  };
}

async function send(queueUrl: string, message: WagerTransactionRequested, groupId: string): Promise<void> {
  await client.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: canonicalJson(message),
    MessageGroupId: groupId, MessageDeduplicationId: message.messageId }));
}

async function receive(queueUrl: string, wait = 1): Promise<Message[]> {
  return (await client.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: wait }))).Messages ?? [];
}

async function state(walletId: string) {
  return (await query(`SELECT balance::text balance,version::text version,
    (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger,
    (SELECT count(*)::int FROM wager_transactions WHERE wallet_id=w.id AND status='PROCESSED') processed
    FROM wallets w WHERE id=?`, [walletId]))[0]!;
}

async function inboxRows(messageIds: string[]): Promise<number> {
  return (await query(`SELECT count(*)::int n FROM wager_message_inbox WHERE processed_at IS NOT NULL AND message_id = ANY(?::text[])`,
    [`{${messageIds.join(',')}}`]))[0]!['n'] as number;
}

const consumer = (queueUrl: string, processor: WagerMessageProcessor = new ProcessInboxWager(new SqlInboxProcessWagerStore(orm)),
  options: ConstructorParameters<typeof SqsWagerConsumer>[3] = {}) =>
  new SqsWagerConsumer(client, queueUrl, processor, { waitTimeSeconds: 1, retryVisibilitySeconds: 0, ...options });

async function databaseReady(): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt++) {
    if (Bun.spawnSync(['docker', 'exec', container, 'pg_isready', '-U', 'jwp']).exitCode === 0) return;
    await Bun.sleep(100);
  }
  throw new Error('PostgreSQL did not restart.');
}

test('backoff grows from ApproximateReceiveCount and is capped', () => {
  expect([1, 2, 3, 4, 5, 6, 7].map(count => transientRetryVisibilitySeconds(count, 3, 60))).toEqual([3, 6, 12, 24, 48, 60, 60]);
  expect(transientRetryVisibilitySeconds(Number.NaN, 3, 60)).toBe(3);
  expect(() => new SqsWagerConsumer(client, 'unused', { execute: async () => { throw new Error('unused'); } },
    { visibilityTimeoutSeconds: 2, heartbeatIntervalMs: 2000 })).toThrow('Invalid SQS wager consumer options.');
});

test('PostgreSQL down for more than 20s: no valid command reaches the DLQ; each is applied once after recovery', async () => {
  const target = await wallet();
  const queues = await pair();
  const worker = consumer(queues.url);
  const observations: (ConsumerObservation & { at: number })[] = [];
  let polling = true;
  const loop = (async () => {
    while (polling) {
      try { for (const observation of await worker.pollOnce()) observations.push({ ...observation, at: Date.now() }); }
      catch { await Bun.sleep(100); }
    }
  })();

  expect(Bun.spawnSync(['docker', 'kill', '--signal=KILL', container]).exitCode).toBe(0);
  const downAt = Date.now();
  const messages = [envelope(target, '1.00'), envelope(target, '2.00'), envelope(target, '3.00')];
  for (const message of messages) await send(queues.url, message, target.id);
  let downMs = 0;
  try {
    await Bun.sleep(25_000);
  } finally {
    expect(Bun.spawnSync(['docker', 'start', container]).exitCode).toBe(0);
    downMs = Date.now() - downAt;
    await databaseReady();
  }
  expect(downMs).toBeGreaterThan(20_000);

  const ids = messages.map(message => message.messageId);
  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline && (await inboxRows(ids).catch(() => 0)) < ids.length) await Bun.sleep(500);
  polling = false;
  await loop;

  expect(await inboxRows(ids)).toBe(3);
  expect(await state(target.id)).toMatchObject({ balance: '94.00', version: '4', ledger: 4, processed: 4 });
  expect(await receive(queues.dlqUrl)).toEqual([]);
  expect(await receive(queues.url)).toEqual([]);



  for (const id of ids) {
    const mine = observations.filter(observation => observation.messageId === id);
    const retries = mine.filter(observation => observation.disposition === 'RETRY');
    expect(retries.length).toBeGreaterThanOrEqual(2);
    expect(mine.some(observation => observation.disposition === 'PERMANENT')).toBe(false);
    expect(mine.at(-1)).toMatchObject({ disposition: 'ACKNOWLEDGED' });
    expect(mine.at(-1)!.receiveCount).toBeLessThanOrEqual(5);

    for (let i = 1; i < mine.length; i++) {
      const waited = mine[i]!.at - mine[i - 1]!.at;
      expect(waited).toBeGreaterThanOrEqual(transientRetryVisibilitySeconds(mine[i - 1]!.receiveCount, 3, 60) * 1000 - 500);
    }
  }
  console.log(JSON.stringify({ proof: 'consumer-transient-outage', downMs,
    perMessage: ids.map(id => observations.filter(o => o.messageId === id).map(o => [o.disposition, o.receiveCount])),
    dlq: 0, balance: '94.00' }));
}, 180_000);

test('heartbeat keeps a received message hidden while it is processed longer than the queue visibility', async () => {
  const target = await wallet();
  const queues = await pair();
  await client.send(new SetQueueAttributesCommand({ QueueUrl: queues.url, Attributes: { VisibilityTimeout: '2' } }));

  // Sem heartbeat, o receipt deve reaparecer após a visibilidade de 2 s.
  const control = envelope(target, '1.00');
  await send(queues.url, control, target.id);
  expect(await receive(queues.url)).toHaveLength(1);
  await Bun.sleep(3_000);
  const reappeared = await receive(queues.url);
  expect(reappeared).toHaveLength(1);

  const actual = new ProcessInboxWager(new SqlInboxProcessWagerStore(orm));
  let competitorSaw = 0;
  const slow: WagerMessageProcessor = {
    async execute(input: unknown, key: unknown, metadata: InboxMessageMetadata) {

      for (let i = 0; i < 6; i++) competitorSaw += (await receive(queues.url, 1)).length;
      return actual.execute(input, key, metadata);
    },
  };
  await Bun.sleep(2_500);
  const worker = consumer(queues.url, slow, { visibilityTimeoutSeconds: 2, heartbeatIntervalMs: 500 });
  const [observation] = await worker.pollOnce();
  expect(observation).toMatchObject({ messageId: control.messageId });
  expect(competitorSaw).toBe(0);
  expect(await state(target.id)).toMatchObject({ balance: '99.00', ledger: 2 });
  // LocalStack pode expirar o receipt original mesmo após renovar a visibilidade;
  // ack recusado após commit gera redelivery, absorvida pela inbox,
  // sem um segundo débito.
  const observations = [observation!];
  for (let i = 0; i < 10 && observations.at(-1)!.disposition !== 'ACKNOWLEDGED'; i++) {
    const [next] = await consumer(queues.url).pollOnce();
    if (next) observations.push(next);
  }
  expect(observations.every(o => o.disposition === 'ACKNOWLEDGED' || (o.disposition === 'RETRY' && o.errorCode !== 'INFRASTRUCTURE_UNAVAILABLE'))).toBe(true);
  expect(observations.at(-1)).toMatchObject({ messageId: control.messageId, disposition: 'ACKNOWLEDGED' });
  expect(await state(target.id)).toMatchObject({ balance: '99.00', ledger: 2 });
  expect(await inboxRows([control.messageId])).toBe(1);
  expect(await receive(queues.url, 3)).toEqual([]);
  expect(await receive(queues.dlqUrl)).toEqual([]);
  console.log(JSON.stringify({ proof: 'consumer-visibility-heartbeat', heldSeconds: 6, queueVisibility: 2, competitorSaw,
    dispositions: observations.map(o => [o.disposition, o.receiveCount, o.errorCode ?? null]) }));
}, 60_000);

test('MessageGroupId different from walletId is processed, not dead-lettered', async () => {
  const target = await wallet();
  const queues = await pair();
  const message = envelope(target, '5.00');
  const groupId = `provider-group-${randomUUID()}`;
  expect(groupId).not.toBe(target.id);
  await send(queues.url, message, groupId);
  const worker = consumer(queues.url);
  let observation: ConsumerObservation | undefined;
  for (let i = 0; i < 10 && !observation; i++) [observation] = await worker.pollOnce();
  expect(observation).toMatchObject({ messageId: message.messageId, disposition: 'ACKNOWLEDGED', receiveCount: 1 });
  expect(await state(target.id)).toMatchObject({ balance: '95.00', ledger: 2 });
  expect(await receive(queues.dlqUrl)).toEqual([]);
  console.log(JSON.stringify({ proof: 'consumer-foreign-group-id', groupId, disposition: observation!.disposition }));
}, 60_000);

test('final invariant: every wallet balance equals its real PostgreSQL ledger and is nonnegative', async()=>{
  await assertWalletLedgerInvariant(orm);
});
