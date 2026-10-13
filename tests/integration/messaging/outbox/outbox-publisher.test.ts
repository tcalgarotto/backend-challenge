import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../../src/persistence/financial-orm.js';
import {
  SqlEventDeduplicator,
  SqsEventConsumer,
} from '../../../../src/messaging/outbox/sqs-event-consumer.js';
import { SqlOutboxStore } from '../../../../src/messaging/outbox/sql-outbox-store.js';
import { SqsOutboxPublisher } from '../../../../src/messaging/outbox/sqs-outbox-publisher.js';
import { canonicalJson, ProcessWager } from '../../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../../src/wagering/infrastructure/sql-process-wager-store.js';
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

test('two publishers claim disjoint rows, call real SQS after commit, and never publish into the commands queue', async () => {
  const events = await pair();
  const commands = await pair('wager-transactions.fifo');
  const wallets = await Promise.all([wallet(), wallet(), wallet()]);
  const visibleClaims: string[] = [];
  const inspectingClient = {
    send: async (command: unknown, options?: unknown) => {
      if (command instanceof SendMessageCommand) {
        const body = JSON.parse(command.input.MessageBody!);
        const [row] = await query(
          'SELECT claim_token,published_at FROM outbox_events WHERE event_id=?',
          [body.eventId],
        );
        expect(row).toMatchObject({ published_at: null });
        expect(row?.['claim_token']).toMatch(/^[0-9a-f-]{36}$/);
        visibleClaims.push(body.eventId);
      }
      return client.send(command as never, options as never);
    },
  } as unknown as SQSClient;
  const publisherA = new SqsOutboxPublisher(
    inspectingClient,
    events.url,
    new SqlOutboxStore(orm),
    { batchSize: 3 },
  );
  const publisherB = new SqsOutboxPublisher(
    inspectingClient,
    events.url,
    new SqlOutboxStore(orm),
    { batchSize: 3 },
  );

  // Claims exclusivos por wallet exigem rodadas conjuntas dos dois publishers.
  const published: { eventId: string; status: string }[] = [];
  for (let round = 0; round < 10 && published.length < 6; round++) {
    const [roundA, roundB] = await Promise.all([publisherA.publishBatch(), publisherB.publishBatch()]);
    published.push(...roundA, ...roundB);
  }
  expect(published).toHaveLength(6);
  expect(published.every(result => result.status === 'PUBLISHED')).toBe(true);
  expect(new Set(visibleClaims).size).toBe(6);
  expect(visibleClaims).toHaveLength(6);

  const ordered = await query('SELECT event_id,wallet_id FROM outbox_events ORDER BY wallet_id,seq');
  for (const current of wallets) {
    const expected = ordered.filter(row => row['wallet_id'] === current.id).map(row => row['event_id'] as string);
    expect(visibleClaims.filter(eventId => expected.includes(eventId))).toEqual(expected);
  }
  expect((await query('SELECT count(*)::int count FROM outbox_events WHERE published_at IS NOT NULL'))[0]?.['count']).toBe(6);

  const consumer = new SqsEventConsumer(
    client,
    events.url,
    new SqlEventDeduplicator(orm),
    { waitTimeSeconds: 1 },
  );
  const consumed = await consume(consumer, 6);
  expect(consumed.every(result => result.disposition === 'ACKNOWLEDGED')).toBe(true);
  expect(new Set(consumed.map(result => result.eventId)).size).toBe(6);
  expect((await query('SELECT count(*)::int count FROM wager_event_inbox'))[0]?.['count']).toBe(6);
  expect(await receive(commands.url)).toEqual([]);
  expect(new Set(wallets.map(current => current.id)).size).toBe(3);
});

test('a confirmed send followed by a crash is reclaimed with the same eventId and the old token is fenced', async () => {
  const events = await pair();
  const target = await wallet();
  const rows = await query(
    'SELECT event_id FROM outbox_events WHERE wallet_id=? ORDER BY seq',
    [target.id],
  );
  const eventId = rows[0]?.['event_id'] as string;
  const delayedEventId = rows[1]?.['event_id'] as string;
  const startedAt = new Date();
  await query(
    "UPDATE outbox_events SET next_attempt_at=? WHERE event_id=?",
    [new Date(startedAt.getTime() + 86_400_000), delayedEventId],
  );

  const crashedStore = new SqlOutboxStore(orm, 1_000);
  const [crashedClaim] = await crashedStore.claimBatch(1, startedAt);
  expect(crashedClaim?.eventId).toBe(eventId);
  await client.send(new SendMessageCommand({
    QueueUrl: events.url,
    MessageBody: canonicalJson(crashedClaim!.envelope),
    MessageGroupId: crashedClaim!.walletId,
    MessageDeduplicationId: crashedClaim!.eventId,
  }));

  const recoveredAt = new Date(startedAt.getTime() + 1_001);
  const recovered = new SqsOutboxPublisher(
    client,
    events.url,
    new SqlOutboxStore(orm, 1_000),
    { batchSize: 1, clock: () => recoveredAt },
  );
  expect(await recovered.publishBatch(recoveredAt)).toEqual([{ eventId, status: 'PUBLISHED' }]);
  expect(await crashedStore.markPublished(crashedClaim!, recoveredAt)).toBe(false);
  expect((await query(
    'SELECT attempts,published_at IS NOT NULL published FROM outbox_events WHERE event_id=?',
    [eventId],
  ))[0]).toEqual({ attempts: 2, published: true });

  const [message] = await receive(events.url);
  expect(JSON.parse(message!.Body!).eventId).toBe(eventId);
  expect(message!.Attributes?.MessageDeduplicationId).toBe(eventId);
  await client.send(new DeleteMessageCommand({
    QueueUrl: events.url,
    ReceiptHandle: message!.ReceiptHandle!,
  }));
  expect(await receive(events.url)).toEqual([]);
});

test('the reference consumer durably deduplicates eventId even when transport deduplication is bypassed', async () => {
  const events = await pair();
  const target = await wallet();
  const [row] = await query(
    'SELECT event_id,envelope FROM outbox_events WHERE wallet_id=? ORDER BY event_id LIMIT 1',
    [target.id],
  );
  const eventId = row!['event_id'] as string;
  const envelope = row!['envelope'] as Record<string, unknown>;
  for (const suffix of ['first', 'second']) {
    await client.send(new SendMessageCommand({
      QueueUrl: events.url,
      MessageBody: canonicalJson(envelope),
      MessageGroupId: target.id,
      MessageDeduplicationId: `${eventId}-${suffix}`,
    }));
  }

  const consumer = new SqsEventConsumer(
    client,
    events.url,
    new SqlEventDeduplicator(orm),
    { waitTimeSeconds: 1 },
  );
  const observations = await consume(consumer, 2);
  // Promise.all mantém a ordem dos resultados, não a aquisição dos locks SQL.
  expect(observations.every(result => result.eventId === eventId && result.disposition === 'ACKNOWLEDGED')).toBe(true);
  expect(observations.map(result => result.disposition === 'ACKNOWLEDGED' && result.duplicate).sort()).toEqual([false, true]);
  expect((await query(
    'SELECT count(*)::int count FROM wager_event_inbox WHERE event_id=?',
    [eventId],
  ))[0]?.['count']).toBe(1);
});

test('per-wallet order: concurrent publishers and a failed head never let a later event overtake', async () => {
  const events = await pair();
  const target = await wallet();
  const processWager = new ProcessWager(new SqlProcessWagerStore(orm));
  for (let i = 0; i < 5; i++) {
    const result = await processWager.execute({
      providerId: 'order-provider', externalTransactionId: `order-${target.id}-${i}`, playerId: target.playerId,
      walletId: target.id, roundId: 'order-round', gameId: 'order-game', kind: 'BET', money: { amount: '1.00', currency: 'BRL' },
    }, `order-${target.id}-${i}`);
    expect(result.httpStatus).toBe(200);
  }
  const expected = (await query('SELECT event_id FROM outbox_events WHERE wallet_id=? ORDER BY seq', [target.id]))
    .map(row => row['event_id'] as string);
  expect(expected).toHaveLength(12);
  const head = expected[0]!;
  let headFailures = 1;
  const delivered: string[] = [];
  const flaky = {
    send: async (command: unknown, options?: unknown) => {
      if (command instanceof SendMessageCommand) {
        const eventId = JSON.parse(command.input.MessageBody!).eventId as string;
        if (eventId === head && headFailures-- > 0) throw Object.assign(new Error('injected send failure'), { code: 'ECONNRESET' });
        if (expected.includes(eventId)) delivered.push(eventId);
      }
      return client.send(command as never, options as never);
    },
  } as unknown as SQSClient;
  const publishers = [0, 1].map(() => new SqsOutboxPublisher(flaky, events.url, new SqlOutboxStore(orm),
    { batchSize: 4, retryBaseMs: 1, retryMaxMs: 1 }));
  const outcomes: string[] = [];
  for (let round = 0; round < 50 && delivered.length < expected.length; round++) {
    const results = await Promise.all(publishers.map(publisher => publisher.publishBatch(new Date(Date.now() + 1_000))));
    outcomes.push(...results.flat().filter(result => expected.includes(result.eventId)).map(result => result.status));
  }
  expect(outcomes).toContain('RETRY');
  expect(outcomes).toContain('DEFERRED');

  expect(delivered).toEqual(expected);
  expect((await query('SELECT count(*)::int n FROM outbox_events WHERE wallet_id=? AND published_at IS NULL', [target.id]))[0]?.['n']).toBe(0);
});
