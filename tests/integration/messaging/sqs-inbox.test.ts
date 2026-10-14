import { assertWalletLedgerInvariant } from '../support/wallet-invariant.js';
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { ProcessInboxWager, SqlInboxProcessWagerStore } from '../../../src/messaging/sql-inbox-process-wager-store.js';
import { SqsWagerConsumer } from '../../../src/messaging/sqs-wager-consumer.js';
import { sendWagerTransactionRequested, type WagerTransactionRequested } from '../../../src/messaging/wager-message.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { ProcessWager } from '../../../src/wagering/application/process-wager.js';
import { WagerTransactionStatus } from '../../../src/wagering/domain/wager-transaction.js';
import { SqlProcessWagerStore } from '../../../src/wagering/infrastructure/sql-process-wager-store.js';
import { isolatedPair, requireSqs, sqsClient, type QueuePair } from '../sqs/support/sqs.js';

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

test('controlled producer groups by walletId and consumer commits inbox with the financial result before ack', async () => {
  const target = await wallet();
  const queues = await pair();
  const message = envelope(target);
  await sendWagerTransactionRequested(client, queues.url, message);
  const [received] = await receive(queues.url);
  expect(received!.Attributes?.MessageGroupId).toBe(target.id);
  await client.send(new ChangeMessageVisibilityCommand({ QueueUrl: queues.url, ReceiptHandle: received!.ReceiptHandle, VisibilityTimeout: 0 }));

  expect(await pollMessage(consumer(client, queues.url))).toMatchObject({
    messageId: message.messageId,
    disposition: 'ACKNOWLEDGED',
  });
  expect(await receive(queues.url)).toEqual([]);
  expect(await state(target.id)).toMatchObject({ balance: '75.00', version: '2', wagers: 2, ledger: 2 });
  expect(await inboxCount(message.messageId)).toBe(1);
  const [inbox] = await query(`SELECT envelope_hash,processed_at,result_snapshot->'body'->>'status' status
    FROM wager_message_inbox WHERE message_id=?`, [message.messageId]);
  expect(inbox).toMatchObject({ status: 'PROCESSED' });
  expect(inbox!['envelope_hash']).toMatch(/^[0-9a-f]{64}$/);
  expect(Date.parse(inbox!['processed_at'] as string)).not.toBeNaN();
});

test('business rejection is durably recorded and acknowledged', async () => {
  const target = await wallet('10.00');
  const queues = await pair();
  const message = envelope(target, randomUUID(), '25.00');
  await sendWagerTransactionRequested(client, queues.url, message);
  expect(await pollMessage(consumer(client, queues.url))).toMatchObject({ disposition: 'ACKNOWLEDGED' });
  expect(await receive(queues.url)).toEqual([]);
  expect(await state(target.id)).toMatchObject({ balance: '10.00', version: '1', wagers: 2, ledger: 1 });
  expect(await inboxCount(message.messageId)).toBe(1);
  const [wager] = await query('SELECT status,failure_code FROM wager_transactions WHERE external_transaction_id=?',
    [message.data['externalTransactionId']]);
  expect(wager).toEqual({ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' });
});

test('failed ack redelivers safely and inbox prevents a duplicate debit', async () => {
  const target = await wallet();
  const queues = await pair();
  const message = envelope(target);
  await sendWagerTransactionRequested(client, queues.url, message);
  let receipt: string | undefined;
  let failDelete = true;
  const unreliable = {
    send: async (command: unknown, options?: unknown) => {
      if (command instanceof DeleteMessageCommand && failDelete) {
        failDelete = false;
        throw Object.assign(new Error('simulated delete failure'), { code: 'ECONNRESET' });
      }
      const result = await client.send(command as never, options as never);
      if (command instanceof ReceiveMessageCommand) receipt = (result as { Messages?: Message[] }).Messages?.[0]?.ReceiptHandle;
      return result;
    },
  } as unknown as SQSClient;

  expect(await pollMessage(consumer(unreliable, queues.url))).toMatchObject({ disposition: 'RETRY' });
  expect(await state(target.id)).toMatchObject({ balance: '75.00', wagers: 2, ledger: 2 });
  expect(await inboxCount(message.messageId)).toBe(1);
  expect(receipt).toBeDefined();
  await client.send(new ChangeMessageVisibilityCommand({ QueueUrl: queues.url, ReceiptHandle: receipt!, VisibilityTimeout: 0 }));
  expect(await pollMessage(consumer(client, queues.url))).toMatchObject({ disposition: 'ACKNOWLEDGED', receiveCount: 2 });
  expect(await state(target.id)).toMatchObject({ balance: '75.00', version: '2', wagers: 2, ledger: 2 });
  expect(await inboxCount(message.messageId)).toBe(1);
});

test('HTTP use case and SQS adapter race on one identity without duplicating the debit', async () => {
  const target = await wallet();
  const queues = await pair();
  const message = envelope(target);
  const { idempotencyKey, ...input } = message.data;
  await sendWagerTransactionRequested(client, queues.url, message);
  const httpUseCase = new ProcessWager(new SqlProcessWagerStore(orm));
  const [observations, direct] = await Promise.all([
    consumer(client, queues.url).pollOnce(),
    httpUseCase.execute(input, idempotencyKey),
  ]);
  expect(observations[0]).toMatchObject({ disposition: 'ACKNOWLEDGED' });
  expect(direct.body.status).toBe(WagerTransactionStatus.Processed);
  expect(await state(target.id)).toMatchObject({ balance: '75.00', version: '2', wagers: 2, ledger: 2 });
  expect(await inboxCount(message.messageId)).toBe(1);
  expect(await receive(queues.url)).toEqual([]);
});

test('transient PostgreSQL outage is retried by SQS and recovers exactly once', async () => {
  const target = await wallet();
  const queues = await pair();
  const message = envelope(target);
  await sendWagerTransactionRequested(client, queues.url, message);
  const container = process.env['JWP_MESSAGING_POSTGRES_CONTAINER']!;
  expect(Bun.spawnSync(['docker', 'kill', '--signal=KILL', container]).exitCode).toBe(0);
  try {
    expect(await pollMessage(consumer(client, queues.url))).toMatchObject({ disposition: 'RETRY' });
  } finally {
    await restartDatabase(container);
  }
  expect(await pollMessage(consumer(client, queues.url))).toMatchObject({ disposition: 'ACKNOWLEDGED', receiveCount: 2 });
  expect(await state(target.id)).toMatchObject({ balance: '75.00', version: '2', wagers: 2, ledger: 2 });
  expect(await inboxCount(message.messageId)).toBe(1);
});

test('permanent poison is not acknowledged and reaches the real DLQ after maxReceiveCount', async () => {
  const target = await wallet();
  const queues = await pair();
  const poison = envelope(target);
  await client.send(new SendMessageCommand({
    QueueUrl: queues.url,
    MessageBody: JSON.stringify({ ...poison, unexpected: true }),
    MessageGroupId: target.id,
    MessageDeduplicationId: randomUUID(),
  }));
  const worker = consumer(client, queues.url);
  for (let receiveCount = 1; receiveCount <= 5; receiveCount++) {
    expect(await pollMessage(worker)).toMatchObject({ disposition: 'PERMANENT', receiveCount });
  }
  expect(await receive(queues.url)).toEqual([]);
  const [deadLetter] = await receive(queues.dlqUrl);
  expect(JSON.parse(deadLetter!.Body!)).toMatchObject({ unexpected: true });
  expect(await state(target.id)).toMatchObject({ balance: '100.00', version: '1', wagers: 1, ledger: 1 });
  expect(await inboxCount(poison.messageId)).toBe(0);
});

test('same logical messageId with a divergent valid envelope is poison and cannot create a second effect', async () => {
  const target = await wallet();
  const queues = await pair();
  const original = envelope(target);
  await sendWagerTransactionRequested(client, queues.url, original);
  expect(await pollMessage(consumer(client, queues.url))).toMatchObject({ disposition: 'ACKNOWLEDGED' });

  const conflict: WagerTransactionRequested = {
    ...original,
    data: {
      ...original.data,
      externalTransactionId: `conflict-${randomUUID()}`,
      idempotencyKey: `conflict-${randomUUID()}`,
    },
  };
  await client.send(new SendMessageCommand({
    QueueUrl: queues.url,
    MessageBody: JSON.stringify(conflict),
    MessageGroupId: target.id,
    MessageDeduplicationId: randomUUID(),
  }));
  const worker = consumer(client, queues.url);
  for (let receiveCount = 1; receiveCount <= 5; receiveCount++) {
    expect(await pollMessage(worker)).toMatchObject({
      messageId: original.messageId,
      disposition: 'PERMANENT',
      receiveCount,
      errorCode: 'INBOX_MESSAGE_CONFLICT',
    });
  }
  expect(await receive(queues.url)).toEqual([]);
  const [deadLetter] = await receive(queues.dlqUrl);
  expect(JSON.parse(deadLetter!.Body!)).toEqual(conflict);
  expect(await state(target.id)).toMatchObject({ balance: '75.00', version: '2', wagers: 2, ledger: 2 });
  expect(await inboxCount(original.messageId)).toBe(1);
});

test('SQS: moeda habilitada diferente da wallet é CURRENCY_MISMATCH permanente, vai à DLQ e não grava nada', async () => {
  const target = await wallet();
  const queues = await pair();
  const usd = envelope(target);
  const mismatch: WagerTransactionRequested = { ...usd, data: { ...usd.data, money: { amount: '25.00', currency: 'USD' } } };
  await sendWagerTransactionRequested(client, queues.url, mismatch);
  const worker = consumer(client, queues.url);
  for (let receiveCount = 1; receiveCount <= 5; receiveCount++) {
    expect(await pollMessage(worker)).toMatchObject({
      messageId: mismatch.messageId, disposition: 'PERMANENT', receiveCount, errorCode: 'CURRENCY_MISMATCH' });
  }
  expect(await receive(queues.url)).toEqual([]);
  const [deadLetter] = await receive(queues.dlqUrl);
  expect(JSON.parse(deadLetter!.Body!)).toEqual(mismatch);
  expect(await state(target.id)).toMatchObject({ balance: '100.00', version: '1', wagers: 1, ledger: 1 });
  expect(await inboxCount(mismatch.messageId)).toBe(0);
  expect((await query('SELECT count(*)::int n FROM wager_transactions WHERE external_transaction_id=?', [mismatch.data.externalTransactionId]))[0]!['n']).toBe(0);
});

test('SQS: wallet USD processa comando USD pelo mesmo consumidor', async () => {
  const target = await openWallet.execute({ playerId: randomUUID(), initialBalance: { amount: '10.00', currency: 'USD' } });
  const queues = await pair();
  const base = envelope(target);
  const bet: WagerTransactionRequested = { ...base, data: { ...base.data, money: { amount: '2.50', currency: 'USD' } } };
  await sendWagerTransactionRequested(client, queues.url, bet);
  expect(await pollMessage(consumer(client, queues.url))).toMatchObject({ disposition: 'ACKNOWLEDGED' });
  expect(await state(target.id)).toMatchObject({ balance: '7.50', version: '2', wagers: 2, ledger: 2 });
  expect((await query('SELECT currency FROM ledger_entries WHERE wallet_id=? ORDER BY wallet_version', [target.id])).map(row => row['currency'])).toEqual(['USD', 'USD']);
});

test('SIGTERM stops polling and returns in-flight visibility when the grace period expires', async () => {
  const target = await wallet();
  const queues = await pair();
  const message = envelope(target);
  const child = Bun.spawn(['bun', 'tests/integration/messaging/sigterm-worker.ts'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      JWP_MESSAGING_DATABASE_URL: process.env['JWP_MESSAGING_DATABASE_URL']!,
      JWP_MESSAGING_QUEUE_URL: queues.url,
      JWP_MESSAGING_PROCESS_DELAY_MS: '10000',
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  let output = '';
  const collect = (async () => {
    for await (const chunk of child.stdout) output += new TextDecoder().decode(chunk);
  })();
  const waitFor = async (needle: string) => {
    for (let attempt = 0; attempt < 200; attempt++) {
      if (output.includes(needle)) return;
      if (await Promise.race([child.exited.then(() => true), Bun.sleep(25).then(() => false)])) break;
    }
    throw new Error(`worker did not emit ${needle}; output=${output}; stderr=${await new Response(child.stderr).text()}`);
  };

  await waitFor('READY');
  await sendWagerTransactionRequested(client, queues.url, message);
  await waitFor(`PROCESSING ${message.messageId}`);
  child.kill('SIGTERM');
  expect(await child.exited).toBe(0);
  await collect;
  expect(output).toContain('STOPPED VISIBILITY_RETURNED');
  expect(await inboxCount(message.messageId)).toBe(0);

  expect(await pollMessage(consumer(client, queues.url))).toMatchObject({
    messageId: message.messageId,
    disposition: 'ACKNOWLEDGED',
    receiveCount: 2,
  });
  expect(await state(target.id)).toMatchObject({ balance: '75.00', version: '2', wagers: 2, ledger: 2 });
  expect(await inboxCount(message.messageId)).toBe(1);
});

test('final invariant: every wallet balance equals its real PostgreSQL ledger and is nonnegative', async()=>{
  await assertWalletLedgerInvariant(orm);
});
