import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { ProcessInboxWager, SqlInboxProcessWagerStore } from '../../../src/messaging/sql-inbox-process-wager-store.js';
import { sendWagerTransactionRequested, type WagerTransactionRequested } from '../../../src/messaging/wager-message.js';
import { ObservedPendingReferenceScheduler, ObservedSqsWagerConsumer } from '../../../src/observability/messaging.js';
import { Observability } from '../../../src/observability/observability.js';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { ProcessWager } from '../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../src/wagering/infrastructure/sql-process-wager-store.js';
import { isolatedPair, type QueuePair } from '../sqs/support/sqs.js';
import { infrastructure, logCapture, migratedOrm, requireSqs, sqsClient, until, wager, type Orm } from './support.js';

const env = infrastructure();
const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
let orm: Orm, client: SQSClient, pair: QueuePair, observability: Observability, consumer: ObservedSqsWagerConsumer;
const logs = logCapture();
// Prazo curto de lock torna determinística a falha transitória na barreira.
const policy = { lockTimeoutMs: 300, statementTimeoutMs: 2000, transactionTimeoutMs: 2000, budgetMs: 5000, maxAttempts: 2, retryBaseMs: 25 };

beforeAll(async () => {
  client = sqsClient(env.sqsEndpoint);
  await requireSqs(client);
  pair = await isolatedPair(client, 'wager-transactions.fifo');
  orm = await migratedOrm(env.databaseUrl);

  observability = new Observability({ orm, logger: logs.logger, readinessTimeoutMs: 2000, sqs: { client, queues: [
    { name: 'wager-transactions.fifo', url: pair.url }, { name: 'wager-transactions-dlq.fifo', url: pair.dlqUrl }] } });
  consumer = new ObservedSqsWagerConsumer(client, pair.url,
    new ProcessInboxWager(new SqlInboxProcessWagerStore(orm, undefined, policy)),
    { waitTimeSeconds: 1, retryVisibilitySeconds: 0 }, observability);
}, 30000);

afterAll(async () => {
  await pair?.dispose();
  client?.destroy();
  if (orm) await orm.close(true);
  const cleanup = await openFinancialOrm(env.databaseUrl);
  try { await cleanup.migrator.down({ to: 0 }); } finally { await cleanup.close(true); }
}, 60000);

const value = (metric: keyof Observability['metrics'], labels: Record<string, string> = {}) => observability.metrics[metric].value(labels);
async function openWallet(amount = '100.00') {
  return new OpenWallet(new SqlWalletOpeningStore(orm)).execute({ playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' } });
}
function envelope(target: { id: string; playerId: string }, amount: string): WagerTransactionRequested {
  const { externalTransactionId, providerId, playerId, walletId, roundId, gameId } = wager(target, 'BET', amount);
  return { messageId: `msg-${randomUUID()}`, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
    data: { providerId, externalTransactionId, idempotencyKey: `key-${randomUUID()}`, playerId, walletId, roundId, gameId,
      kind: 'BET', money: { amount, currency: 'BRL' } } } as WagerTransactionRequested;
}
async function pollUntilObserved() {
  for (let attempt = 0; attempt < 30; attempt++) {
    const observations = await consumer.pollOnce();
    if (observations.length > 0) return observations;
  }
  throw new Error('No SQS message observed.');
}

test('O01/O02 over SQS: ack, lock wait with retry and redelivery are logged with identifiers and counted without money', async () => {
  const wallet = await openWallet('500.00');
  const ok = envelope(wallet, '321.09');
  await sendWagerTransactionRequested(client, pair.url, ok);
  const [acked] = await pollUntilObserved();
  expect(acked).toMatchObject({ disposition: 'ACKNOWLEDGED', messageId: ok.messageId, receiveCount: 1 });
  expect(value('transactions', { channel: 'sqs', status: 'PROCESSED', kind: 'BET' })).toBe(1);
  expect(value('sqsMessages', { consumer: 'wager-transactions-consumer', disposition: 'ACKNOWLEDGED', error_code: 'none' })).toBe(1);

  // Outra sessão segura o lock da wallet para provocar espera, timeout, retry e redelivery.
  const blocked = envelope(wallet, '12.34');
  await sendWagerTransactionRequested(client, pair.url, blocked);
  const holder = orm.em.fork(); await holder.begin();
  let retried;
  try {
    await holder.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE', [wallet.id], 'all', holder.getTransactionContext());
    const polling = pollUntilObserved();
    await until(async () => { await observability.registry.collect(2000); return value('lockWaiters'); }, waiters => waiters >= 1, 3000);
    expect(value('lockWaiters')).toBeGreaterThanOrEqual(1);
    [retried] = await polling;
  } finally { await holder.rollback(); }
  expect(retried).toMatchObject({ disposition: 'RETRY', messageId: blocked.messageId, errorCode: 'INFRASTRUCTURE_UNAVAILABLE' });
  expect(value('sqlTransient', { channel: 'sqs', reason: 'lock_timeout' })).toBe(1);
  expect(value('retries', { component: 'sqs_consumer' })).toBe(1);

  const [redelivered] = await pollUntilObserved();
  expect(redelivered).toMatchObject({ disposition: 'ACKNOWLEDGED', messageId: blocked.messageId, receiveCount: 2 });
  expect(value('duplicates', { layer: 'sqs_redelivery' })).toBe(1);
  expect(value('transactions', { channel: 'sqs', status: 'PROCESSED', kind: 'BET' })).toBe(2);
  await observability.registry.collect(2000);
  expect(value('lockWaiters')).toBe(0);

  const entries = logs.entries();
  const processed = entries.filter(entry => entry['type'] === 'wager.processed');
  expect(processed.map(entry => entry['messageId'])).toEqual([ok.messageId, blocked.messageId]);
  for (const entry of processed) {
    expect(entry).toMatchObject({ level: 'info', walletId: wallet.id, providerId: 'obs-provider', status: 'PROCESSED', kind: 'BET',
      consumer: 'wager-transactions-consumer', idempotentReplay: false });
    expect(entry['correlationId']).toBe(entry['messageId']);
    expect(String(entry['transactionId'])).toMatch(UUID);
  }
  expect(entries.find(entry => entry['type'] === 'wager.failed')).toMatchObject({ level: 'warn', messageId: blocked.messageId,
    walletId: wallet.id, errorCode: 'INFRASTRUCTURE_UNAVAILABLE', reason: 'lock_timeout' });
  expect(entries.filter(entry => entry['type'] === 'sqs.message').map(entry => [entry['disposition'], entry['receiveCount'], entry['level']]))
    .toEqual([['ACKNOWLEDGED', 1, 'info'], ['RETRY', 1, 'warn'], ['ACKNOWLEDGED', 2, 'info']]);
  for (const line of logs.lines) for (const leak of ['321.09', '12.34', '500.00', 'amount', 'money', wallet.playerId]) expect(line).not.toContain(leak);
}, 60000);

test('O02 DLQ: a poison message is permanent on every receive, then redriven; DLQ depth becomes observable', async () => {
  const mark = logs.lines.length;
  await client.send(new SendMessageCommand({ QueueUrl: pair.url, MessageGroupId: randomUUID(), MessageDeduplicationId: randomUUID(),
    MessageBody: '{"messageId":"poison","data":{"money":{"amount":"777.77"}' }));
  await observability.registry.collect(2000);
  expect(value('queueMessages', { queue: 'wager-transactions-dlq.fifo', state: 'visible' })).toBe(0);

  const dlqDepth = await until(async () => {
    await consumer.pollOnce();
    await observability.registry.collect(2000);
    return value('queueMessages', { queue: 'wager-transactions-dlq.fifo', state: 'visible' });
  }, depth => depth === 1, 30000);
  expect(dlqDepth).toBe(1);
  const permanent = logs.lines.slice(mark).map(line => JSON.parse(line)).filter(entry => entry.type === 'sqs.message');
  expect(permanent.length).toBe(5);
  const codes = new Set(permanent.map(entry => entry.errorCode));
  expect(codes.size).toBe(1);
  const [code] = [...codes] as string[];
  for (const entry of permanent) expect(entry).toMatchObject({ level: 'error', disposition: 'PERMANENT' });
  expect(permanent.map(entry => entry.receiveCount)).toEqual([1, 2, 3, 4, 5]);
  expect(value('sqsMessages', { consumer: 'wager-transactions-consumer', disposition: 'PERMANENT', error_code: code ?? 'none' })).toBe(5);
  expect(value('queueMessages', { queue: 'wager-transactions.fifo', state: 'visible' })).toBe(0);
  for (const line of logs.lines.slice(mark)) expect(line).not.toContain('777.77');
  const text = await observability.registry.render(2000);
  expect(text).not.toMatch(UUID);
  expect(text).not.toContain(pair.dlqUrl);
}, 60000);

test('O02 pending references: scheduler outcomes and reschedules are counted and logged by transaction', async () => {
  const wallet = await openWallet('50.00');
  const pending = await new ProcessWager(new SqlProcessWagerStore(orm)).execute(
    wager(wallet, 'REFUND', '5.00', { referenceExternalTransactionId: `missing-${randomUUID()}` }), `key-${randomUUID()}`);
  expect(pending).toMatchObject({ httpStatus: 202, body: { status: 'PENDING_REFERENCE' } });
  const scheduler = new ObservedPendingReferenceScheduler(orm, { jitter: () => 0 }, observability);
  const outcome = await scheduler.runOnce(new Date(Date.now() + 60_000));
  expect(outcome).not.toMatchObject({ status: 'IDLE' });
  const status = outcome.status;
  const failure = outcome.status === 'REJECTED' ? outcome.failureCode : 'none';
  expect(value('pendingReferences', { status, failure_code: failure })).toBe(1);
  if (status === 'RESCHEDULED') expect(value('retries', { component: 'pending_reference' })).toBe(1);
  expect(logs.entries().find(entry => entry['type'] === 'pending_reference.outcome')).toMatchObject({
    level: 'info', status, transactionId: pending.body.transactionId });
}, 30000);
