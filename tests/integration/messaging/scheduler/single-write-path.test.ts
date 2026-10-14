import 'reflect-metadata';
import { afterAll, beforeAll, beforeEach, expect, spyOn, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../../src/persistence/financial-orm.js';
import { OpenWallet } from '../../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { WageringModule } from '../../../../src/wagering/http/wagering.module.js';
import { configureReversalPolicy } from '../../../../src/wagering/domain/reversal-policy.js';
import * as writer from '../../../../src/wagering/infrastructure/persist-wager-effects.js';
import { SqlPendingReferenceScheduler } from '../../../../src/messaging/pending-reference-scheduler.js';
import { ProcessInboxWager, SqlInboxProcessWagerStore } from '../../../../src/messaging/sql-inbox-process-wager-store.js';
import { SqsWagerConsumer } from '../../../../src/messaging/sqs-wager-consumer.js';
import { sendWagerTransactionRequested } from '../../../../src/messaging/wager-message.js';
import { isolatedPair, requireSqs, sqsClient, type QueuePair } from '../../sqs/support/sqs.js';
import { downToZeroAfterReversals } from '../../support/reversal-policy-teardown.js';
import { assertWalletLedgerInvariant } from '../../support/wallet-invariant.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let app: INestApplication;
let openWallet: OpenWallet;
let client: SQSClient;
let base: string;
let migrated = false;
const pairs: QueuePair[] = [];
const query = (sql: string, args: unknown[] = []) =>
  orm.em.fork().getConnection().execute<Record<string, unknown>[]>(sql, args);

beforeAll(async () => {
  const url = process.env['JWP_SCHEDULER_DATABASE_URL'];
  const container = process.env['JWP_SCHEDULER_POSTGRES_CONTAINER'];
  if (!url || !container?.startsWith('jwp-scheduler-')) throw new Error('Dedicated scheduler PostgreSQL required.');
  client = sqsClient();
  await requireSqs(client);
  orm = await openFinancialOrm(url);
  expect((await query("SELECT to_regclass('public.wallets') relation"))[0]?.['relation']).toBeNull();
  await orm.migrator.up();
  migrated = true;
  openWallet = new OpenWallet(new SqlWalletOpeningStore(orm));
  app = await NestFactory.create(WageringModule.register(orm), { logger: false });
  await app.listen(0, '127.0.0.1');
  base = await app.getUrl();
}, 30000);

beforeEach(async () => {
  configureReversalPolicy({});
  await query("UPDATE pending_reference_jobs SET next_attempt_at=clock_timestamp()+interval '1 hour'");
});

afterAll(async () => {
  configureReversalPolicy({});
  await app?.close();
  await Promise.all(pairs.map(pair => pair.dispose()));
  client?.destroy();
  if (orm) {
    try { if (migrated) await downToZeroAfterReversals(orm); }
    finally { await orm.close(true); }
  }
});

async function fixture(channel: string) {
  const wallet = await openWallet.execute({ playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
  return { wallet, provider: `single-write-${channel}-${randomUUID()}`, ids: new Map<string, string>() };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;
function command(target: Fixture, kind: string, external: string, reference?: string) {
  return { providerId: target.provider, externalTransactionId: external, walletId: target.wallet.id,
    playerId: target.wallet.playerId, roundId: 'single-write-round', gameId: 'single-write-game', kind,
    money: { amount: kind === 'LOSS' ? '0.00' : '25.00', currency: 'BRL' },
    ...(reference ? { referenceExternalTransactionId: reference } : {}) };
}
async function http(target: Fixture, kind: string, external: string, reference?: string) {
  const response = await fetch(`${base}/wagering/transactions`, { method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': `${target.provider}:${external}` },
    body: JSON.stringify(command(target, kind, external, reference)) });
  const body = await response.json() as { transactionId: string; status: string; idempotentReplay: boolean; failureCode?: string };
  target.ids.set(external, body.transactionId);
  return { status: response.status, body };
}
async function queue(target: Fixture) {
  const pair = await isolatedPair(client, 'wager-transactions.fifo');
  pairs.push(pair);
  const consumer = new SqsWagerConsumer(client, pair.url,
    new ProcessInboxWager(new SqlInboxProcessWagerStore(orm)), { waitTimeSeconds: 1, retryVisibilitySeconds: 0 });
  return async (kind: string, external: string, reference?: string) => {
    await sendWagerTransactionRequested(client, pair.url, { messageId: randomUUID(), type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(), data: { ...command(target, kind, external, reference),
        idempotencyKey: `${target.provider}:${external}` } });
    let observed = false;
    for (let attempt = 0; attempt < 20; attempt++) {
      const [result] = await consumer.pollOnce();
      if (result) { expect(result.disposition).toBe('ACKNOWLEDGED'); observed = true; break; }
    }
    expect(observed).toBe(true);
    const [row] = await query('SELECT id,response_snapshot FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',
      [target.provider, external]);
    expect(row).toBeDefined();
    target.ids.set(external, row!['id'] as string);
    return row!['response_snapshot'] as { httpStatus: number; body: { status: string; failureCode?: string } };
  };
}

function normalize(value: unknown, target: Fixture): unknown {
  const identities = new Map<string, string>([
    [target.wallet.id, 'wallet'], [target.wallet.playerId, 'player'], [target.provider, 'provider'],
    ...[...target.ids].map(([external, id]): [string, string] => [id, external]),
  ]);
  const walk = (item: unknown): unknown => {
    if (Array.isArray(item)) return item.map(walk);
    if (item !== null && typeof item === 'object') return Object.fromEntries(Object.entries(item)
      .filter(([key]) => !['eventId', 'occurredAt', 'processedAt', 'rejectedAt', 'correlationId'].includes(key))
      .map(([key, inner]) => [key, walk(inner)]));
    return typeof item === 'string' ? identities.get(item) ?? item : item;
  };
  return walk(value);
}
async function ledger(target: Fixture) {
  return query(`SELECT currency,wallet_version,direction,amount::text,balance_before::text,balance_after::text
    FROM ledger_entries WHERE wallet_id=? ORDER BY wallet_version`, [target.wallet.id]);
}
async function terminalEvents(target: Fixture, external: string) {
  const rows = await query(`SELECT envelope FROM outbox_events
    WHERE transaction_id=? AND event_type<>'WagerTransactionPendingReference' ORDER BY seq`, [target.ids.get(external)]);
  return rows.map(row => normalize(row['envelope'], target));
}

for (const policy of ['per_type', 'per_reference'] as const) {
  test(`${policy}: HTTP, SQS e scheduler usam a mesma escrita sob lock e persistem ledger/outbox equivalentes`, async () => {
    configureReversalPolicy({ JWP_REVERSAL_POLICY: policy });
    const actual = writer.persistWagerEffects;
    const calls: { transactionId: string; status: string }[] = [];
    const shared = spyOn(writer, 'persistWagerEffects').mockImplementation(async (em, effects) => {
      expect(em.getTransactionContext()).toBeDefined();
      // Outra conexão não pode adquirir a wallet enquanto a função compartilhada escreve.
      await expect(orm.em.fork().transactional(async competing => {
        await competing.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE NOWAIT',
          [effects.wallet.id], 'all', competing.getTransactionContext());
      })).rejects.toMatchObject({ code: '55P03' });
      calls.push({ transactionId: effects.transaction.id, status: effects.transaction.status });
      await actual(em, effects);
    });
    try {
      const direct = await fixture('http'), messaging = await fixture('sqs'), pending = await fixture('scheduler');
      const send = await queue(messaging);
      expect((await http(direct, 'BET', 'bet')).status).toBe(200);
      expect((await send('BET', 'bet')).httpStatus).toBe(200);
      expect((await http(pending, 'REFUND', 'refund', 'bet')).status).toBe(202);
      expect((await http(pending, 'ROLLBACK', 'rollback', 'bet')).status).toBe(202);
      expect((await http(pending, 'BET', 'bet')).status).toBe(200);
      const refund = await http(direct, 'REFUND', 'refund', 'bet');
      expect(refund.status).toBe(200);
      expect((await send('REFUND', 'refund', 'bet')).httpStatus).toBe(200);
      const rollback = await http(direct, 'ROLLBACK', 'rollback', 'bet');
      const expectedStatus = policy === 'per_type' ? 200 : 422;
      expect(rollback.status).toBe(expectedStatus);
      expect((await send('ROLLBACK', 'rollback', 'bet')).httpStatus).toBe(expectedStatus);
      const at = new Date(Date.now() + 2000);
      await query('UPDATE pending_reference_jobs SET next_attempt_at=? WHERE transaction_id=?',
        [new Date(at.getTime() - 2), pending.ids.get('refund')]);
      await query('UPDATE pending_reference_jobs SET next_attempt_at=? WHERE transaction_id=?',
        [new Date(at.getTime() - 1), pending.ids.get('rollback')]);
      const scheduler = new SqlPendingReferenceScheduler(orm);
      expect(await scheduler.runOnce(at)).toMatchObject({ status: 'PROCESSED', transactionId: pending.ids.get('refund') });
      expect(await scheduler.runOnce(at)).toMatchObject({
        status: policy === 'per_type' ? 'PROCESSED' : 'REJECTED', transactionId: pending.ids.get('rollback'),
        ...(policy === 'per_reference' ? { failureCode: 'ALREADY_REVERSED' } : {}),
      });
      for (const target of [direct, messaging, pending]) {
        for (const external of ['refund', 'rollback']) {
          const terminal = external === 'rollback' && policy === 'per_reference' ? 'REJECTED' : 'PROCESSED';
          expect(calls.filter(call => call.transactionId === target.ids.get(external)).map(call => call.status))
            .toEqual(target === pending ? ['PENDING_REFERENCE', terminal] : [terminal]);
        }
      }
      await assertWalletLedgerInvariant(orm);
      expect(await ledger(messaging)).toEqual(await ledger(direct));
      expect(await ledger(pending)).toEqual(await ledger(direct));
      for (const external of ['refund', 'rollback']) {
        expect(await terminalEvents(messaging, external)).toEqual(await terminalEvents(direct, external));
        expect(await terminalEvents(pending, external)).toEqual(await terminalEvents(direct, external));
        const responses = await Promise.all([direct, messaging, pending].map(async target => {
          const [row] = await query('SELECT response_snapshot FROM wager_transactions WHERE id=?', [target.ids.get(external)]);
          const snapshot = row!['response_snapshot'] as { httpStatus: number; body: object };
          return normalize({ httpStatus: snapshot.httpStatus, body: snapshot.body }, target);
        }));
        expect(responses[1]).toEqual(responses[0]);
        expect(responses[2]).toEqual(responses[0]);
        expect((await query(`SELECT count(*)::int count FROM outbox_events
          WHERE transaction_id=? AND event_type='WagerTransactionPendingReference'`, [pending.ids.get(external)]))[0]!['count']).toBe(1);
        const correlations = await query(`SELECT DISTINCT envelope->>'correlationId' correlation
          FROM outbox_events WHERE transaction_id=?`, [pending.ids.get(external)]);
        expect(correlations).toHaveLength(1);
      }
      const beforeReplay = calls.length;
      expect((await http(direct, 'REFUND', 'refund', 'bet')).body).toEqual({ ...refund.body, idempotentReplay: true });
      expect((await http(pending, 'REFUND', 'refund', 'bet')).status).toBe(200);
      await send('REFUND', 'refund', 'bet');
      expect(calls).toHaveLength(beforeReplay);
      const countsBefore = await query('SELECT count(*)::int count FROM outbox_events WHERE wallet_id=?', [direct.wallet.id]);
      const loss = await http(direct, 'LOSS', 'loss');
      expect(loss.status).toBe(200);
      expect(calls.at(-1)?.transactionId).toBe(loss.body.transactionId);
      expect(await ledger(direct)).toEqual(await ledger(messaging));
      expect((await query('SELECT event_type FROM outbox_events WHERE transaction_id=?', [loss.body.transactionId]))
        .map(row => row['event_type'])).toEqual(['WagerTransactionProcessed']);
      const countsAfter = await query('SELECT count(*)::int count FROM outbox_events WHERE wallet_id=?', [direct.wallet.id]);
      expect(countsAfter[0]!['count']).toBe((countsBefore[0]!['count'] as number) + 1);
    } finally { shared.mockRestore(); configureReversalPolicy({}); }
  }, 60000);
}
