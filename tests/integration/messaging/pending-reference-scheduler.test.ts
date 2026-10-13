import { assertWalletLedgerInvariant } from '../support/wallet-invariant.js';
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import {
  SqlPendingReferenceScheduler,
  type PendingReferenceClaim,
} from '../../../src/messaging/pending-reference-scheduler.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { ProcessWager } from '../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../src/wagering/infrastructure/sql-process-wager-store.js';

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

async function pendingRefund(ids: ReturnType<typeof identities>) {
  const input = {
    providerId: ids.providerId,
    externalTransactionId: ids.refundExternalTransactionId,
    walletId: ids.walletId,
    playerId: ids.playerId,
    roundId: ids.roundId,
    gameId: ids.gameId,
    kind: 'REFUND',
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

test('two workers claim once, persist backoff, then a restarted worker resolves the out-of-order reference exactly once', async () => {
  const target = await wallet();
  const ids = identities(target);
  const pending = await pendingRefund(ids);
  const firstJob = await job(pending.transactionId);
  expect(firstJob).toMatchObject({ attempts: 0, claim_token: null, lease_until: null });
  const firstDue = new Date(firstJob!['next_attempt_at'] as string);
  const workerA = new SqlPendingReferenceScheduler(orm, { jitter: () => 0 });
  const workerB = new SqlPendingReferenceScheduler(orm, { jitter: () => 0 });

  const concurrent = await Promise.all([workerA.claimOne(firstDue), workerB.claimOne(firstDue)]);
  const claims = concurrent.filter((claim): claim is PendingReferenceClaim => claim !== null);
  expect(claims).toHaveLength(1);
  expect(concurrent.filter(claim => claim === null)).toHaveLength(1);
  expect(await workerA.processClaim(claims[0]!, firstDue)).toMatchObject({ status: 'RESCHEDULED' });
  const backedOff = await job(pending.transactionId);
  expect(backedOff).toMatchObject({ attempts: 1, claim_token: null, lease_until: null });
  expect(
    new Date(backedOff!['next_attempt_at'] as string).getTime() - firstDue.getTime(),
  ).toBe(1_000);

  const referenceId = await referenceBet(ids);
  const restarted = new SqlPendingReferenceScheduler(orm, { jitter: () => 0 });
  const resolution = await restarted.runOnce(new Date(backedOff!['next_attempt_at'] as string));
  expect(resolution).toEqual({ status: 'PROCESSED', transactionId: pending.transactionId });
  expect(await job(pending.transactionId)).toBeUndefined();
  expect(await financialState(target.id)).toMatchObject({
    balance: '100.00',
    version: '3',
    ledger: 3,
    wagers: 3,
  });
  expect(
    (await query('SELECT reference_transaction_id FROM wager_transactions WHERE id=?', [
      pending.transactionId,
    ]))[0],
  ).toEqual({ reference_transaction_id: referenceId });
  expect(await eventCount(pending.transactionId, 'WagerTransactionPendingReference')).toBe(1);
  expect(await eventCount(pending.transactionId, 'WagerTransactionProcessed')).toBe(1);
  expect(await eventCount(pending.transactionId, 'WalletBalanceChanged')).toBe(1);

  const correlations = await query(
    `SELECT DISTINCT envelope->>'correlationId' AS correlation_id FROM outbox_events WHERE transaction_id=?`,
    [pending.transactionId],
  );
  expect(correlations).toHaveLength(1);
  expect(await restarted.runOnce(new Date(Date.now() + 60_000))).toEqual({ status: 'IDLE' });

  const replay = await processWager.execute(pending.input, pending.key);
  expect(replay).toMatchObject({
    httpStatus: 200,
    body: {
      transactionId: pending.transactionId,
      status: 'PROCESSED',
      balance: { amount: '100.00', currency: 'BRL' },
      idempotentReplay: true,
    },
  });
});

test('an expired lease is reclaimed after a crash and the old token is fenced', async () => {
  const target = await wallet();
  const ids = identities(target);
  const pending = await pendingRefund(ids);
  const firstDue = new Date((await job(pending.transactionId))!['next_attempt_at'] as string);
  const crashed = new SqlPendingReferenceScheduler(orm, { leaseMs: 2_000, jitter: () => 0 });
  const oldClaim = await crashed.claimOne(firstDue);
  expect(oldClaim).not.toBeNull();
  expect(await new SqlPendingReferenceScheduler(orm, { leaseMs: 2_000 }).claimOne(
    new Date(oldClaim!.leaseUntil.getTime() - 1),
  )).toBeNull();

  await referenceBet(ids);
  const restartAt = new Date(oldClaim!.leaseUntil.getTime() + 1);
  const restarted = new SqlPendingReferenceScheduler(orm, { leaseMs: 2_000, jitter: () => 0 });
  const recovered = await restarted.claimOne(restartAt);
  expect(recovered).toMatchObject({ transactionId: pending.transactionId, attempts: 2 });
  expect(recovered!.token).not.toBe(oldClaim!.token);
  expect(await crashed.processClaim(oldClaim!, restartAt)).toEqual({
    status: 'LOST_LEASE',
    transactionId: pending.transactionId,
  });
  expect(await restarted.processClaim(recovered!, restartAt)).toEqual({
    status: 'PROCESSED',
    transactionId: pending.transactionId,
  });
  expect(await financialState(target.id)).toMatchObject({ balance: '100.00', version: '3', ledger: 3 });
  expect(await eventCount(pending.transactionId, 'WagerTransactionProcessed')).toBe(1);
});

test('the TTL boundary rechecks an already-applied reference under the wallet lock before expiring', async () => {
  const target = await wallet();
  const ids = identities(target);
  const pending = await pendingRefund(ids);
  await referenceBet(ids);
  const expiresAt = new Date((await job(pending.transactionId))!['expires_at'] as string);

  expect(await new SqlPendingReferenceScheduler(orm).runOnce(expiresAt)).toEqual({
    status: 'PROCESSED',
    transactionId: pending.transactionId,
  });
  expect(await financialState(target.id)).toMatchObject({ balance: '100.00', version: '3', ledger: 3 });
  expect(await eventCount(pending.transactionId, 'WagerTransactionRejected')).toBe(0);
});

test('a terminal reference is rejected immediately instead of consuming the remaining TTL', async () => {
  const target = await wallet('10.00');
  const ids = identities(target);
  const pending = await pendingRefund(ids);
  const rejectedReference = await processWager.execute(
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
  expect(rejectedReference).toMatchObject({
    httpStatus: 422,
    body: { status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS' },
  });
  const referenceId = rejectedReference.body.transactionId;
  expect(
    (await query('SELECT status,failure_code FROM wager_transactions WHERE id=?', [referenceId]))[0],
  ).toEqual({ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' });
  const pendingJob = await job(pending.transactionId);
  const firstDue = new Date(pendingJob!['next_attempt_at'] as string);
  expect(firstDue.getTime()).toBeLessThan(new Date(pendingJob!['expires_at'] as string).getTime());

  expect(await new SqlPendingReferenceScheduler(orm).runOnce(firstDue)).toEqual({
    status: 'REJECTED',
    transactionId: pending.transactionId,
    failureCode: 'REFERENCE_NOT_PROCESSED',
  });
  expect(await job(pending.transactionId)).toBeUndefined();
  expect(await financialState(target.id)).toMatchObject({
    balance: '10.00',
    version: '1',
    ledger: 1,
    wagers: 3,
  });
  expect(await eventCount(pending.transactionId, 'WagerTransactionRejected')).toBe(1);
});

test('TTL expiry is terminal, emits REFERENCE_EXPIRED once, and a late reference cannot reopen it', async () => {
  const target = await wallet();
  const ids = identities(target);
  const pending = await pendingRefund(ids);
  const expiresAt = new Date((await job(pending.transactionId))!['expires_at'] as string);
  const scheduler = new SqlPendingReferenceScheduler(orm);

  expect(await scheduler.runOnce(expiresAt)).toEqual({
    status: 'REJECTED',
    transactionId: pending.transactionId,
    failureCode: 'REFERENCE_EXPIRED',
  });
  expect(await job(pending.transactionId)).toBeUndefined();
  expect(await financialState(target.id)).toMatchObject({
    balance: '100.00',
    version: '1',
    ledger: 1,
    wagers: 2,
  });
  expect(await eventCount(pending.transactionId, 'WagerTransactionPendingReference')).toBe(1);
  expect(await eventCount(pending.transactionId, 'WagerTransactionRejected')).toBe(1);

  await referenceBet(ids);
  expect(await scheduler.runOnce(new Date(expiresAt.getTime() + 60_000))).toEqual({ status: 'IDLE' });
  expect(await financialState(target.id)).toMatchObject({
    balance: '75.00',
    version: '2',
    ledger: 2,
    wagers: 3,
  });
  expect(
    (await query('SELECT status,failure_code,reference_transaction_id FROM wager_transactions WHERE id=?', [
      pending.transactionId,
    ]))[0],
  ).toEqual({
    status: 'REJECTED',
    failure_code: 'REFERENCE_EXPIRED',
    reference_transaction_id: null,
  });
  expect(await processWager.execute(pending.input, pending.key)).toMatchObject({
    httpStatus: 422,
    body: {
      status: 'REJECTED',
      failureCode: 'REFERENCE_EXPIRED',
      idempotentReplay: true,
    },
  });
});

test('final invariant: every wallet balance equals its real PostgreSQL ledger and is nonnegative', async()=>{
  await assertWalletLedgerInvariant(orm);
});
