import { LedgerDirection } from '../../src/wallet/domain/wallet-ledger-entry.js';
import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Money } from '../../src/shared/domain/money.js';
import { Wallet, type WalletMovement } from '../../src/wallet/domain/wallet.js';
import {
  WagerTransaction as Tx,
  WagerTransactionKind as K,
} from '../../src/wagering/domain/wager-transaction.js';
import {
  WagerTransactionProcessed as Processed,
  WagerTransactionRejected as Rejected,
  WagerTransactionPendingReference as Pending,
} from '../../src/wagering/events/wager-events.js';
import { WalletBalanceChanged as Changed } from '../../src/wallet/events/wallet-balance-changed.js';
import type { EventContext } from '../../src/shared/events/integration-event.js';
const created = new Date('2026-10-11T20:00:00Z'),
  at = new Date('2026-10-11T20:00:01Z');
const m = (amount: string) => Money.from({ amount, currency: 'BRL' });
const wallet = (amount = '100.00') =>
  Wallet.open({
    id: randomUUID(),
    playerId: randomUUID(),
    initialBalance: m(amount),
  });
const ctx = (o: Partial<EventContext> = {}): EventContext => ({
  eventId: randomUUID(),
  correlationId: 'request',
  occurredAt: at,
  ...o,
});
function tx(w: Wallet, kind = K.Bet, amount = '25.00', ref?: Tx) {
  return Tx.create({
    id: randomUUID(),
    walletId: w.id,
    playerId: w.playerId,
    providerId: 'provider',
    externalTransactionId: randomUUID(),
    idempotencyKey: randomUUID(),
    payloadHash: 'a'.repeat(64),
    roundId: 'round',
    gameId: 'game',
    kind,
    money: m(amount),
    createdAt: created,
    ...(ref ? { referenceExternalTransactionId: ref.externalTransactionId } : {}),
  });
}

test('processed and balance-changed envelopes carry fixed class types/version and MoneyProps', () => {
  const w = wallet(),
    t = tx(w),
    movement = w.debit(t.money);
  t.markProcessed(undefined, at);
  const context = ctx({ causationId: 'command' }),
    p = Processed.from(t, w, context).toJSON(),
    c = Changed.from(w, t, movement, ctx()).toJSON();
  expect(p).toEqual({
    eventId: context.eventId,
    eventType: 'WagerTransactionProcessed',
    aggregateId: t.id,
    correlationId: 'request',
    causationId: 'command',
    occurredAt: at.toISOString(),
    version: 1,
    data: {
      transactionId: t.id,
      providerId: 'provider',
      externalTransactionId: t.externalTransactionId,
      walletId: w.id,
      playerId: w.playerId,
      roundId: 'round',
      gameId: 'game',
      kind: K.Bet,
      money: { amount: '25.00', currency: 'BRL' },
      balance: { amount: '75.00', currency: 'BRL' },
      walletVersion: 2,
      processedAt: at.toISOString(),
    },
  });
  expect(c.eventType).toBe('WalletBalanceChanged');
  expect(c.aggregateId).toBe(w.id);
  expect(c.data.balanceBefore.amount).toBe('100.00');
  expect(c.data.balanceAfter.amount).toBe('75.00');
  expect(c.data.money).not.toBeInstanceOf(Money);
  expect(typeof c.data.money.amount).toBe('string');
  expect(c).not.toHaveProperty('causationId');
});

test('LOSS emits Processed with unchanged wallet version and cannot emit BalanceChanged', () => {
  const w = wallet(),
    t = tx(w, K.Loss, '0.00');
  t.markProcessed(undefined, at);
  expect(Processed.from(t, w, ctx()).data.walletVersion).toBe(1);
  const fake: WalletMovement = {
    direction: LedgerDirection.Credit,
    money: t.money,
    balanceBefore: w.balance,
    balanceAfter: w.balance,
    walletVersion: 1,
  };
  expect(() => Changed.from(w, t, fake, ctx())).toThrow('INVALID_EVENT');
  expect(w.balance.toString()).toBe('100.00');
});

test('positive internal OPENING generates version-1 Processed and BalanceChanged from zero', () => {
  const w = wallet(),
    t = Tx.createOpening({
      id: randomUUID(),
      walletId: w.id,
      playerId: w.playerId,
      providerId: 'internal',
      externalTransactionId: w.id,
      idempotencyKey: w.id,
      payloadHash: 'a'.repeat(64),
      roundId: 'opening',
      gameId: 'opening',
      money: w.balance,
      createdAt: created,
    });
  t.markProcessed(undefined, at);
  const movement: WalletMovement = {
    direction: LedgerDirection.Credit,
    money: w.balance,
    balanceBefore: m('0.00'),
    balanceAfter: w.balance,
    walletVersion: 1,
  };
  expect(Processed.from(t, w, ctx()).data.kind).toBe(K.Opening);
  expect(Changed.from(w, t, movement, ctx()).data.walletVersion).toBe(1);
});

test('rejection carries typed code and unchanged wallet snapshot; FAILED is distinct', () => {
  const w = wallet(),
    t = tx(w);
  t.reject('REVERSAL_INSUFFICIENT_FUNDS');
  const e = Rejected.from(t, w, ctx()).toJSON();
  expect(e.eventType).toBe('WagerTransactionRejected');
  expect(e.data.failureCode).toBe('REVERSAL_INSUFFICIENT_FUNDS');
  expect(e.data.balance.amount).toBe('100.00');
  expect(e.data.rejectedAt).toBe(at.toISOString());
  const f = tx(w);
  f.fail('PERMANENT_INFRASTRUCTURE_FAILURE');
  expect(() => Rejected.from(f, w, ctx())).toThrow('INVALID_EVENT');
});

test('pending event uses persisted expiry/entry time; subsequent polling cannot move event time', () => {
  const w = wallet(),
    b = tx(w),
    t = tx(w, K.Refund, '25.00', b);
  t.markPendingReference(at, new Date('2026-10-11T20:15:01Z'));
  const e = Pending.from(t, ctx());
  expect(e.data.referenceExternalTransactionId).toBe(b.externalTransactionId);
  expect(e.data.expiresAt).toBe('2026-10-11T20:15:01.000Z');
  expect(
    Pending.from(Tx.rehydrate(t.toState()), {
      ...ctx(),
      eventId: e.eventId,
    }).toJSON(),
  ).toEqual(e.toJSON());
  expect(() => Pending.from(t, ctx({ occurredAt: new Date('2026-10-11T20:01:00Z') }))).toThrow(
    'INVALID_EVENT',
  );
});

test('envelope is stable across serialization and protected from nested input/date mutations', () => {
  const w = wallet(),
    t = tx(w),
    movement = w.debit(t.money);
  t.markProcessed(undefined, at);
  const date = new Date(at),
    e = Changed.from(w, t, movement, ctx({ occurredAt: date })),
    serialized = JSON.stringify(e);
  date.setUTCFullYear(2000);
  e.occurredAt.setUTCFullYear(2001);
  expect(Object.isFrozen(e)).toBe(true);
  expect(Object.isFrozen(e.data)).toBe(true);
  expect(Object.isFrozen(e.data.money)).toBe(true);
  expect(Reflect.set(e.data.money, 'amount', '0.00')).toBe(false);
  expect(JSON.stringify(e)).toBe(serialized);
  expect(JSON.stringify(e.toJSON())).toBe(serialized);
  const p = Processed.from(t, w, ctx());
  w.credit(m('1.00'));
  expect(p.data.balance.amount).toBe('75.00');
  expect(p.data.walletVersion).toBe(2);
});

test('factories reject wrong transaction state, wallet context, metadata and movement arithmetic', () => {
  const w = wallet(),
    t = tx(w);
  expect(() => Processed.from(t, w, ctx())).toThrow('INVALID_EVENT');
  expect(() => Rejected.from(t, w, ctx())).toThrow('INVALID_EVENT');
  expect(() => Pending.from(t, ctx())).toThrow('INVALID_EVENT');
  const movement = w.debit(t.money);
  t.markProcessed(undefined, at);
  expect(() => Processed.from(t, wallet(), ctx())).toThrow('INVALID_EVENT');
  for (const context of [
    ctx({ eventId: 'bad' }),
    ctx({ correlationId: '' }),
    ctx({ causationId: '' }),
    ctx({ occurredAt: new Date('invalid') }),
  ])
    expect(() => Processed.from(t, w, context)).toThrow('INVALID_EVENT');
  for (const change of [
    { money: m('24.00') },
    { balanceBefore: m('99.00') },
    { balanceAfter: m('76.00') },
    { walletVersion: 1 },
    { direction: LedgerDirection.Credit as const },
  ])
    expect(() => Changed.from(w, t, { ...movement, ...change }, ctx())).toThrow('INVALID_EVENT');
});

test('ROLLBACK of a WIN generates a DEBIT BalanceChanged and validates the resolved reference', () => {
  const w = wallet(),
    win = tx(w, K.Win);
  w.credit(win.money);
  win.markProcessed(undefined, at);
  const rollback = tx(w, K.Rollback, '25.00', win),
    movement = w.debit(rollback.money);
  rollback.markProcessed(win, at);
  expect(Changed.from(w, rollback, movement, ctx(), win).data.direction).toBe(LedgerDirection.Debit);
  expect(() => Changed.from(w, rollback, movement, ctx())).toThrow('INVALID_REFERENCE_STATE');
});
