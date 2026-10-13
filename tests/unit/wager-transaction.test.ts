import { InvalidTransactionStateError } from '../../src/wagering/domain/wager-error.js';
import { LedgerDirection } from '../../src/wallet/domain/wallet-ledger-entry.js';
import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Money } from '../../src/shared/domain/money.js';
import {
  WagerTransaction as Tx,
  WagerTransactionKind as K,
  WagerTransactionStatus as S,
  type CreateWagerTransactionProps as Props,
  type WagerTransactionState as State,
} from '../../src/wagering/domain/wager-transaction.js';
import type { FailureCode } from '../../src/wagering/domain/wager-error.js';
const w = randomUUID(),
  p = randomUUID(),
  createdAt = new Date('2026-10-11T20:00:00Z'),
  at = new Date('2026-10-11T20:00:01Z');
const money = (amount = '25.00', currency = 'BRL') => Money.from({ amount, currency });
const props = (o: Partial<Props> = {}): Props => ({
  id: randomUUID(),
  walletId: w,
  playerId: p,
  providerId: 'provider',
  externalTransactionId: randomUUID(),
  idempotencyKey: randomUUID(),
  payloadHash: 'a'.repeat(64),
  roundId: 'round',
  gameId: 'game',
  kind: K.Bet,
  money: money(),
  createdAt,
  ...o,
});
const create = (o: Partial<Props> = {}) => Tx.create(props(o));
function bet() {
  const t = create();
  t.markProcessed(undefined, at);
  return t;
}
function reversal(r: Tx, kind = K.Refund, o: Partial<Props> = {}) {
  return create({
    kind,
    referenceExternalTransactionId: r.externalTransactionId,
    ...o,
  });
}

test('factories enforce external kinds, positive money and zero LOSS; OPENING is internal only', () => {
  for (const kind of [K.Bet, K.Win]) expect(create({ kind }).status).toBe(S.Pending);
  expect(create({ kind: K.Loss, money: money('0.00') }).affectsBalance()).toBe(false);
  expect(() => create({ kind: K.Opening })).toThrow('INVALID_WAGER');
  const opening = Tx.createOpening(props());
  expect(opening.kind).toBe(K.Opening);
  expect(opening.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
  for (const kind of [K.Bet, K.Win, K.Refund, K.Rollback])
    for (const m of [money('0.00'), money().negate()])
      expect(() => create({ kind, money: m, referenceExternalTransactionId: 'ref' })).toThrow(
        'INVALID_WAGER',
      );
  expect(() => create({ kind: K.Loss })).toThrow('INVALID_WAGER');
  expect(() => Tx.createOpening(props({ money: money('0.00') }))).toThrow('INVALID_WAGER');
});

test('reference requirements and optional WIN reference are distinct from null/empty and forbidden kinds', () => {
  for (const kind of [K.Refund, K.Rollback]) expect(() => create({ kind })).toThrow('REFERENCE_REQUIRED');
  expect(create({ kind: K.Win }).requiresReference()).toBe(false);
  expect(
    create({
      kind: K.Win,
      referenceExternalTransactionId: 'ref',
    }).referenceDecision(),
  ).toEqual({ status: 'WAIT' });
  for (const referenceExternalTransactionId of ['', null, 42])
    expect(() =>
      create({
        kind: K.Win,
        referenceExternalTransactionId: referenceExternalTransactionId as string,
      }),
    ).toThrow('INVALID_WAGER');
  for (const kind of [K.Bet, K.Loss])
    expect(() =>
      create({
        kind,
        money: kind === K.Loss ? money('0.00') : money(),
        referenceExternalTransactionId: 'ref',
      }),
    ).toThrow('INVALID_WAGER');
});

test('identity/date/hash validation and immutable identity do not change payload identity', () => {
  for (const o of [
    { id: 'invalid' },
    { walletId: 'invalid' },
    { playerId: 'invalid' },
    { payloadHash: 'invalid' },
    { providerId: '' },
    { idempotencyKey: '' },
    { externalTransactionId: '' },
    { roundId: '' },
    { gameId: '' },
    { createdAt: new Date('invalid') },
    { kind: 'other' as K },
  ])
    expect(() => create(o)).toThrow('INVALID_WAGER');
  const date = new Date(createdAt),
    t = create({ createdAt: date });
  date.setUTCFullYear(2000);
  t.createdAt.setUTCFullYear(2001);
  expect(t.createdAt.toISOString()).toBe(createdAt.toISOString());
  expect(Object.isFrozen(t)).toBe(true);
  expect(t.matchesPayload('a'.repeat(64))).toBe(true);
  expect(t.matchesPayload('b'.repeat(64))).toBe(false);
});

test('monetary directions cover BET/WIN/LOSS and inverse BET/WIN/REFUND', () => {
  const b = bet();
  expect(b.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
  expect(create({ kind: K.Win }).ledgerDirectionFor()).toBe(LedgerDirection.Credit);
  expect(create({ kind: K.Loss, money: money('0.00') }).ledgerDirectionFor()).toBeUndefined();
  const refund = reversal(b);
  expect(refund.ledgerDirectionFor(b)).toBe(LedgerDirection.Credit);
  refund.markProcessed(b, at);
  expect(reversal(b, K.Rollback).ledgerDirectionFor(b)).toBe(LedgerDirection.Credit);
  expect(reversal(refund, K.Rollback).ledgerDirectionFor(refund)).toBe(LedgerDirection.Debit);
  const win = create({ kind: K.Win });
  win.markProcessed(undefined, at);
  expect(reversal(win, K.Rollback).ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
});

test('absent and nonterminal references wait; terminal failures and wrong kinds reject immediately', () => {
  const b = create(),
    t = reversal(b);
  expect(t.referenceDecision()).toEqual({ status: 'WAIT' });
  expect(t.referenceDecision(b)).toEqual({ status: 'WAIT' });
  expect(() => t.markProcessed(undefined, at)).toThrow('INVALID_REFERENCE_STATE');
  expect(t.status).toBe(S.Pending);
  b.reject('INSUFFICIENT_FUNDS');
  expect(t.referenceDecision(b)).toEqual({
    status: 'REJECT',
    code: 'REFERENCE_NOT_PROCESSED',
  });
  const failed = create(),
    f = reversal(failed);
  failed.fail('PERMANENT_INFRASTRUCTURE_FAILURE');
  expect(f.referenceDecision(failed)).toEqual({
    status: 'REJECT',
    code: 'REFERENCE_NOT_PROCESSED',
  });
  const loss = create({ kind: K.Loss, money: money('0.00') });
  loss.markProcessed(undefined, at);
  expect(reversal(loss).referenceDecision(loss)).toEqual({
    status: 'REJECT',
    code: 'REFERENCE_KIND_INVALID',
  });
  const opening = Tx.createOpening(props());
  expect(() => opening.reject('INSUFFICIENT_FUNDS')).toThrow('INVALID_TRANSACTION_STATE');
  expect(() => opening.fail('PERMANENT_INFRASTRUCTURE_FAILURE')).toThrow('INVALID_TRANSACTION_STATE');
  opening.markProcessed(undefined, at);
  expect(reversal(opening, K.Rollback).referenceDecision(opening)).toEqual({
    status: 'REJECT',
    code: 'REFERENCE_KIND_INVALID',
  });
});

test('provider/wallet/player/currency/round/external context matches; gameId need not match', () => {
  const b = bet();
  for (const o of [
    { providerId: 'other' },
    { walletId: randomUUID() },
    { playerId: randomUUID() },
    { money: money('25.00', 'USD') },
    { roundId: 'other' },
    { referenceExternalTransactionId: 'other' },
  ])
    expect(reversal(b, K.Refund, o).referenceDecision(b)).toEqual({
      status: 'REJECT',
      code: 'REFERENCE_CONTEXT_MISMATCH',
    });
  expect(reversal(b, K.Refund, { gameId: 'different' }).referenceDecision(b)).toEqual({ status: 'READY' });
  expect(
    create({
      kind: K.Win,
      money: money('10.00'),
      referenceExternalTransactionId: b.externalTransactionId,
    }).referenceDecision(b),
  ).toEqual({ status: 'READY' });
  expect(reversal(b, K.Refund, { money: money('10.00') }).referenceDecision(b)).toEqual({
    status: 'REJECT',
    code: 'REVERSAL_AMOUNT_MISMATCH',
  });
});

test('self-reference and one reversal per type use explicit decisions; rejected identity is terminal', () => {
  const external = 'self',
    t = create({
      kind: K.Refund,
      externalTransactionId: external,
      referenceExternalTransactionId: external,
    });
  expect(t.referenceDecision()).toEqual({
    status: 'REJECT',
    code: 'SELF_REFERENCE',
  });
  const b = bet(),
    r = reversal(b);
  expect(r.referenceDecision(b, true)).toEqual({
    status: 'REJECT',
    code: 'ALREADY_REVERSED',
  });
  r.reject('ALREADY_REVERSED');
  expect(() => r.markProcessed(b, at)).toThrow('INVALID_TRANSACTION_STATE');
  const refund = reversal(b);
  refund.markProcessed(b, at);
  const rollback = reversal(b, K.Rollback);
  expect(rollback.referenceDecision(b, false)).toEqual({ status: 'READY' });
  rollback.markProcessed(b, at);
  expect(b.status).toBe(S.Processed);
  expect(refund.status).toBe(S.Processed);
  expect(rollback.status).toBe(S.Processed);
});

test('initial pending transition signals one event, preserves deadline on polling and can finish', () => {
  const b = create(),
    t = reversal(b),
    deadline = new Date('2026-10-11T20:15:01Z');
  expect(t.markPendingReference(at, deadline)).toBe(true);
  expect(t.status).toBe(S.PendingReference);
  expect(t.markPendingReference(new Date('2026-10-11T20:01:01Z'), new Date('2026-10-11T21:01:01Z'))).toBe(
    false,
  );
  expect(t.referenceExpiresAt?.toISOString()).toBe(deadline.toISOString());
  deadline.setUTCFullYear(2000);
  t.referenceExpiresAt!.setUTCFullYear(2001);
  expect(t.referenceExpiresAt?.toISOString()).toBe('2026-10-11T20:15:01.000Z');
  const restored = Tx.rehydrate(t.toState());
  expect(restored.markPendingReference(at, new Date('2026-10-11T21:00:00Z'))).toBe(false);
  b.markProcessed(undefined, at);
  restored.markProcessed(b, at);
  expect(restored.referenceTransactionId).toBe(b.id);
  expect(restored.processedAt?.toISOString()).toBe(at.toISOString());
  restored.processedAt!.setUTCFullYear(2000);
  expect(restored.processedAt?.toISOString()).toBe(at.toISOString());
});

test('pending only applies to references; invalid times and expiry do not partially transition', () => {
  expect(() => create().markPendingReference(at, new Date('2026-10-11T21:00:00Z'))).toThrow(
    'INVALID_REFERENCE_STATE',
  );
  const t = reversal(create());
  expect(() => t.markPendingReference(at, at)).toThrow('INVALID_REFERENCE_STATE');
  expect(t.status).toBe(S.Pending);
  expect(() => create().markProcessed(undefined, new Date('2000-01-01Z'))).toThrow('INVALID_WAGER');
  expect(() => create().markProcessed(undefined, new Date('invalid'))).toThrow('INVALID_WAGER');
});

test('every terminal state blocks every transition; typed failure categories are distinct', () => {
  for (const terminal of [S.Processed, S.Rejected, S.Failed]) {
    const t = create();
    if (terminal === S.Processed) t.markProcessed(undefined, at);
    else if (terminal === S.Rejected) t.reject('INSUFFICIENT_FUNDS');
    else t.fail('PERMANENT_INFRASTRUCTURE_FAILURE');
    const before = t.toState();
    expect(t.isTerminal()).toBe(true);
    for (const action of [
      () => t.markProcessed(undefined, at),
      () => t.markPendingReference(at, new Date('2026-10-11T21:00:00Z')),
      () => t.reject('INSUFFICIENT_FUNDS'),
      () => t.fail('PERMANENT_INFRASTRUCTURE_FAILURE'),
    ])
      expect(action).toThrow('INVALID_TRANSACTION_STATE');
    expect(t.toState()).toEqual(before);
    expect(Tx.rehydrate(before).toState()).toEqual(before);
  }
  expect(() => create().reject('PERMANENT_INFRASTRUCTURE_FAILURE' as never)).toThrow('INVALID_WAGER');
  expect(() => create().fail('INSUFFICIENT_FUNDS' as never)).toThrow('INVALID_WAGER');
});

test('rehydration rejects contradictory/corrupt persisted states without executing transitions', () => {
  const base = create().toState();
  for (const change of [
    { status: 'UNKNOWN' as S },
    { status: S.Processed },
    { status: S.Rejected },
    { status: S.Failed, failureCode: 'INSUFFICIENT_FUNDS' as FailureCode },
    { status: S.Pending, processedAt: at },
    { status: S.Pending, failureCode: 'INSUFFICIENT_FUNDS' as FailureCode },
    { status: S.PendingReference },
    { referenceTransactionId: randomUUID() },
    {
      pendingReferenceAt: at,
      referenceExpiresAt: new Date('2026-10-11T21:00:00Z'),
    },
  ])
    expect(() => Tx.rehydrate({ ...base, ...change } as State)).toThrow();
  const b = bet(),
    r = reversal(b);
  r.markProcessed(b, at);
  expect(Tx.rehydrate(r.toState()).referenceTransactionId).toBe(b.id);
  expect(() =>
    Tx.rehydrate({
      ...r.toState(),
      referenceTransactionId: undefined,
    } as unknown as State),
  ).toThrow('INVALID_TRANSACTION_STATE');
  expect(() => Tx.rehydrate({ ...r.toState(), processedAt: new Date('invalid') })).toThrow(
    'INVALID_TRANSACTION_STATE',
  );
});

test('self-reference cannot enter pending and processing cannot predate pending entry', () => {
  const self = create({
    kind: K.Refund,
    externalTransactionId: 'self',
    referenceExternalTransactionId: 'self',
  });
  expect(() => self.markPendingReference(at, new Date('2026-10-11T21:00:00Z'))).toThrow('SELF_REFERENCE');
  expect(self.status).toBe(S.Pending);
  const b = bet(),
    t = reversal(b),
    entered = new Date('2026-10-11T20:01:00Z');
  t.markPendingReference(entered, new Date('2026-10-11T20:16:00Z'));
  expect(() => t.markProcessed(b, at)).toThrow('INVALID_WAGER');
  expect(t.status).toBe(S.PendingReference);
  expect(() =>
    Tx.rehydrate({ ...t.toState(), status: S.Processed, referenceTransactionId: b.id, processedAt: at }),
  ).toThrow('INVALID_TRANSACTION_STATE');
});

test('WIN can reference only BET and ROLLBACK cannot target another ROLLBACK', () => {
  const b = bet(),
    r = reversal(b, K.Rollback);
  r.markProcessed(b, at);
  expect(reversal(r, K.Rollback).referenceDecision(r)).toEqual({
    status: 'REJECT',
    code: 'REFERENCE_KIND_INVALID',
  });
  const win = create({ kind: K.Win });
  win.markProcessed(undefined, at);
  const referencedWin = create({ kind: K.Win, referenceExternalTransactionId: win.externalTransactionId });
  expect(referencedWin.referenceDecision(win)).toEqual({ status: 'REJECT', code: 'REFERENCE_KIND_INVALID' });
  expect(() => referencedWin.markProcessed(win, at)).toThrow('REFERENCE_KIND_INVALID');
  expect(referencedWin.status).toBe(S.Pending);
});

test('rehydration cannot create persisted unprocessed OPENING or pending self-reference', () => {
  const opening = Tx.createOpening(props());
  expect(() => Tx.rehydrate(opening.toState())).toThrow('INVALID_TRANSACTION_STATE');
  expect(() => opening.reject('INSUFFICIENT_FUNDS')).toThrow('INVALID_TRANSACTION_STATE');
  expect(() => opening.fail('PERMANENT_INFRASTRUCTURE_FAILURE')).toThrow('INVALID_TRANSACTION_STATE');
  opening.markProcessed(undefined, at);
  expect(Tx.rehydrate(opening.toState()).status).toBe(S.Processed);
  const self = create({
    kind: K.Refund,
    externalTransactionId: 'self',
    referenceExternalTransactionId: 'self',
  });
  expect(() =>
    Tx.rehydrate({
      ...self.toState(),
      status: S.PendingReference,
      pendingReferenceAt: at,
      referenceExpiresAt: new Date('2026-10-11T21:00:00Z'),
    }),
  ).toThrow('INVALID_REFERENCE_STATE');
  const b = bet(),
    t = reversal(b);
  t.markProcessed(b, at);
  const other = Tx.rehydrate({ ...b.toState(), id: randomUUID() });
  expect(t.referenceDecision(other)).toEqual({ status: 'REJECT', code: 'REFERENCE_CONTEXT_MISMATCH' });
});

test('terminal transition failures use InvalidTransactionStateError with the existing code',()=>{
  const transaction=create(); transaction.markProcessed(undefined,new Date(Date.now()+1000));
  for(const transition of [()=>transaction.reject('INSUFFICIENT_FUNDS'),()=>transaction.fail('PERMANENT_INFRASTRUCTURE_FAILURE'),
    ()=>transaction.markProcessed(undefined,new Date())]) {
    try {transition();throw new Error('transition unexpectedly succeeded');}
    catch(error){expect(error).toBeInstanceOf(InvalidTransactionStateError);expect(error).toMatchObject({code:'INVALID_TRANSACTION_STATE'});}
  }
});
