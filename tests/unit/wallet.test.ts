import { LedgerDirection } from '../../src/wallet/domain/wallet-ledger-entry.js';
import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/shared/domain/money.js';
import { Wallet } from '../../src/wallet/domain/wallet.js';

const brl = (amount: string): Money => Money.from({ amount, currency: 'BRL' });
const open = (): Wallet => Wallet.open({ id: 'wallet-1', playerId: 'player-1', initialBalance: brl('100.00') });

describe('JWP-CASE-002: Wallet: local business invariants (not a distributed locking test)', () => {
  test('JWP-TST-U21: starts at version one and returns balanced movement data', () => {
    const wallet = open();
    expect(wallet.version).toBe(1);
    const movement = wallet.debit(brl('80.00'));
    expect(movement.balanceBefore.toString()).toBe('100.00');
    expect(movement.balanceAfter.toString()).toBe('20.00');
    expect(movement.direction).toBe(LedgerDirection.Debit);
    expect(movement.walletVersion).toBe(2);
    expect(movement.balanceBefore.subtract(movement.money).equals(movement.balanceAfter)).toBe(true);
  });

  test('JWP-TST-U22: rejects a debit without changing balance or version', () => {
    const wallet = open();
    wallet.debit(brl('80.00'));
    expect(() => wallet.debit(brl('80.00'))).toThrow('INSUFFICIENT_FUNDS');
    expect(wallet.balance.toString()).toBe('20.00');
    expect(wallet.version).toBe(2);
  });

  test('JWP-TST-U23: allows debit to exactly zero and increments version on credit', () => {
    const wallet = open();
    wallet.debit(brl('100.00'));
    expect(wallet.balance.isZero()).toBe(true);
    const movement = wallet.credit(brl('0.01'));
    expect(movement.direction).toBe(LedgerDirection.Credit);
    expect(wallet.balance.toString()).toBe('0.01');
    expect(wallet.version).toBe(3);
  });

  test('JWP-TST-U24: rejects negative opening and missing identity', () => {
    expect(() => Wallet.open({ id: 'w', playerId: 'p', initialBalance: brl('1.00').negate() }))
      .toThrow('INVALID_WALLET_STATE');
    expect(() => Wallet.open({ id: '', playerId: 'p', initialBalance: brl('0.00') }))
      .toThrow('INVALID_WALLET_STATE');
  });

  test('JWP-TST-U25: permits a zero opening with version one', () => {
    const wallet = Wallet.open({ id: 'w', playerId: 'p', initialBalance: brl('0.00') });
    expect(wallet.balance.isZero()).toBe(true);
    expect(wallet.version).toBe(1);
  });

  test('JWP-TST-U26: rejects zero, negative and cross-currency movements without mutation', () => {
    const wallet = open();
    expect(() => wallet.credit(brl('0.00'))).toThrow('INVALID_MOVEMENT_AMOUNT');
    expect(() => wallet.debit(brl('0.01').negate())).toThrow('INVALID_MOVEMENT_AMOUNT');
    expect(() => wallet.credit(Money.from({ amount: '1.00', currency: 'USD' })))
      .toThrow('CURRENCY_MISMATCH');
    expect(wallet.version).toBe(1);
    expect(wallet.balance.toString()).toBe('100.00');
  });

  test('JWP-TST-U27: a credit overflow does not partially mutate the wallet', () => {
    const wallet = Wallet.open({ id: 'w', playerId: 'p', initialBalance: brl('999999999999999999.99') });
    expect(() => wallet.credit(brl('0.01'))).toThrow('MONEY_OUT_OF_RANGE');
    expect(wallet.balance.toString()).toBe('999999999999999999.99');
    expect(wallet.version).toBe(1);
  });

  test('JWP-TST-U28: rehydrates a persisted version without applying movements', () => {
    const wallet = Wallet.rehydrate({ id: 'w', playerId: 'p', balance: brl('20.00'), version: 9, currency: 'BRL', createdAt: new Date(1000), updatedAt: new Date(2000) });
    expect(wallet.version).toBe(9);
    expect(wallet.balance.toString()).toBe('20.00');
  });
});

test('wallet timestamps are restored, immutable to callers, monotonic and change only with balance', () => {
  const createdAt=new Date(1000), changedAt=new Date(2000);
  const w=Wallet.open({id:'w',playerId:'p',initialBalance:brl('100.00'),createdAt});
  createdAt.setTime(0); w.createdAt.setTime(0);
  expect(w.createdAt.getTime()).toBe(1000); expect(w.updatedAt.getTime()).toBe(1000);
  w.debit(brl('25.00'),changedAt); changedAt.setTime(0); w.updatedAt.setTime(0);
  expect(w.updatedAt.getTime()).toBe(2000);
  expect(()=>w.debit(brl('80.00'),new Date(3000))).toThrow('INSUFFICIENT_FUNDS');
  expect(w.updatedAt.getTime()).toBe(2000); expect(w.version).toBe(2);
  w.credit(brl('1.00'),new Date(1500)); expect(w.updatedAt.getTime()).toBe(2000);
  const restored=Wallet.rehydrate({id:w.id,playerId:w.playerId,currency:w.currency,balance:w.balance,
    version:w.version,createdAt:w.createdAt,updatedAt:w.updatedAt});
  expect(restored.updatedAt.getTime()).toBe(2000); expect(restored.balance.toString()).toBe('76.00');
  expect(()=>Wallet.rehydrate({id:'w',playerId:'p',currency:'USD',balance:brl('1.00'),version:1,
    createdAt:new Date(1000),updatedAt:new Date(0)})).toThrow('INVALID_WALLET_STATE');
});
