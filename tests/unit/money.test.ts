import { describe, expect, test } from 'bun:test';
import { Money, type MoneyProps } from '../../src/shared/domain/money.js';

const brl = (amount: string): Money => Money.from({ amount, currency: 'BRL' });

describe('JWP-CASE-001: Money: exact, immutable, two-decimal value', () => {
  test('JWP-TST-U01: adds cents exactly and keeps operands intact', () => {
    const left = brl('0.10');
    expect(left.add(brl('0.20')).toString()).toBe('0.30');
    expect(left.toString()).toBe('0.10');
    expect(Object.isFrozen(left)).toBe(true);
  });

  test('JWP-TST-U02: handles values above the JavaScript safe integer limit', () => {
    expect(brl('9007199254740993.01').add(brl('0.01')).toString())
      .toBe('9007199254740993.02');
  });

  test('JWP-TST-U03: serializes zero, cents and signed internal results without bigint leaking', () => {
    expect(Money.zero('BRL').toString()).toBe('0.00');
    expect(brl('1.00').subtract(brl('1.01')).toString()).toBe('-0.01');
    expect(brl('0.01').negate().negate().equals(brl('0.01'))).toBe(true);
    expect(JSON.stringify(brl('25.00'))).toBe('{"amount":"25.00","currency":"BRL"}');
  });

  test.each([
    '', ' ', 'NaN', 'Infinity', '1e2', '1E+2', '-1.00', '-0.00',
    '1', '1.0', '1.001', '.50', '01.00', '+1.00', '1,00',
    ' 1.00', '1.00 ', '1.00\n', '1.00\r\n', '1000000000000000000.00',
  ])('JWP-TST-U20: rejects invalid external amount %j without rounding', (amount) => {
    expect(() => brl(amount)).toThrow('INVALID_MONEY');
  });

  test('JWP-TST-U04: rejects numeric JSON input at the boundary', () => {
    const malformed = { amount: 25, currency: 'BRL' } as unknown as MoneyProps;
    expect(() => Money.from(malformed)).toThrow('INVALID_MONEY');
  });

  test('JWP-TST-U05: bounds arithmetic to the selected SQL monetary representation', () => {
    expect(brl('999999999999999999.99').toString()).toBe('999999999999999999.99');
    expect(() => brl('999999999999999999.99').add(brl('0.01')))
      .toThrow('MONEY_OUT_OF_RANGE');
  });

  test('JWP-TST-U06: checks signs, equality and ordering', () => {
    expect(brl('0.01').isPositive()).toBe(true);
    expect(brl('0.01').negate().isNegative()).toBe(true);
    expect(brl('0.00').isZero()).toBe(true);
    expect(brl('1.00').isLessThan(brl('2.00'))).toBe(true);
    expect(brl('1.00').equals(brl('1.01'))).toBe(false);
  });

  test('JWP-TST-U07: rejects mixed currencies for arithmetic and comparisons', () => {
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    for (const operation of ['add', 'subtract', 'equals', 'isLessThan'] as const) {
      expect(() => brl('1.00')[operation](usd)).toThrow('CURRENCY_MISMATCH');
    }
  });

  test('JWP-TST-U08: limits the supported ISO currency subset explicitly', () => {
    expect(() => Money.from({ amount: '1.00', currency: 'XYZ' }))
      .toThrow('UNSUPPORTED_CURRENCY');
    expect(() => Money.zero('brl')).toThrow('UNSUPPORTED_CURRENCY');
    expect(Money.zero('EUR').currency).toBe('EUR');
  });

  test('JWP-TST-U09: rehydrates signed internal values without exposing a negative input contract', () => {
    expect(Money.rehydrate({ amount: '-0.01', currency: 'BRL' }).toString()).toBe('-0.01');
    expect(Money.rehydrate({ amount: '-0.00', currency: 'BRL' }).toString()).toBe('0.00');
    expect(() => Money.rehydrate({ amount: 'NaN', currency: 'BRL' })).toThrow('INVALID_MONEY');
  });
});
