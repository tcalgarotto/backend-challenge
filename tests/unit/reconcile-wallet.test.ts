import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { ReconcileWallet, type ReconciliationSnapshot } from '../../src/wallet/application/reconcile-wallet.js';
import { ReconciliationSignals } from '../../src/wallet/infrastructure/reconciliation-signals.js';
import { formatMinorUnits, parseMinorUnits } from '../../src/shared/domain/exact-decimal.js';

const walletId = randomUUID();
function useCase(snapshot: Partial<ReconciliationSnapshot> | undefined, lines: string[] = []) {
  const signals = new ReconciliationSignals(line => { lines.push(line); });
  const calls: string[] = [];
  const reconcile = new ReconcileWallet({ async snapshot(id) {
    calls.push(id);
    return snapshot && { walletId, currency: 'BRL', storedBalance: '0.00', ledgerTotal: '0.00', checkedEntries: 0, ...snapshot };
  } }, signals);
  return { reconcile, signals, calls, lines };
}

test('exact decimals are unbounded, signed and never pass through Number', () => {
  expect(parseMinorUnits('0.00')).toBe(0n);
  expect(parseMinorUnits('-25.10')).toBe(-2510n);
  const huge = '1999999999999999999999.98';
  expect(formatMinorUnits(parseMinorUnits(huge))).toBe(huge);
  expect(formatMinorUnits(-1n)).toBe('-0.01');
  expect(formatMinorUnits(parseMinorUnits('0.01') + parseMinorUnits('0.02'))).toBe('0.03');
  for (const bad of ['1', '1.0', '01.00', '-0.00', '+1.00', '1e2', ' 1.00', 'NaN']) expect(() => parseMinorUnits(bad)).toThrow();
});

test('consistent wallet: zero difference, counted metric and no divergence log', async () => {
  const { reconcile, signals, lines } = useCase({ storedBalance: '75.00', ledgerTotal: '75.00', checkedEntries: 2 });
  expect(await reconcile.execute(walletId.toUpperCase(), undefined, 'corr-1')).toEqual({
    walletId, storedBalance: { amount: '75.00', currency: 'BRL' }, calculatedBalance: { amount: '75.00', currency: 'BRL' },
    difference: { amount: '0.00', currency: 'BRL' }, consistent: true, checkedEntries: 2,
  });
  expect(lines).toEqual([]);
  expect(signals.metrics()).toEqual([{ name: 'jwp_wallet_reconciliations_total', labels: { outcome: 'consistent', currency: 'BRL' }, value: 1 }]);
});

test('divergence is reported with signed difference, logged and counted, never repaired', async () => {
  const { reconcile, signals, lines } = useCase({ storedBalance: '10.00', ledgerTotal: '35.50', checkedEntries: 3 });
  const result = await reconcile.execute(walletId, {}, 'corr-2');
  expect(result).toMatchObject({ consistent: false, difference: { amount: '-25.50', currency: 'BRL' },
    storedBalance: { amount: '10.00' }, calculatedBalance: { amount: '35.50' } });
  expect(await reconcile.execute(walletId, {}, 'corr-3')).toMatchObject({ consistent: false });
  expect(lines).toHaveLength(2);
  const log = JSON.parse(lines[0]!);
  expect(log).toMatchObject({ level: 'error', type: 'wallet.reconciliation.divergent', correlationId: 'corr-2',
    walletId, currency: 'BRL', checkedEntries: 3, repaired: false });

  expect(lines[0]).not.toContain('10.00');
  expect(lines[0]).not.toContain('35.50');
  const metrics = signals.metrics();
  expect(metrics).toEqual([{ name: 'jwp_wallet_reconciliations_total', labels: { outcome: 'divergent', currency: 'BRL' }, value: 2 }]);
  expect(JSON.stringify(metrics)).not.toContain(walletId);
});

test('calculated total beyond one balance range is exact; positive difference keeps no sign', async () => {
  const { reconcile } = useCase({ storedBalance: '999999999999999999.99', ledgerTotal: '1999999999999999999.98', checkedEntries: 3 });
  expect(await reconcile.execute(walletId, undefined)).toMatchObject({
    calculatedBalance: { amount: '1999999999999999999.98' }, difference: { amount: '-999999999999999999.99' }, consistent: false });
  const positive = useCase({ storedBalance: '5.00', ledgerTotal: '-1.00', checkedEntries: 1 });
  expect(await positive.reconcile.execute(walletId, undefined)).toMatchObject({ difference: { amount: '6.00' }, consistent: false });
});

test('correction commands, invalid IDs and missing wallets fail typed before or without effects', async () => {
  const { reconcile, calls } = useCase({});
  for (const body of [{ repair: true }, { storedBalance: '1.00' }, [], 'fix', null])
    await expect(reconcile.execute(walletId, body)).rejects.toMatchObject({ code: 'INVALID_PAYLOAD', httpStatus: 400 });
  await expect(reconcile.execute('nope', undefined)).rejects.toMatchObject({ code: 'INVALID_PAYLOAD' });
  expect(calls).toEqual([]);
  await expect(useCase(undefined).reconcile.execute(walletId, undefined)).rejects.toMatchObject({ code: 'WALLET_NOT_FOUND', httpStatus: 404 });
});
