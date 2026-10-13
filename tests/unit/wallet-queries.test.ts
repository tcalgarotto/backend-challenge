import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  WalletQueries, decodeLedgerCursor, encodeLedgerCursor, ledgerLimit,
  type LedgerEntryView, type LedgerPosition, type WalletQueryStore,
} from '../../src/wallet/application/wallet-queries.js';

const walletId = randomUUID();
const entry = (walletVersion: number): LedgerEntryView => ({
  id: randomUUID(), walletId, transactionId: randomUUID(), walletVersion, direction: 'CREDIT',
  money: { amount: '1.00', currency: 'BRL' }, balanceBefore: { amount: `${walletVersion - 1}.00`, currency: 'BRL' },
  balanceAfter: { amount: `${walletVersion}.00`, currency: 'BRL' }, createdAt: new Date(0).toISOString(),
});

function store(ledger: LedgerEntryView[], calls: { after?: LedgerPosition; limit: number }[] = []): WalletQueryStore {
  return {
    async findWallet() { return undefined; },
    async listLedger(id, after, limit) {
      calls.push({ ...(after ? { after } : {}), limit });
      if (id !== walletId) return undefined;
      const sorted = [...ledger].sort((a, b) => a.walletVersion - b.walletVersion || a.id.localeCompare(b.id));
      return sorted.filter(e => !after || e.walletVersion > after.walletVersion ||
        (e.walletVersion === after.walletVersion && e.id > after.id)).slice(0, limit);
    },
  };
}

test('cursor is opaque base64url, bound to its wallet and rejects any non-canonical form', () => {
  const position = { walletVersion: 7, id: randomUUID() };
  const cursor = encodeLedgerCursor(walletId, position);
  expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
  expect(cursor).not.toContain(walletId);
  expect(decodeLedgerCursor(walletId, cursor)).toEqual(position);
  const invalid = [
    '', 'not base64!', encodeLedgerCursor(randomUUID(), position), cursor + '=',
    Buffer.from(JSON.stringify({ v: 2, walletId, walletVersion: 7, id: position.id })).toString('base64url'),
    Buffer.from(JSON.stringify({ v: 1, walletId, walletVersion: 0, id: position.id })).toString('base64url'),
    Buffer.from(JSON.stringify({ v: 1, walletId, walletVersion: '7', id: position.id })).toString('base64url'),
    Buffer.from(JSON.stringify({ v: 1, walletId, walletVersion: 2 ** 53, id: position.id })).toString('base64url'),
    Buffer.from(JSON.stringify({ v: 1, walletId, walletVersion: 7, id: position.id, offset: 0 })).toString('base64url'),
    Buffer.from(JSON.stringify({ v: 1, walletId, walletVersion: 7, id: 'x' })).toString('base64url'),
    Buffer.from(JSON.stringify({ walletVersion: 7, v: 1, walletId, id: position.id })).toString('base64url'),
    Buffer.from(JSON.stringify({ v: 1, walletId: walletId.toUpperCase(), walletVersion: 7, id: position.id })).toString('base64url'),
    Buffer.from('{"v":1').toString('base64url'),
  ];
  for (const value of invalid) expect(() => decodeLedgerCursor(walletId, value)).toThrow(expect.objectContaining({ code: 'INVALID_CURSOR', httpStatus: 400 }));
});

test('limit defaults to 50 and accepts only canonical integers 1..100', () => {
  expect(ledgerLimit(undefined)).toBe(50);
  expect(ledgerLimit('1')).toBe(1);
  expect(ledgerLimit('100')).toBe(100);
  for (const value of ['0', '101', '-1', '1.5', '01', ' 5', '1\n', '1\r', '1\u2028', '1e2', '', 'abc', ['5'], 5])
    expect(() => ledgerLimit(value)).toThrow(expect.objectContaining({ code: 'INVALID_PAYLOAD', httpStatus: 400 }));
});

test('pages never duplicate or skip entries while new versions are appended', async () => {
  const ledger = [1, 2, 3, 4, 5].map(entry), calls: { after?: LedgerPosition; limit: number }[] = [];
  const queries = new WalletQueries(store(ledger, calls));
  const seen: number[] = [];
  let page = await queries.listLedger(walletId, { limit: '2' });
  seen.push(...page.items.map(e => e.walletVersion));
  ledger.push(entry(6), entry(7));
  while (page.nextCursor) {
    page = await queries.listLedger(walletId.toUpperCase(), { cursor: page.nextCursor, limit: '2' });
    seen.push(...page.items.map(e => e.walletVersion));
  }
  expect(seen).toEqual([1, 2, 3, 4, 5, 6, 7]);

  expect(calls.every(call => call.limit === 3)).toBe(true);
  expect(page.nextCursor).toBeNull();
});

test('exact last page and empty ledger return null cursor; typed errors precede the store', async () => {
  const calls: { limit: number }[] = [];
  const queries = new WalletQueries(store([1, 2].map(entry), calls));
  expect((await queries.listLedger(walletId, { limit: '2' })).nextCursor).toBeNull();
  expect(await new WalletQueries(store([])).listLedger(walletId, {})).toEqual({ items: [], nextCursor: null });
  const before = calls.length;
  await expect(queries.listLedger('not-a-uuid', {})).rejects.toMatchObject({ code: 'INVALID_PAYLOAD' });
  await expect(queries.listLedger(walletId, { cursor: ['a', 'b'] })).rejects.toMatchObject({ code: 'INVALID_CURSOR' });
  await expect(queries.listLedger(walletId, { cursor: encodeLedgerCursor(randomUUID(), { walletVersion: 1, id: randomUUID() }) }))
    .rejects.toMatchObject({ code: 'INVALID_CURSOR', httpStatus: 400 });
  expect(calls.length).toBe(before);
  await expect(queries.listLedger(randomUUID(), {})).rejects.toMatchObject({ code: 'WALLET_NOT_FOUND', httpStatus: 404 });
  await expect(queries.getWallet(randomUUID())).rejects.toMatchObject({ code: 'WALLET_NOT_FOUND', httpStatus: 404 });
  await expect(queries.getWallet(`${walletId} `)).rejects.toMatchObject({ code: 'INVALID_PAYLOAD', httpStatus: 400 });
});
