import type { MoneyProps } from '../../shared/domain/money.js';
import { isExactUuid } from '../../shared/domain/identifiers.js';

export type WalletReadErrorCode = 'INVALID_PAYLOAD' | 'INVALID_CURSOR' | 'WALLET_NOT_FOUND';
export class WalletReadError extends Error {
  readonly httpStatus: number;
  constructor(readonly code: WalletReadErrorCode) {
    super(code);
    this.name = 'WalletReadError';
    this.httpStatus = code === 'WALLET_NOT_FOUND' ? 404 : 400;
  }
}
export interface WalletView {
  readonly id: string;
  readonly playerId: string;
  readonly balance: MoneyProps;
  readonly version: number;
  readonly createdAt: string;
  readonly updatedAt: string;
}
export interface LedgerEntryView {
  readonly id: string;
  readonly walletId: string;
  readonly transactionId: string;
  readonly walletVersion: number;
  readonly direction: 'CREDIT' | 'DEBIT';
  readonly money: MoneyProps;
  readonly balanceBefore: MoneyProps;
  readonly balanceAfter: MoneyProps;
  readonly createdAt: string;
}
export interface LedgerPage {
  readonly items: readonly LedgerEntryView[];
  readonly nextCursor: string | null;
}
/** A paginação usa posição estável por walletVersion/id, evitando deslocar páginas antigas. */
export interface LedgerPosition {
  readonly walletVersion: number;
  readonly id: string;
}
export interface WalletQueryStore {
  findWallet(walletId: string): Promise<WalletView | undefined>;

  listLedger(walletId: string, after: LedgerPosition | undefined, limit: number): Promise<LedgerEntryView[] | undefined>;
}

export const defaultLedgerLimit = 50;
export const maximumLedgerLimit = 100;
const base64url = /^[A-Za-z0-9_-]+$/;
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

export function walletIdentity(value: unknown): string {
  if (!isExactUuid(value)) throw new WalletReadError('INVALID_PAYLOAD');
  return value.toLowerCase();
}

/** A posição opaca é vinculada à wallet; não codifica offset. */
export function encodeLedgerCursor(walletId: string, position: LedgerPosition): string {
  return Buffer.from(JSON.stringify({ v: 1, walletId, walletVersion: position.walletVersion, id: position.id }), 'utf8').toString('base64url');
}

export function decodeLedgerCursor(walletId: string, cursor: string): LedgerPosition {
  let decoded: unknown;
  try {
    if (!base64url.test(cursor) || cursor.length > 512) throw new Error('cursor');
    decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch { throw new WalletReadError('INVALID_CURSOR'); }
  if (!record(decoded) || Object.keys(decoded).length !== 4 || decoded['v'] !== 1 ||
      !isExactUuid(decoded['walletId']) || !isExactUuid(decoded['id']) ||
      !Number.isSafeInteger(decoded['walletVersion']) || (decoded['walletVersion'] as number) < 1)
    throw new WalletReadError('INVALID_CURSOR');
  const position = { walletVersion: decoded['walletVersion'] as number, id: (decoded['id'] as string).toLowerCase() };
  // Recusa cursor de outra wallet ou codificação não canônica.
  if (decoded['walletId'] !== walletId || encodeLedgerCursor(walletId, position) !== cursor)
    throw new WalletReadError('INVALID_CURSOR');
  return position;
}

export function ledgerLimit(value: unknown): number {
  if (value === undefined) return defaultLedgerLimit;
  if (typeof value !== 'string' || !/^[1-9][0-9]{0,2}$/.test(value) || Number(value) > maximumLedgerLimit)
    throw new WalletReadError('INVALID_PAYLOAD');
  return Number(value);
}

export class WalletQueries {
  constructor(private readonly store: WalletQueryStore) {}

  async getWallet(walletId: unknown): Promise<WalletView> {
    const wallet = await this.store.findWallet(walletIdentity(walletId));
    if (!wallet) throw new WalletReadError('WALLET_NOT_FOUND');
    return wallet;
  }

  async listLedger(walletId: unknown, query: { cursor?: unknown; limit?: unknown }): Promise<LedgerPage> {
    const id = walletIdentity(walletId);
    if (query.cursor !== undefined && typeof query.cursor !== 'string') throw new WalletReadError('INVALID_CURSOR');
    const after = query.cursor === undefined ? undefined : decodeLedgerCursor(id, query.cursor);
    const limit = ledgerLimit(query.limit);
    // A linha extra evita COUNT para descobrir a próxima página.
    const entries = await this.store.listLedger(id, after, limit + 1);
    if (!entries) throw new WalletReadError('WALLET_NOT_FOUND');
    const items = entries.slice(0, limit);
    const last = items.at(-1);
    return { items, nextCursor: entries.length > limit && last ? encodeLedgerCursor(id, last) : null };
  }
}
