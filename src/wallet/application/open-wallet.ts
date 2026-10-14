import { randomUUID } from 'node:crypto';
import { Money, type MoneyProps } from '../../shared/domain/money.js';
import { isEnabledCurrency } from '../../shared/domain/currency.js';
import { Wallet } from '../domain/wallet.js';

export class WalletOpeningError extends Error {
  constructor(readonly code: 'INVALID_PAYLOAD' | 'UNSUPPORTED_CURRENCY' | 'WALLET_ALREADY_EXISTS') {
    super(code);
    this.name = 'WalletOpeningError';
  }
}
export interface WalletOpeningContext {
  readonly correlationId: string;
  readonly occurredAt: Date;
}
export interface WalletOpeningStore {
  /** Só retorna após confirmar wallet, abertura, ledger e eventos na mesma TX. */
  create(wallet: Wallet, context: WalletOpeningContext): Promise<void>;
}
export interface OpenWalletResult {
  readonly id: string;
  readonly playerId: string;
  readonly balance: MoneyProps;
  readonly version: number;
}
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

/** O mapeamento HTTP fica fora da aplicação, preservando independência de transporte. */
export class OpenWallet {
  constructor(private readonly store: WalletOpeningStore) {}

  async execute(input: unknown, correlationId: string = randomUUID()): Promise<OpenWalletResult> {
    if (!record(input) || Object.keys(input).length !== 2 ||
        typeof input['playerId'] !== 'string' || !uuid.test(input['playerId']) ||
        !record(input['initialBalance']) || Object.keys(input['initialBalance']).length !== 2 ||
        typeof input['initialBalance']['amount'] !== 'string' || typeof input['initialBalance']['currency'] !== 'string' ||
        typeof correlationId !== 'string' || !correlationId.trim() || correlationId.length > 128) {
      throw new WalletOpeningError('INVALID_PAYLOAD');
    }
    // Moeda fora do registro ou da allowlist tem código próprio; formato inválido continua INVALID_PAYLOAD.
    const currency = input['initialBalance']['currency'];
    if (!isEnabledCurrency(currency)) throw new WalletOpeningError('UNSUPPORTED_CURRENCY');
    let balance: Money;
    try {
      balance = Money.from({ amount: input['initialBalance']['amount'], currency });
    } catch { throw new WalletOpeningError('INVALID_PAYLOAD'); }
    const at = new Date();
    const wallet = Wallet.open({ id: randomUUID(), playerId: input['playerId'].toLowerCase(), initialBalance: balance, createdAt: at });
    await this.store.create(wallet, { correlationId, occurredAt: at });
    return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
  }
}
