import { DomainError } from '../../shared/domain/domain-error.js';
import { LedgerDirection } from './wallet-ledger-entry.js';
import { Money } from '../../shared/domain/money.js';

export interface WalletState {
  readonly id: string;
  readonly playerId: string;
  readonly currency: string;
  readonly balance: Money;
  readonly version: number;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}
export interface WalletMovement {
  readonly direction: LedgerDirection;
  readonly money: Money;
  readonly balanceBefore: Money;
  readonly balanceAfter: Money;
  readonly walletVersion: number;
}
const timestamp = (at: Date): number => {
  if (!(at instanceof Date) || !Number.isFinite(at.getTime())) throw new DomainError('INVALID_WALLET_STATE');
  return at.getTime();
};

/** SQL coordena lock e ledger durável; o agregado de domínio permanece puro. */
export class Wallet {
  #balance: Money;
  #version: number;
  readonly #createdAt: number;
  #updatedAt: number;
  private constructor(public readonly id: string, public readonly playerId: string,
    public readonly currency: string, balance: Money, version: number, createdAt: Date, updatedAt: Date) {
    this.#balance = balance; this.#version = version;
    this.#createdAt = timestamp(createdAt); this.#updatedAt = timestamp(updatedAt);
    Object.freeze(this);
  }
  static open(props: { id: string; playerId: string; initialBalance: Money; createdAt?: Date }): Wallet {
    if (!props.id || !props.playerId || props.initialBalance.isNegative()) throw new DomainError('INVALID_WALLET_STATE');
    const at = props.createdAt ?? new Date();
    return new Wallet(props.id, props.playerId, props.initialBalance.currency, props.initialBalance, 1, at, at);
  }
  static rehydrate(state: WalletState): Wallet {
    // Integridade do estado persistido não aplica débito, crédito nem incremento de versão.
    if (!state.id || !state.playerId || !(state.balance instanceof Money) || state.balance.isNegative()
      || state.currency !== state.balance.currency || !Number.isSafeInteger(state.version) || state.version < 1
      || timestamp(state.updatedAt) < timestamp(state.createdAt)) throw new DomainError('INVALID_WALLET_STATE');
    return new Wallet(state.id, state.playerId, state.currency, state.balance, state.version, state.createdAt, state.updatedAt);
  }
  get balance(): Money { return this.#balance; }
  get version(): number { return this.#version; }
  get createdAt(): Date { return new Date(this.#createdAt); }
  get updatedAt(): Date { return new Date(this.#updatedAt); }
  debit(money: Money, at: Date = new Date()): WalletMovement {
    this.assertMovement(money);
    if (this.#balance.isLessThan(money)) throw new DomainError('INSUFFICIENT_FUNDS');
    return this.move(LedgerDirection.Debit, money, this.#balance.subtract(money), at);
  }
  credit(money: Money, at: Date = new Date()): WalletMovement {
    this.assertMovement(money);
    return this.move(LedgerDirection.Credit, money, this.#balance.add(money), at);
  }
  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) throw new DomainError('CURRENCY_MISMATCH');
  }
  private assertMovement(money: Money): void {
    this.assertSameCurrency(money);
    if (!money.isPositive()) throw new DomainError('INVALID_MOVEMENT_AMOUNT');
  }
  private move(direction: LedgerDirection, money: Money, after: Money, at: Date): WalletMovement {
    // Diferenças de relógio entre instâncias não podem recuar updatedAt.
    const updatedAt = Math.max(timestamp(at), this.#updatedAt);
    const before = this.#balance;
    this.#balance = after; this.#version += 1; this.#updatedAt = updatedAt;
    return Object.freeze({ direction, money, balanceBefore: before, balanceAfter: after, walletVersion: this.#version });
  }
}
