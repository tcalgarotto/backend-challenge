import { Money } from '../../shared/domain/money.js';
import { isExactUuid } from '../../shared/domain/identifiers.js';

export enum LedgerDirection { Debit = 'DEBIT', Credit = 'CREDIT' }
export interface LedgerEntryState {
  readonly id: string;
  readonly walletId: string;
  readonly transactionId: string;
  readonly direction: LedgerDirection;
  readonly money: Money;
  readonly balanceBefore: Money;
  readonly balanceAfter: Money;
  readonly createdAt: Date;
}
export type CreateLedgerEntryProps = LedgerEntryState;

/** Fábricas do ledger validam a mesma equação das constraints SQL. */
export class WalletLedgerEntry {
  readonly id: string;
  readonly walletId: string;
  readonly transactionId: string;
  readonly direction: LedgerDirection;
  readonly money: Money;
  readonly balanceBefore: Money;
  readonly balanceAfter: Money;
  readonly #createdAt: number;
  private constructor(p: LedgerEntryState) {
    this.id = p.id; this.walletId = p.walletId; this.transactionId = p.transactionId;
    this.direction = p.direction; this.money = p.money;
    this.balanceBefore = p.balanceBefore; this.balanceAfter = p.balanceAfter;
    this.#createdAt = p.createdAt.getTime(); Object.freeze(this);
  }
  private static assertIntegrity(p: LedgerEntryState): void {
    if (![p.id,p.walletId,p.transactionId].every(isExactUuid) || !Object.values(LedgerDirection).includes(p.direction)
      || !(p.createdAt instanceof Date) || !Number.isFinite(p.createdAt.getTime())
      || ![p.money,p.balanceBefore,p.balanceAfter].every(m => m instanceof Money)
      || !p.money.isPositive() || p.balanceBefore.isNegative() || p.balanceAfter.isNegative()
      || p.money.currency !== p.balanceBefore.currency || p.money.currency !== p.balanceAfter.currency) {
      throw new Error('INVALID_LEDGER_ENTRY');
    }
    const expected = p.direction === LedgerDirection.Debit ? p.balanceBefore.subtract(p.money) : p.balanceBefore.add(p.money);
    if (!expected.equals(p.balanceAfter)) throw new Error('UNBALANCED_LEDGER_ENTRY');
  }
  static create(p: CreateLedgerEntryProps): WalletLedgerEntry {
    this.assertIntegrity(p);
    return new WalletLedgerEntry(p);
  }
  static rehydrate(p: LedgerEntryState): WalletLedgerEntry {
    // A equação é integridade do registro, não uma nova movimentação da wallet.
    this.assertIntegrity(p);
    return new WalletLedgerEntry(p);
  }
  get createdAt(): Date { return new Date(this.#createdAt); }
  isBalanced(): boolean {
    const expected = this.direction === LedgerDirection.Debit
      ? this.balanceBefore.subtract(this.money) : this.balanceBefore.add(this.money);
    return expected.equals(this.balanceAfter);
  }
}
