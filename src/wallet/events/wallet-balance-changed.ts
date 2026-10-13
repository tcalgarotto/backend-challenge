import { LedgerDirection } from '../domain/wallet-ledger-entry.js';
import {
  IntegrationEvent,
  InvalidEventError,
  type EventContext,
} from '../../shared/events/integration-event.js';
import type { MoneyProps } from '../../shared/domain/money.js';
import type { Wallet, WalletMovement } from '../domain/wallet.js';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../wagering/domain/wager-transaction.js';

export interface WalletBalanceChangedData {
  readonly walletId: string;
  readonly transactionId: string;
  readonly direction: LedgerDirection;
  readonly money: MoneyProps;
  readonly balanceBefore: MoneyProps;
  readonly balanceAfter: MoneyProps;
  readonly walletVersion: number;
}
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;
  private constructor(w: Wallet, t: WagerTransaction, m: WalletMovement, ctx: EventContext) {
    super({
      ...ctx,
      aggregateId: w.id,
      data: {
        walletId: w.id,
        transactionId: t.id,
        direction: m.direction,
        money: m.money.toJSON(),
        balanceBefore: m.balanceBefore.toJSON(),
        balanceAfter: m.balanceAfter.toJSON(),
        walletVersion: m.walletVersion,
      },
    });
    Object.freeze(this);
  }
  static from(
    w: Wallet,
    t: WagerTransaction,
    m: WalletMovement,
    ctx: EventContext,
    reference?: WagerTransaction,
  ): WalletBalanceChanged {
    if (
      t.status !== WagerTransactionStatus.Processed ||
      !t.affectsBalance() ||
      w.id !== t.walletId ||
      w.playerId !== t.playerId ||
      w.currency !== t.money.currency ||
      !m.money.isPositive() ||
      m.money.currency !== w.currency ||
      m.balanceBefore.currency !== w.currency ||
      m.balanceAfter.currency !== w.currency ||
      m.balanceBefore.isNegative() ||
      m.balanceAfter.isNegative() ||
      !Number.isSafeInteger(m.walletVersion) ||
      m.walletVersion < 1 ||
      m.walletVersion !== w.version ||
      !m.balanceAfter.equals(w.balance) ||
      !m.money.equals(t.money) ||
      (reference && t.referenceTransactionId !== reference.id) ||
      m.direction !== t.ledgerDirectionFor(reference) ||
      ctx.occurredAt.getTime() !== t.processedAt?.getTime()
    )
      throw new InvalidEventError();
    const expected =
      m.direction === LedgerDirection.Credit ? m.balanceBefore.add(m.money) : m.balanceBefore.subtract(m.money);
    if (
      !expected.equals(m.balanceAfter) ||
      m.balanceBefore.equals(m.balanceAfter) ||
      (t.kind === WagerTransactionKind.Opening && (m.walletVersion !== 1 || !m.balanceBefore.isZero())) ||
      (t.kind !== WagerTransactionKind.Opening && m.walletVersion < 2)
    )
      throw new InvalidEventError();
    return new WalletBalanceChanged(w, t, m, ctx);
  }
}
