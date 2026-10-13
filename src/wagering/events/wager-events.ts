import {
  IntegrationEvent,
  InvalidEventError,
  type EventContext,
} from '../../shared/events/integration-event.js';
import type { MoneyProps } from '../../shared/domain/money.js';
import type { Wallet } from '../../wallet/domain/wallet.js';
import {
  WagerTransaction,
  WagerTransactionStatus,
  type WagerTransactionKind,
} from '../domain/wager-transaction.js';
import type { WagerFailureDiagnostic, RejectionCode } from '../domain/wager-error.js';

export interface WagerIdentityData {
  readonly transactionId: string;
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: WagerTransactionKind;
  readonly money: MoneyProps;
}
export interface ProcessedData extends WagerIdentityData {
  readonly balance: MoneyProps;
  readonly walletVersion: number;
  readonly processedAt: string;
}
export interface RejectedData extends WagerIdentityData {
  readonly failureCode: RejectionCode;
  readonly balance: MoneyProps;
  readonly walletVersion: number;
  readonly rejectedAt: string;
}
export interface PendingReferenceData extends WagerIdentityData {
  readonly referenceExternalTransactionId: string;
  readonly expiresAt: string;
}
function identity(t: WagerTransaction): WagerIdentityData {
  return {
    transactionId: t.id,
    providerId: t.providerId,
    externalTransactionId: t.externalTransactionId,
    walletId: t.walletId,
    playerId: t.playerId,
    roundId: t.roundId,
    gameId: t.gameId,
    kind: t.kind,
    money: t.money.toJSON(),
  };
}
function assertWallet(t: WagerTransaction, w: Wallet, ctx: EventContext) {
  if (
    w.id !== t.walletId ||
    w.playerId !== t.playerId ||
    w.currency !== t.money.currency ||
    w.balance.isNegative() ||
    !Number.isSafeInteger(w.version) ||
    w.version < 1 ||
    !(ctx.occurredAt instanceof Date) ||
    ctx.occurredAt < t.createdAt
  )
    throw new InvalidEventError();
}
export class WagerTransactionProcessed extends IntegrationEvent<ProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;
  private constructor(t: WagerTransaction, w: Wallet, ctx: EventContext) {
    super({
      ...ctx,
      aggregateId: t.id,
      data: {
        ...identity(t),
        balance: w.balance.toJSON(),
        walletVersion: w.version,
        processedAt: t.processedAt!.toISOString(),
      },
    });
    Object.freeze(this);
  }
  static from(t: WagerTransaction, w: Wallet, ctx: EventContext): WagerTransactionProcessed {
    assertWallet(t, w, ctx);
    if (
      t.status !== WagerTransactionStatus.Processed ||
      ctx.occurredAt.getTime() !== t.processedAt?.getTime()
    )
      throw new InvalidEventError();
    return new WagerTransactionProcessed(t, w, ctx);
  }
}
export class WagerTransactionRejected extends IntegrationEvent<RejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;
  private constructor(t: WagerTransaction, w: Wallet, ctx: EventContext) {
    super({
      ...ctx,
      aggregateId: t.id,
      data: {
        ...identity(t),
        failureCode: t.failureCode as RejectionCode,
        balance: w.balance.toJSON(),
        walletVersion: w.version,
        rejectedAt: ctx.occurredAt.toISOString(),
      },
    });
    Object.freeze(this);
  }
  static from(t: WagerTransaction, w: Wallet, ctx: EventContext): WagerTransactionRejected {
    assertWallet(t, w, ctx);
    if (t.status !== WagerTransactionStatus.Rejected) throw new InvalidEventError();
    return new WagerTransactionRejected(t, w, ctx);
  }
}
export class WagerTransactionPendingReference extends IntegrationEvent<PendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;
  private constructor(t: WagerTransaction, ctx: EventContext) {
    super({
      ...ctx,
      aggregateId: t.id,
      data: {
        ...identity(t),
        referenceExternalTransactionId: t.referenceExternalTransactionId!,
        expiresAt: t.referenceExpiresAt!.toISOString(),
      },
    });
    Object.freeze(this);
  }
  static from(t: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    if (
      t.status !== WagerTransactionStatus.PendingReference ||
      ctx.occurredAt.getTime() !== t.pendingReferenceAt?.getTime()
    )
      throw new InvalidEventError();
    return new WagerTransactionPendingReference(t, ctx);
  }
}

export interface FailedData extends WagerIdentityData {
  readonly failureCode: 'PERMANENT_INFRASTRUCTURE_FAILURE';
  readonly balance: MoneyProps;
  readonly walletVersion: number;
  readonly failedAt: string;
  readonly diagnosticCode: 'SQL_FEATURE_NOT_SUPPORTED';
  readonly sqlState: '0A000';
  readonly attempts: number;
}
export class WagerTransactionFailed extends IntegrationEvent<FailedData> {
  readonly eventType = 'WagerTransactionFailed';
  readonly version = 1;
  private constructor(t: WagerTransaction, w: Wallet, ctx: EventContext, diagnostic: WagerFailureDiagnostic) {
    super({...ctx,aggregateId:t.id,data:{...identity(t),failureCode:'PERMANENT_INFRASTRUCTURE_FAILURE',
      balance:w.balance.toJSON(),walletVersion:w.version,failedAt:ctx.occurredAt.toISOString(),...diagnostic}});
    Object.freeze(this);
  }
  static from(t: WagerTransaction, w: Wallet, ctx: EventContext, diagnostic: WagerFailureDiagnostic): WagerTransactionFailed {
    assertWallet(t,w,ctx);
    if(t.status!==WagerTransactionStatus.Failed || t.failureCode!=='PERMANENT_INFRASTRUCTURE_FAILURE'
      || diagnostic.diagnosticCode!=='SQL_FEATURE_NOT_SUPPORTED' || diagnostic.sqlState!=='0A000'
      || !Number.isSafeInteger(diagnostic.attempts) || diagnostic.attempts<1 || diagnostic.attempts>1000) throw new InvalidEventError();
    return new WagerTransactionFailed(t,w,ctx,diagnostic);
  }
}
