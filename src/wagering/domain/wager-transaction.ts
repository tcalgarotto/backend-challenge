import { LedgerDirection } from '../../wallet/domain/wallet-ledger-entry.js';
import { Money } from '../../shared/domain/money.js';
import { rejectionCodes, WagerError, InvalidTransactionStateError, type RejectionCode, type FailureCode } from './wager-error.js';

export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}
export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}
export interface CreateWagerTransactionProps {
  readonly id: string;
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly idempotencyKey: string;
  readonly payloadHash: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: WagerTransactionKind;
  readonly money: Money;
  readonly referenceExternalTransactionId?: string;
  readonly createdAt: Date;
}
export interface WagerTransactionState extends CreateWagerTransactionProps {
  readonly status: WagerTransactionStatus;
  readonly referenceTransactionId?: string;
  readonly failureCode?: FailureCode;
  readonly processedAt?: Date;
  readonly pendingReferenceAt?: Date;
  readonly referenceExpiresAt?: Date;
}
export type ReferenceDecision =
  | { readonly status: 'READY' }
  | { readonly status: 'WAIT' }
  | { readonly status: 'REJECT'; readonly code: RejectionCode };
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const text = (value: unknown): value is string =>
  typeof value === 'string' && value.trim().length > 0 && value.length <= 128;
const validDate = (value: unknown): value is Date =>
  value instanceof Date && Number.isFinite(value.getTime());
const terminal = (s: WagerTransactionStatus) =>
  [WagerTransactionStatus.Processed, WagerTransactionStatus.Rejected, WagerTransactionStatus.Failed].includes(
    s,
  );

/** O domínio não armazena transações; SQL coordena identidade durável, lock, ledger e outbox. */
export class WagerTransaction {
  readonly id: string;
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly idempotencyKey: string;
  readonly payloadHash: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: WagerTransactionKind;
  readonly money: Money;
  readonly referenceExternalTransactionId: string | undefined;
  #createdAt: number;
  #status: WagerTransactionStatus;
  #referenceTransactionId: string | undefined;
  #failureCode: FailureCode | undefined;
  #processedAt: number | undefined;
  #pendingReferenceAt: number | undefined;
  #referenceExpiresAt: number | undefined;

  private constructor(p: CreateWagerTransactionProps) {
    this.id = p.id;
    this.providerId = p.providerId;
    this.externalTransactionId = p.externalTransactionId;
    this.idempotencyKey = p.idempotencyKey;
    this.payloadHash = p.payloadHash;
    this.walletId = p.walletId;
    this.playerId = p.playerId;
    this.roundId = p.roundId;
    this.gameId = p.gameId;
    this.kind = p.kind;
    this.money = p.money;
    this.referenceExternalTransactionId = p.referenceExternalTransactionId;
    this.#createdAt = p.createdAt.getTime();
    this.#status = WagerTransactionStatus.Pending;
    Object.freeze(this);
  }
  static create(p: CreateWagerTransactionProps): WagerTransaction {
    WagerTransaction.validate(p, false);
    return new WagerTransaction(p);
  }
  /** A fábrica interna é exclusiva da abertura de wallet. */
  static createOpening(
    p: Omit<CreateWagerTransactionProps, 'kind' | 'referenceExternalTransactionId'>,
  ): WagerTransaction {
    const opening = { ...p, kind: WagerTransactionKind.Opening };
    WagerTransaction.validate(opening, true);
    return new WagerTransaction(opening);
  }
  static rehydrate(s: WagerTransactionState): WagerTransaction {
    WagerTransaction.assertPersistedIdentity(s);
    if (!Object.values(WagerTransactionStatus).includes(s.status))
      throw new WagerError('INVALID_TRANSACTION_STATE');
    if (s.kind === WagerTransactionKind.Opening && s.status !== WagerTransactionStatus.Processed)
      throw new WagerError('INVALID_TRANSACTION_STATE');
    if (
      s.referenceExternalTransactionId === s.externalTransactionId &&
      [WagerTransactionStatus.Processed, WagerTransactionStatus.PendingReference].includes(s.status)
    )
      throw new WagerError('INVALID_REFERENCE_STATE');
    const hasReference = s.referenceExternalTransactionId !== undefined;
    if (
      s.referenceTransactionId !== undefined &&
      (!uuid.test(s.referenceTransactionId) || !hasReference || s.referenceTransactionId === s.id)
    )
      throw new WagerError('INVALID_REFERENCE_STATE');
    if (s.status === WagerTransactionStatus.Processed) {
      if (
        !validDate(s.processedAt) ||
        s.processedAt < s.createdAt ||
        s.failureCode !== undefined ||
        (hasReference && s.referenceTransactionId === undefined)
      )
        throw new WagerError('INVALID_TRANSACTION_STATE');
    } else if (s.processedAt !== undefined || s.referenceTransactionId !== undefined)
      throw new WagerError('INVALID_TRANSACTION_STATE');
    if (
      s.status === WagerTransactionStatus.Rejected &&
      !rejectionCodes.includes(s.failureCode as RejectionCode)
    )
      throw new WagerError('INVALID_TRANSACTION_STATE');
    if (s.status === WagerTransactionStatus.Failed && s.failureCode !== 'PERMANENT_INFRASTRUCTURE_FAILURE')
      throw new WagerError('INVALID_TRANSACTION_STATE');
    if (
      ![WagerTransactionStatus.Rejected, WagerTransactionStatus.Failed].includes(s.status) &&
      s.failureCode !== undefined
    )
      throw new WagerError('INVALID_TRANSACTION_STATE');
    const hasPending = s.pendingReferenceAt !== undefined || s.referenceExpiresAt !== undefined;
    if (
      hasPending &&
      (!hasReference ||
        !validDate(s.pendingReferenceAt) ||
        !validDate(s.referenceExpiresAt) ||
        s.pendingReferenceAt < s.createdAt ||
        s.referenceExpiresAt <= s.pendingReferenceAt)
    )
      throw new WagerError('INVALID_REFERENCE_STATE');
    if (
      s.status === WagerTransactionStatus.Processed &&
      s.pendingReferenceAt &&
      s.processedAt! < s.pendingReferenceAt
    )
      throw new WagerError('INVALID_TRANSACTION_STATE');
    if (s.status === WagerTransactionStatus.PendingReference && !hasPending)
      throw new WagerError('INVALID_REFERENCE_STATE');
    if (s.status === WagerTransactionStatus.Pending && hasPending)
      throw new WagerError('INVALID_REFERENCE_STATE');
    const t = new WagerTransaction(s);
    t.#status = s.status;
    t.#referenceTransactionId = s.referenceTransactionId;
    t.#failureCode = s.failureCode;
    t.#processedAt = s.processedAt?.getTime();
    t.#pendingReferenceAt = s.pendingReferenceAt?.getTime();
    t.#referenceExpiresAt = s.referenceExpiresAt?.getTime();
    return t;
  }
  private static assertPersistedIdentity(s: WagerTransactionState): void {
    // Mesmas representações válidas do schema, sem executar decisões de referência ou transições.
    if (!uuid.test(s.id) || !uuid.test(s.walletId) || !uuid.test(s.playerId)
      || ![s.providerId,s.externalTransactionId,s.idempotencyKey,s.roundId,s.gameId].every(text)
      || typeof s.payloadHash !== 'string' || !/^[0-9a-f]{64}$/.test(s.payloadHash) || !validDate(s.createdAt)
      || !(s.money instanceof Money) || !Object.values(WagerTransactionKind).includes(s.kind)
      || s.money.isNegative() || (s.kind===WagerTransactionKind.Loss ? !s.money.isZero() : !s.money.isPositive()))
      throw new WagerError('INVALID_WAGER');
    const hasReference=s.referenceExternalTransactionId!==undefined;
    if (hasReference && !text(s.referenceExternalTransactionId)) throw new WagerError('INVALID_WAGER');
    if ([WagerTransactionKind.Refund,WagerTransactionKind.Rollback].includes(s.kind) && !hasReference)
      throw new WagerError('REFERENCE_REQUIRED');
    if (hasReference && ![WagerTransactionKind.Win,WagerTransactionKind.Refund,WagerTransactionKind.Rollback].includes(s.kind))
      throw new WagerError('INVALID_WAGER');
  }
  private static validate(p: CreateWagerTransactionProps, internal: boolean): void {
    if (
      !uuid.test(p.id) ||
      !uuid.test(p.walletId) ||
      !uuid.test(p.playerId) ||
      ![p.providerId, p.externalTransactionId, p.idempotencyKey, p.roundId, p.gameId].every(text) ||
      typeof p.payloadHash !== 'string' ||
      !/^[0-9a-f]{64}$/.test(p.payloadHash) ||
      !validDate(p.createdAt) ||
      !(p.money instanceof Money) ||
      !Object.values(WagerTransactionKind).includes(p.kind) ||
      (p.kind === WagerTransactionKind.Opening && !internal) ||
      p.money.isNegative() ||
      (p.kind === WagerTransactionKind.Loss ? !p.money.isZero() : !p.money.isPositive())
    )
      throw new WagerError('INVALID_WAGER');
    const hasReference = p.referenceExternalTransactionId !== undefined;
    if (hasReference && !text(p.referenceExternalTransactionId)) throw new WagerError('INVALID_WAGER');
    if ([WagerTransactionKind.Refund, WagerTransactionKind.Rollback].includes(p.kind) && !hasReference)
      throw new WagerError('REFERENCE_REQUIRED');
    if (
      hasReference &&
      ![WagerTransactionKind.Win, WagerTransactionKind.Refund, WagerTransactionKind.Rollback].includes(p.kind)
    )
      throw new WagerError('INVALID_WAGER');
  }
  get status(): WagerTransactionStatus {
    return this.#status;
  }
  get createdAt(): Date {
    return new Date(this.#createdAt);
  }
  get processedAt(): Date | undefined {
    return this.#processedAt === undefined ? undefined : new Date(this.#processedAt);
  }
  get pendingReferenceAt(): Date | undefined {
    return this.#pendingReferenceAt === undefined ? undefined : new Date(this.#pendingReferenceAt);
  }
  get referenceExpiresAt(): Date | undefined {
    return this.#referenceExpiresAt === undefined ? undefined : new Date(this.#referenceExpiresAt);
  }
  get referenceTransactionId(): string | undefined {
    return this.#referenceTransactionId;
  }
  get failureCode(): FailureCode | undefined {
    return this.#failureCode;
  }
  isTerminal(): boolean {
    return terminal(this.#status);
  }
  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }
  requiresReference(): boolean {
    return [WagerTransactionKind.Refund, WagerTransactionKind.Rollback].includes(this.kind);
  }
  matchesPayload(hash: string): boolean {
    return this.payloadHash === hash;
  }

  referenceDecision(reference?: WagerTransaction, alreadyReversed = false): ReferenceDecision {
    if (this.referenceExternalTransactionId === undefined) {
      if (reference) return { status: 'REJECT', code: 'REFERENCE_CONTEXT_MISMATCH' };
      return { status: 'READY' };
    }
    if (this.referenceExternalTransactionId === this.externalTransactionId || reference?.id === this.id)
      return { status: 'REJECT', code: 'SELF_REFERENCE' };
    if (!reference) return { status: 'WAIT' };
    if (this.#referenceTransactionId !== undefined && reference.id !== this.#referenceTransactionId)
      return { status: 'REJECT', code: 'REFERENCE_CONTEXT_MISMATCH' };
    if (
      reference.providerId !== this.providerId ||
      reference.externalTransactionId !== this.referenceExternalTransactionId ||
      reference.walletId !== this.walletId ||
      reference.playerId !== this.playerId ||
      reference.money.currency !== this.money.currency ||
      reference.roundId !== this.roundId
    )
      return { status: 'REJECT', code: 'REFERENCE_CONTEXT_MISMATCH' };
    const allowed =
      this.kind === WagerTransactionKind.Rollback
        ? [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund]
        : [WagerTransactionKind.Bet];
    if (!allowed.includes(reference.kind)) return { status: 'REJECT', code: 'REFERENCE_KIND_INVALID' };
    if (reference.status !== WagerTransactionStatus.Processed)
      return reference.isTerminal()
        ? { status: 'REJECT', code: 'REFERENCE_NOT_PROCESSED' }
        : { status: 'WAIT' };
    if (this.requiresReference() && !this.money.equals(reference.money))
      return { status: 'REJECT', code: 'REVERSAL_AMOUNT_MISMATCH' };
    if (this.requiresReference() && alreadyReversed) return { status: 'REJECT', code: 'ALREADY_REVERSED' };
    return { status: 'READY' };
  }
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection | undefined {
    const decision = this.referenceDecision(reference);
    if (decision.status !== 'READY')
      throw new WagerError(decision.status === 'REJECT' ? decision.code : 'INVALID_REFERENCE_STATE');
    if (this.kind === WagerTransactionKind.Loss) return undefined;
    if (this.kind === WagerTransactionKind.Bet) return LedgerDirection.Debit;
    if (
      this.kind === WagerTransactionKind.Rollback &&
      reference &&
      [WagerTransactionKind.Win, WagerTransactionKind.Refund].includes(reference.kind)
    )
      return LedgerDirection.Debit;
    return LedgerDirection.Credit;
  }
  markProcessed(reference: WagerTransaction | undefined, at: Date): void {
    this.assertMutable();
    this.assertTime(at);
    this.ledgerDirectionFor(reference);
    this.#referenceTransactionId = reference?.id;
    this.#processedAt = at.getTime();
    this.#status = WagerTransactionStatus.Processed;
  }
  /** Somente a primeira entrada em pendência gera evento; polling não deve duplicá-lo. */
  markPendingReference(at: Date, expiresAt: Date): boolean {
    this.assertMutable();
    this.assertTime(at);
    if (this.referenceDecision().status === 'REJECT') throw new WagerError('SELF_REFERENCE');
    if (this.referenceExternalTransactionId === undefined || !validDate(expiresAt) || expiresAt <= at)
      throw new WagerError('INVALID_REFERENCE_STATE');
    if (this.#status === WagerTransactionStatus.PendingReference) return false;
    this.#pendingReferenceAt = at.getTime();
    this.#referenceExpiresAt = expiresAt.getTime();
    this.#status = WagerTransactionStatus.PendingReference;
    return true;
  }
  reject(code: RejectionCode): void {
    this.assertMutable();
    if (this.kind === WagerTransactionKind.Opening) throw new InvalidTransactionStateError();
    if (!rejectionCodes.includes(code)) throw new WagerError('INVALID_WAGER');
    this.#failureCode = code;
    this.#status = WagerTransactionStatus.Rejected;
  }
  fail(code: 'PERMANENT_INFRASTRUCTURE_FAILURE'): void {
    this.assertMutable();
    if (this.kind === WagerTransactionKind.Opening) throw new InvalidTransactionStateError();
    if (code !== 'PERMANENT_INFRASTRUCTURE_FAILURE') throw new WagerError('INVALID_WAGER');
    this.#failureCode = code;
    this.#status = WagerTransactionStatus.Failed;
  }
  private assertMutable(): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError();
  }
  private assertTime(at: Date): void {
    if (
      !validDate(at) ||
      at.getTime() < this.#createdAt ||
      (this.#pendingReferenceAt !== undefined && at.getTime() < this.#pendingReferenceAt)
    )
      throw new WagerError('INVALID_WAGER');
  }
  toState(): WagerTransactionState {
    return Object.freeze({
      id: this.id,
      providerId: this.providerId,
      externalTransactionId: this.externalTransactionId,
      idempotencyKey: this.idempotencyKey,
      payloadHash: this.payloadHash,
      walletId: this.walletId,
      playerId: this.playerId,
      roundId: this.roundId,
      gameId: this.gameId,
      kind: this.kind,
      money: this.money,
      createdAt: this.createdAt,
      status: this.#status,
      ...(this.referenceExternalTransactionId === undefined
        ? {}
        : {
            referenceExternalTransactionId: this.referenceExternalTransactionId,
          }),
      ...(this.#referenceTransactionId === undefined
        ? {}
        : { referenceTransactionId: this.#referenceTransactionId }),
      ...(this.#failureCode === undefined ? {} : { failureCode: this.#failureCode }),
      ...(this.processedAt === undefined ? {} : { processedAt: this.processedAt }),
      ...(this.pendingReferenceAt === undefined ? {} : { pendingReferenceAt: this.pendingReferenceAt }),
      ...(this.referenceExpiresAt === undefined ? {} : { referenceExpiresAt: this.referenceExpiresAt }),
    });
  }
}
