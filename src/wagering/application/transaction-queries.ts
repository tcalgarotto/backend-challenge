import type { MoneyProps } from '../../shared/domain/money.js';
import { isExactUuid, isOpaqueIdentifier } from '../../shared/domain/identifiers.js';
import type { FailureCode } from '../domain/wager-error.js';
import type { WagerTransactionKind, WagerTransactionStatus } from '../domain/wager-transaction.js';

export class TransactionQueryError extends Error {
  readonly httpStatus: number;
  constructor(readonly code: 'INVALID_PAYLOAD' | 'TRANSACTION_NOT_FOUND') {
    super(code);
    this.name = 'TransactionQueryError';
    this.httpStatus = code === 'TRANSACTION_NOT_FOUND' ? 404 : 400;
  }
}
/** A consulta expõe estado persistido, sem chave, hash ou flag de replay exclusivos da submissão. */
export interface TransactionView {
  readonly transactionId: string;
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: WagerTransactionKind;
  readonly money: MoneyProps;
  readonly status: WagerTransactionStatus;
  readonly createdAt: string;
  readonly referenceExternalTransactionId?: string;
  readonly referenceTransactionId?: string;
  readonly failureCode?: FailureCode;
  readonly processedAt?: string;
  readonly resultBalance?: MoneyProps;
}
export interface TransactionQueryStore {
  findById(transactionId: string): Promise<TransactionView | undefined>;
  findByProviderIdentity(providerId: string, externalTransactionId: string): Promise<TransactionView | undefined>;
}

export class TransactionQueries {
  constructor(private readonly store: TransactionQueryStore) {}

  async byId(transactionId: unknown): Promise<TransactionView> {
    if (!isExactUuid(transactionId)) throw new TransactionQueryError('INVALID_PAYLOAD');
    return this.found(await this.store.findById(transactionId.toLowerCase()));
  }

  /** Identidades de provedor preservam a caixa exatamente como recebidas. */
  async byProviderIdentity(providerId: unknown, externalTransactionId: unknown): Promise<TransactionView> {
    if (!isOpaqueIdentifier(providerId) || !isOpaqueIdentifier(externalTransactionId)) throw new TransactionQueryError('INVALID_PAYLOAD');
    return this.found(await this.store.findByProviderIdentity(providerId, externalTransactionId));
  }

  private found(view: TransactionView | undefined): TransactionView {
    if (!view) throw new TransactionQueryError('TRANSACTION_NOT_FOUND');
    return view;
  }
}
