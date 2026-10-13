export type DomainErrorCode =
  | 'INVALID_MONEY'
  | 'UNSUPPORTED_CURRENCY'
  | 'CURRENCY_MISMATCH'
  | 'MONEY_OUT_OF_RANGE'
  | 'INVALID_WALLET_STATE'
  | 'INVALID_MOVEMENT_AMOUNT'
  | 'INSUFFICIENT_FUNDS';

export class DomainError extends Error {
  constructor(public readonly code: DomainErrorCode) {
    super(code);
    this.name = 'DomainError';
  }
}
