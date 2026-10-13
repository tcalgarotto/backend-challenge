export const rejectionCodes = [
  'INSUFFICIENT_FUNDS',
  'REVERSAL_INSUFFICIENT_FUNDS',
  'REFERENCE_CONTEXT_MISMATCH',
  'REFERENCE_KIND_INVALID',
  'REFERENCE_NOT_PROCESSED',
  'REVERSAL_AMOUNT_MISMATCH',
  'ALREADY_REVERSED',
  'SELF_REFERENCE',
  'REFERENCE_EXPIRED',
  'MONETARY_RANGE_EXCEEDED',
] as const;
export type RejectionCode = (typeof rejectionCodes)[number];
export type FailureCode = RejectionCode | 'PERMANENT_INFRASTRUCTURE_FAILURE';
export type WagerErrorCode =
  | FailureCode
  | 'INVALID_WAGER'
  | 'REFERENCE_REQUIRED'
  | 'INVALID_TRANSACTION_STATE'
  | 'INVALID_REFERENCE_STATE';
export class WagerError extends Error {
  constructor(public readonly code: WagerErrorCode) {
    super(code);
    this.name = 'WagerError';
  }
}

export class InvalidTransactionStateError extends WagerError {
  constructor() { super('INVALID_TRANSACTION_STATE'); this.name = 'InvalidTransactionStateError'; }
}

/** Diagnóstico limitado a falha permanente de banco identificada com segurança. */
export interface WagerFailureDiagnostic {
  readonly diagnosticCode: 'SQL_FEATURE_NOT_SUPPORTED';
  readonly sqlState: '0A000';
  readonly attempts: number;
}
