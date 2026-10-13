import type { WagerFailureDiagnostic } from '../../wagering/domain/wager-error.js';
/** Somente SQLSTATE permitido produz falha permanente; transientes, constraints e erros desconhecidos não viram estado terminal ao esgotar retries. */
export function isKnownPermanentWagerFailure(error: unknown): boolean {
  return error !== null && typeof error === 'object' && 'code' in error && error.code === '0A000';
}
export function permanentWagerDiagnostic(attempts: number): WagerFailureDiagnostic {
  if (!Number.isSafeInteger(attempts) || attempts < 1 || attempts > 1000) throw new Error('INVALID_FAILURE_ATTEMPTS');
  return Object.freeze({diagnosticCode:'SQL_FEATURE_NOT_SUPPORTED',sqlState:'0A000',attempts});
}
