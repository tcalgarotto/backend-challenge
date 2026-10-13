import { rejectionCodes } from '../../wagering/domain/wager-error.js';

// Valida o contrato do evento antes de inserir na inbox ou dar ack;
// dinheiro permanece string decimal estrita.
const record = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === 'object' && !Array.isArray(v);
const uuid = (v: unknown) => typeof v === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.exec(v)?.[0] === v;
const text = (v: unknown) => typeof v === 'string' && v.length >= 1 && v.length <= 128;
const instant = (v: unknown) => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/.exec(v)?.[0] === v && Number.isFinite(Date.parse(v));
const version = (v: unknown) => typeof v === 'number' && Number.isSafeInteger(v) && v >= 1;
const money = (v: unknown) => record(v) && Object.keys(v).length === 2 && typeof v['amount'] === 'string'
  && /^(0|[1-9][0-9]{0,17})\.[0-9]{2}$/.exec(v['amount'])?.[0] === v['amount']
  && typeof v['currency'] === 'string' && /^[A-Z]{3}$/.exec(v['currency'])?.[0] === v['currency'];
const exact = (v: Record<string, unknown>, required: string[], optional: string[] = []) =>
  required.every(k => Object.hasOwn(v, k)) && Object.keys(v).every(k => required.includes(k) || optional.includes(k));
const identity = ['transactionId', 'walletId', 'playerId', 'kind', 'money'];
const provider = ['providerId', 'externalTransactionId', 'roundId', 'gameId'];
const nonOpening = ['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'];

export function validateIntegrationEventData(type: string, data: Record<string, unknown>): boolean {
  if (type === 'WalletBalanceChanged') {
    return exact(data, ['walletId', 'transactionId', 'direction', 'money', 'balanceBefore', 'balanceAfter', 'walletVersion'])
      && uuid(data['walletId']) && uuid(data['transactionId']) && ['CREDIT', 'DEBIT'].includes(String(data['direction']))
      && money(data['money']) && money(data['balanceBefore']) && money(data['balanceAfter']) && version(data['walletVersion']);
  }
  if (!uuid(data['transactionId']) || !uuid(data['walletId']) || !uuid(data['playerId']) || !money(data['money'])) return false;
  if (type === 'WagerTransactionProcessed') {
    const opening = data['kind'] === 'OPENING';
    return exact(data, [...identity, ...(opening ? [] : provider), 'balance', 'walletVersion', 'processedAt'],
      [...(opening ? provider : []), 'referenceTransactionId'])
      && (opening || nonOpening.includes(String(data['kind'])))
      && provider.every(k => data[k] === undefined ? opening : text(data[k]))
      && (data['referenceTransactionId'] === undefined || uuid(data['referenceTransactionId']))
      && money(data['balance']) && version(data['walletVersion']) && instant(data['processedAt']);
  }
  if (type === 'WagerTransactionFailed') {
    return exact(data,[...identity,...provider,'failureCode','balance','walletVersion','failedAt','diagnosticCode','sqlState','attempts'])
      && nonOpening.includes(String(data['kind'])) && provider.every(k=>text(data[k]))
      && data['failureCode']==='PERMANENT_INFRASTRUCTURE_FAILURE' && data['diagnosticCode']==='SQL_FEATURE_NOT_SUPPORTED'
      && data['sqlState']==='0A000' && version(data['attempts']) && (data['attempts'] as number)<=1000
      && money(data['balance']) && version(data['walletVersion']) && instant(data['failedAt']);
  }
  if (type === 'WagerTransactionRejected') {
    return exact(data, [...identity, ...provider, 'failureCode', 'balance', 'walletVersion', 'rejectedAt'], ['referenceExternalTransactionId'])
      && nonOpening.includes(String(data['kind'])) && provider.every(k => text(data[k]))
      && rejectionCodes.includes(data['failureCode'] as typeof rejectionCodes[number])
      && (data['referenceExternalTransactionId'] === undefined || text(data['referenceExternalTransactionId']))
      && money(data['balance']) && version(data['walletVersion']) && instant(data['rejectedAt']);
  }
  if (type === 'WagerTransactionPendingReference') {
    return exact(data, [...identity, ...provider, 'referenceExternalTransactionId', 'expiresAt'])
      && ['WIN', 'REFUND', 'ROLLBACK'].includes(String(data['kind'])) && provider.every(k => text(data[k]))
      && text(data['referenceExternalTransactionId']) && instant(data['expiresAt']);
  }
  return false;
}
