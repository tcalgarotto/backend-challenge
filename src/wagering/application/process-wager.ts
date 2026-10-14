import { reversalPolicy, type ReversalPolicy } from '../domain/reversal-policy.js';
import { LedgerDirection } from '../../wallet/domain/wallet-ledger-entry.js';
import { createHash, randomUUID } from 'node:crypto';
import { Money, type MoneyProps } from '../../shared/domain/money.js';
import { isEnabledCurrency } from '../../shared/domain/currency.js';
import { isExactUuid, isOpaqueIdentifier } from '../../shared/domain/identifiers.js';
import { WagerTransaction, WagerTransactionKind, type CreateWagerTransactionProps, WagerTransactionStatus } from '../domain/wager-transaction.js';
import { DomainError } from '../../shared/domain/domain-error.js';
import type { Wallet, WalletMovement } from '../../wallet/domain/wallet.js';
import { WalletBalanceChanged } from '../../wallet/events/wallet-balance-changed.js';
import type { IntegrationEvent } from '../../shared/events/integration-event.js';
import { WagerTransactionProcessed, WagerTransactionRejected, WagerTransactionPendingReference, WagerTransactionFailed } from '../events/wager-events.js';
import type { FailureCode, WagerFailureDiagnostic } from '../domain/wager-error.js';

export type ProcessErrorCode = 'INVALID_PAYLOAD' | 'UNSUPPORTED_CURRENCY' | 'IDEMPOTENCY_KEY_REQUIRED' | 'REFERENCE_REQUIRED'
  | 'WALLET_NOT_FOUND' | 'PLAYER_MISMATCH' | 'CURRENCY_MISMATCH'
  | 'IDEMPOTENCY_CONFLICT' | 'EXTERNAL_IDENTITY_CONFLICT' | 'INFRASTRUCTURE_UNAVAILABLE';
export class ProcessWagerError extends Error {
  constructor(readonly code: ProcessErrorCode, readonly httpStatus: number, options?: ErrorOptions) {
    super(code, options); this.name = 'ProcessWagerError';
  }
}
export interface WagerResponse {
  readonly transactionId: string;
  readonly status: WagerTransactionStatus;
  readonly balance?: MoneyProps;
  readonly failureCode?: FailureCode;
  readonly idempotentReplay: boolean;
  readonly error?: {readonly code: 'PERMANENT_INFRASTRUCTURE_FAILURE';readonly message: string;readonly retryable: false};
}
export interface ProcessWagerResult { readonly httpStatus: number; readonly body: WagerResponse }
export interface ProcessWagerContext { readonly correlationId: string }
export interface ProcessWagerStore {
  process(command: CreateWagerTransactionProps, context: ProcessWagerContext): Promise<ProcessWagerResult>;
}
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(canonicalJson).join(',') + ']';
  if (value !== null && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return '{' + Object.keys(object).sort().map(key => JSON.stringify(key) + ':' + canonicalJson(object[key])).join(',') + '}';
  }
  return JSON.stringify(value);
}
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);
const exactUuid = isExactUuid, text = isOpaqueIdentifier;
const required = ['providerId','externalTransactionId','playerId','walletId','roundId','gameId','kind','money'];
/** HTTP recebe a chave no header e SQS no comando; nenhum canal pode inferir identidade de negócio como fallback. */
export function parseWager(input: unknown, idempotencyKey: unknown): CreateWagerTransactionProps {
  if (!text(idempotencyKey)) throw new ProcessWagerError('IDEMPOTENCY_KEY_REQUIRED', 400);
  if (!record(input) || required.some(key => !Object.hasOwn(input, key)) ||
      Object.keys(input).some(key => ![...required,'referenceExternalTransactionId'].includes(key)) ||
      !['providerId','externalTransactionId','roundId','gameId'].every(key => text(input[key])) ||
      !exactUuid(input['playerId']) || !exactUuid(input['walletId']) ||
      !record(input['money']) || Object.keys(input['money']).length !== 2 ||
      typeof input['money']['amount'] !== 'string' || typeof input['money']['currency'] !== 'string' ||
      (Object.hasOwn(input,'referenceExternalTransactionId') && !text(input['referenceExternalTransactionId']))) {
    throw new ProcessWagerError('INVALID_PAYLOAD', 400);
  }
  // OPENING e outras identidades do sistema usam namespace de provedor reservado.
  if ((input['providerId'] as string).startsWith('internal:'))
    throw new ProcessWagerError('INVALID_PAYLOAD',400);
  const kind = input['kind'] as WagerTransactionKind;
  if (!Object.values(WagerTransactionKind).includes(kind) || kind === WagerTransactionKind.Opening)
    throw new ProcessWagerError('INVALID_PAYLOAD',400);
  if ([WagerTransactionKind.Refund,WagerTransactionKind.Rollback].includes(kind) && !Object.hasOwn(input,'referenceExternalTransactionId'))
    throw new ProcessWagerError('REFERENCE_REQUIRED',400);
  // Moeda habilitada mas diferente da wallet só é detectada sob lock, como CURRENCY_MISMATCH sem gravação.
  const currency = input['money']['currency'];
  if (!isEnabledCurrency(currency)) throw new ProcessWagerError('UNSUPPORTED_CURRENCY',400);
  let money: Money;
  try { money = Money.from({amount:input['money']['amount'],currency}); }
  catch { throw new ProcessWagerError('INVALID_PAYLOAD',400); }
  // Normalizar UUIDs antes do hash permite replay quando só muda a caixa;
  // IDs de provedor, transação externa, round e game continuam opacos e sensíveis à caixa.
  const business = {
    providerId: input['providerId'] as string, externalTransactionId: input['externalTransactionId'] as string,
    playerId: input['playerId'].toLowerCase(), walletId: input['walletId'].toLowerCase(),
    roundId: input['roundId'] as string, gameId: input['gameId'] as string, kind, money: money.toJSON(),
    ...(Object.hasOwn(input,'referenceExternalTransactionId') ? {referenceExternalTransactionId:input['referenceExternalTransactionId'] as string} : {}),
  };
  const command = Object.freeze({...business, money, idempotencyKey, id:randomUUID(),createdAt:new Date(),
    payloadHash:createHash('sha256').update(canonicalJson(business),'utf8').digest('hex')});
  try { WagerTransaction.create(command); }
  catch { throw new ProcessWagerError('INVALID_PAYLOAD',400); }
  return command;
}
/** Todos os canais aplicam regras no agregado sob lock; identidade SQL e lease permanecem nos adaptadores transacionais. */
export class WagerApplicationService {
  constructor(private readonly store: ProcessWagerStore) {}
  static applyToWallet(transaction: WagerTransaction, wallet: Wallet, reference: WagerTransaction | undefined,
    appliedReversals: readonly WagerTransactionKind[], at: Date, expiresAt = transaction.referenceExpiresAt,
    policy: ReversalPolicy = reversalPolicy()): WalletMovement | undefined {
    const decision = transaction.referenceDecision(reference, appliedReversals, policy);
    if (decision.status === 'REJECT') {
      transaction.reject(decision.code);
      return undefined;
    }
    if (decision.status === 'WAIT') {
      if (transaction.status === WagerTransactionStatus.PendingReference) {
        if (!expiresAt) throw new Error('Pending reference has no durable deadline.');
        if (at >= expiresAt) transaction.reject('REFERENCE_EXPIRED');
      } else {
        transaction.markPendingReference(at, new Date(transaction.createdAt.getTime() + 15 * 60 * 1000));
      }
      return undefined;
    }
    try {
      const direction = transaction.ledgerDirectionFor(reference);
      const movement = direction === LedgerDirection.Debit ? wallet.debit(transaction.money, at)
        : direction === LedgerDirection.Credit ? wallet.credit(transaction.money, at) : undefined;
      transaction.markProcessed(reference, at);
      return movement;
    } catch (error) {
      if (!(error instanceof DomainError)) throw error;
      if (error.code === 'INSUFFICIENT_FUNDS') {
        transaction.reject(transaction.kind === WagerTransactionKind.Rollback
          ? 'REVERSAL_INSUFFICIENT_FUNDS' : 'INSUFFICIENT_FUNDS');
      } else if (error.code === 'MONEY_OUT_OF_RANGE') transaction.reject('MONETARY_RANGE_EXCEEDED');
      else throw error;
      return undefined;
    }
  }

  static recordFailure(transaction: WagerTransaction): void {
    transaction.fail('PERMANENT_INFRASTRUCTURE_FAILURE');
  }

  static snapshot(transaction: WagerTransaction, wallet: Wallet): ProcessWagerResult & {
    readonly pendingReferenceAt?: string; readonly referenceExpiresAt?: string;
  } {
    const pending = transaction.status === WagerTransactionStatus.PendingReference;
    return {
      httpStatus: pending ? 202 : transaction.status === WagerTransactionStatus.Rejected ? 422 : transaction.status === WagerTransactionStatus.Failed ? 500 : 200,
      body: { transactionId: transaction.id, status: transaction.status, idempotentReplay: false,
        ...(pending ? {} : { balance: wallet.balance.toJSON() }),
        ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
        ...(transaction.status===WagerTransactionStatus.Failed?{error:{code:'PERMANENT_INFRASTRUCTURE_FAILURE' as const,message:'PERMANENT_INFRASTRUCTURE_FAILURE',retryable:false as const}}:{}) },
      ...(transaction.pendingReferenceAt ? {pendingReferenceAt: transaction.pendingReferenceAt.toISOString()} : {}),
      ...(transaction.referenceExpiresAt ? {referenceExpiresAt: transaction.referenceExpiresAt.toISOString()} : {}),
    };
  }

  static events(transaction: WagerTransaction, wallet: Wallet, movement: WalletMovement | undefined,
    context: {correlationId: string; occurredAt: Date}, reference?: WagerTransaction, diagnostic?: WagerFailureDiagnostic): IntegrationEvent<object>[] {
    const ctx = {...context, causationId: transaction.id};
    if(transaction.status===WagerTransactionStatus.Failed && !diagnostic) throw new Error('FAILED_DIAGNOSTIC_REQUIRED');
    const events: IntegrationEvent<object>[] = transaction.status===WagerTransactionStatus.Failed
      ? [WagerTransactionFailed.from(transaction,wallet,{...ctx,eventId:randomUUID()},diagnostic!)]
      : transaction.status === WagerTransactionStatus.PendingReference
      ? [WagerTransactionPendingReference.from(transaction, {...ctx, eventId: randomUUID()})]
      : transaction.status === WagerTransactionStatus.Rejected
        ? [WagerTransactionRejected.from(transaction, wallet, {...ctx, eventId: randomUUID()})]
        : [WagerTransactionProcessed.from(transaction, wallet, {...ctx, eventId: randomUUID()})];
    if (movement) events.push(WalletBalanceChanged.from(wallet, transaction, movement,
      {...ctx, eventId: randomUUID()}, reference));
    return events;
  }

  async execute(input: unknown, idempotencyKey: unknown, correlationId: string = randomUUID()): Promise<ProcessWagerResult> {
    if (!text(correlationId)) throw new ProcessWagerError('INVALID_PAYLOAD',400);
    return this.store.process(parseWager(input,idempotencyKey),{correlationId});
  }
}


export { WagerApplicationService as ProcessWager };
