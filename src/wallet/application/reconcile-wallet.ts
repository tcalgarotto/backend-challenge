import { randomUUID } from 'node:crypto';
import { Money, type MoneyProps } from '../../shared/domain/money.js';
import { formatMinorUnits, parseMinorUnits } from '../../shared/domain/exact-decimal.js';
import { WalletReadError, walletIdentity } from './wallet-queries.js';

/** Lock da wallet mantém saldo e ledger no mesmo ponto consistente de leitura. */
export interface ReconciliationSnapshot {
  readonly walletId: string;
  readonly currency: string;
  readonly storedBalance: string;
  /** Soma assinada exata pode exceder a faixa de um único saldo. */
  readonly ledgerTotal: string;
  readonly checkedEntries: number;
}
export interface ReconciliationStore {

  snapshot(walletId: string): Promise<ReconciliationSnapshot | undefined>;
}
export interface ReconciliationResult {
  readonly walletId: string;
  readonly storedBalance: MoneyProps;
  readonly calculatedBalance: MoneyProps;
  readonly difference: MoneyProps;
  readonly consistent: boolean;
  readonly checkedEntries: number;
}
export interface ReconciliationMonitor {
  /** Divergência deve chegar a logs e métricas sem alterar o resultado. */
  observe(result: ReconciliationResult, context: { readonly correlationId: string }): void;
}
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

/** Reconciliação apenas verifica; não corrige dados silenciosamente. */
export class ReconcileWallet {
  constructor(private readonly store: ReconciliationStore, private readonly monitor: ReconciliationMonitor) {}

  async execute(walletId: unknown, body: unknown, correlationId: string = randomUUID()): Promise<ReconciliationResult> {
    const id = walletIdentity(walletId);
    if (body !== undefined && (!record(body) || Object.keys(body).length > 0)) throw new WalletReadError('INVALID_PAYLOAD');
    const snapshot = await this.store.snapshot(id);
    if (!snapshot) throw new WalletReadError('WALLET_NOT_FOUND');
    const stored = Money.rehydrate({ amount: snapshot.storedBalance, currency: snapshot.currency });
    const storedUnits = parseMinorUnits(stored.toString()), calculatedUnits = parseMinorUnits(snapshot.ledgerTotal);
    const result: ReconciliationResult = {
      walletId: snapshot.walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: { amount: formatMinorUnits(calculatedUnits), currency: stored.currency },
      difference: { amount: formatMinorUnits(storedUnits - calculatedUnits), currency: stored.currency },
      consistent: storedUnits === calculatedUnits,
      checkedEntries: snapshot.checkedEntries,
    };
    this.monitor.observe(result, { correlationId });
    return result;
  }
}
