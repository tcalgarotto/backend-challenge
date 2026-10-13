import type { ReconciliationMonitor, ReconciliationResult } from '../application/reconcile-wallet.js';

export type ReconciliationOutcome = 'consistent' | 'divergent';
export interface ReconciliationMetric {
  readonly name: 'jwp_wallet_reconciliations_total';
  readonly labels: { readonly outcome: ReconciliationOutcome; readonly currency: string };
  readonly value: number;
}

/** IDs ficam nos logs e fora dos labels; valores monetários não são registrados. */
export class ReconciliationSignals implements ReconciliationMonitor {
  readonly #counts = new Map<string, number>();
  constructor(private readonly write: (line: string) => void = line => { process.stdout.write(line); }) {}

  observe(result: ReconciliationResult, context: { readonly correlationId: string }): void {
    const outcome: ReconciliationOutcome = result.consistent ? 'consistent' : 'divergent';
    const key = `${outcome}\u0000${result.storedBalance.currency}`;
    this.#counts.set(key, (this.#counts.get(key) ?? 0) + 1);
    if (result.consistent) return;
    this.write(`${JSON.stringify({ level: 'error', type: 'wallet.reconciliation.divergent', at: new Date().toISOString(),
      correlationId: context.correlationId, walletId: result.walletId, currency: result.storedBalance.currency,
      checkedEntries: result.checkedEntries, repaired: false })}\n`);
  }

  metrics(): ReconciliationMetric[] {
    return [...this.#counts].map(([key, value]) => {
      const [outcome, currency] = key.split('\u0000') as [ReconciliationOutcome, string];
      return { name: 'jwp_wallet_reconciliations_total', labels: { outcome, currency }, value };
    });
  }
}
