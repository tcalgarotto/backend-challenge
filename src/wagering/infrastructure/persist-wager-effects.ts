import { randomUUID } from 'node:crypto';
import type { EntityManager } from '@mikro-orm/postgresql';
import { sqlQuery } from '../../shared/infrastructure/sql-transaction.js';
import { WalletLedgerEntry } from '../../wallet/domain/wallet-ledger-entry.js';
import type { Wallet, WalletMovement } from '../../wallet/domain/wallet.js';
import { OutboxMessage } from '../../messaging/domain/outbox-message.js';
import { ProcessWager } from '../application/process-wager.js';
import type { WagerTransaction } from '../domain/wager-transaction.js';
import type { WagerFailureDiagnostic } from '../domain/wager-error.js';

export interface WagerEffects {
  readonly transaction: WagerTransaction;
  readonly wallet: Wallet;
  readonly movement: WalletMovement | undefined;
  readonly reference: WagerTransaction | undefined;
  readonly at: Date;
  readonly correlationId: string;
  readonly diagnostic: WagerFailureDiagnostic | undefined;
}

/** O adaptador mantém a TX e o lock da wallet até o commit; identidade, snapshot e lease ficam com ele. */
export async function persistWagerEffects(em: EntityManager, effects: WagerEffects): Promise<void> {
  if (!em.getTransactionContext()) throw new Error('Financial persistence requires an active SQL transaction.');
  const { transaction, wallet, movement, reference, at, correlationId, diagnostic } = effects;
  if (movement) {
    const entry = WalletLedgerEntry.create({
      id: randomUUID(), walletId: wallet.id, transactionId: transaction.id,
      direction: movement.direction, money: movement.money,
      balanceBefore: movement.balanceBefore, balanceAfter: movement.balanceAfter, createdAt: at,
    });
    await sqlQuery(em, 'UPDATE wallets SET balance=?,version=?,updated_at=? WHERE id=?',
      [wallet.balance.toString(), wallet.version, wallet.updatedAt, wallet.id]);
    await sqlQuery(em, `INSERT INTO ledger_entries(
      id,wallet_id,transaction_id,currency,wallet_version,direction,amount,balance_before,balance_after,created_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?)`, [
      entry.id, entry.walletId, entry.transactionId, wallet.currency, wallet.version, entry.direction,
      entry.money.toString(), entry.balanceBefore.toString(), entry.balanceAfter.toString(), entry.createdAt,
    ]);
  }
  const events = ProcessWager.events(transaction, wallet, movement, { correlationId, occurredAt: at }, reference, diagnostic);
  const rows = events.map(event => {
    const message = OutboxMessage.enqueue(event);
    return [event.eventId, wallet.id, transaction.id, wallet.currency, event.eventType,
      JSON.stringify(message.payload), message.occurredAt, message.nextAttemptAt];
  });
  const placeholders = rows.map(() => '(?,?,?,?,?,?::jsonb,?,?)').join(',');
  await sqlQuery(em, `INSERT INTO outbox_events(
    event_id,wallet_id,transaction_id,currency,event_type,envelope,created_at,next_attempt_at
  ) VALUES ${placeholders}`, rows.flat());
}
