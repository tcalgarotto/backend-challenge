import { Migration } from '@mikro-orm/migrations';

/** REFUND e ROLLBACK da mesma BET duplicariam o crédito. Reversão aplicada é única; ROLLBACK de REFUND referencia o próprio REFUND. Dados antigos duplicados impedem o up. */
export class Migration202610110004 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`DROP INDEX wager_one_reversal;
    CREATE UNIQUE INDEX wager_one_reversal ON wager_transactions(reference_transaction_id)
      WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');`);
  }
  override async down(): Promise<void> {
    this.addSql(`DROP INDEX wager_one_reversal;
    CREATE UNIQUE INDEX wager_one_reversal ON wager_transactions(reference_transaction_id,kind)
      WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');`);
  }
}
