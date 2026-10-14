import { Migration } from '@mikro-orm/migrations';

/**
 * Política de reversão configurável (JWP-ADR-005). O índice volta a ser por (referência, tipo): é a garantia
 * no banco da política padrão per_type, conforme o enunciado (um REFUND e um ROLLBACK por referência).
 * A política opcional per_reference ganha um gatilho que, quando a transação publica
 * jwp.reversal_policy=per_reference, recusa uma segunda reversão PROCESSED da mesma referência.
 * O advisory lock por referência serializa escritas concorrentes mesmo fora do lock da wallet.
 * O down restaura o índice por referência e falha (REVERSAL_POLICY_DOWN_BLOCKED) se já houver REFUND e ROLLBACK
 * aplicados à mesma referência: nenhum dado financeiro é apagado para permitir o rollback de schema.
 */
export class Migration202610140010 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`DROP INDEX wager_one_reversal;
    CREATE UNIQUE INDEX wager_one_reversal ON wager_transactions(reference_transaction_id,kind)
      WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');
    CREATE FUNCTION jwp_reversal_policy_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.status='PROCESSED' AND NEW.kind IN ('REFUND','ROLLBACK') AND NEW.reference_transaction_id IS NOT NULL
         AND coalesce(current_setting('jwp.reversal_policy', true), '')='per_reference' THEN
        PERFORM pg_advisory_xact_lock(hashtextextended('jwp_reversal:' || NEW.reference_transaction_id::text, 0));
        IF EXISTS (SELECT 1 FROM wager_transactions
                    WHERE reference_transaction_id=NEW.reference_transaction_id AND id<>NEW.id
                      AND status='PROCESSED' AND kind IN ('REFUND','ROLLBACK')) THEN
          RAISE EXCEPTION 'reversal already applied to this reference' USING ERRCODE='23505';
        END IF;
      END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER wager_reversal_policy BEFORE INSERT OR UPDATE OF status ON wager_transactions
      FOR EACH ROW EXECUTE FUNCTION jwp_reversal_policy_guard();`);
  }
  override async down(): Promise<void> {
    this.addSql(`DO $$
    BEGIN
      IF EXISTS (SELECT 1 FROM wager_transactions WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK')
                  GROUP BY reference_transaction_id HAVING count(*)>1) THEN
        RAISE EXCEPTION 'REVERSAL_POLICY_DOWN_BLOCKED: references with more than one applied reversal' USING ERRCODE='23505';
      END IF;
    END $$;
    DROP TRIGGER wager_reversal_policy ON wager_transactions;
    DROP FUNCTION jwp_reversal_policy_guard();
    DROP INDEX wager_one_reversal;
    CREATE UNIQUE INDEX wager_one_reversal ON wager_transactions(reference_transaction_id)
      WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');`);
  }
}
