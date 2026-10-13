import { Migration } from '@mikro-orm/migrations';

/** Preserva a data histórica do último movimento antes de persistir updated_at. */
export class Migration202610130009 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`ALTER TABLE wallets ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();
      UPDATE wallets w SET updated_at=greatest(w.created_at,coalesce(
        (SELECT l.created_at FROM ledger_entries l WHERE l.wallet_id=w.id ORDER BY l.wallet_version DESC LIMIT 1),w.created_at));
      -- Executa as constraints diferidas do backfill antes de alterar a tabela.
      SET CONSTRAINTS ALL IMMEDIATE;
      ALTER TABLE wallets ADD CONSTRAINT wallet_timestamps CHECK (updated_at>=created_at);
      CREATE FUNCTION jwp_wallet_timestamps() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.created_at IS DISTINCT FROM OLD.created_at OR NEW.updated_at<OLD.updated_at THEN
          RAISE EXCEPTION 'wallet timestamps are immutable or monotonic' USING ERRCODE='23514';
        END IF;
        IF NEW.balance IS NOT DISTINCT FROM OLD.balance AND NEW.updated_at IS DISTINCT FROM OLD.updated_at THEN
          RAISE EXCEPTION 'wallet timestamp changes only with balance' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER wallet_timestamps BEFORE UPDATE ON wallets FOR EACH ROW EXECUTE FUNCTION jwp_wallet_timestamps();
      SET CONSTRAINTS ALL DEFERRED;`);
  }
  override async down(): Promise<void> {
    this.addSql(`DROP TRIGGER wallet_timestamps ON wallets;
      DROP FUNCTION jwp_wallet_timestamps();
      ALTER TABLE wallets DROP CONSTRAINT wallet_timestamps;
      ALTER TABLE wallets DROP COLUMN updated_at;`);
  }
}
