import { Migration } from '@mikro-orm/migrations';
/** Linhas antigas processadas usam o instante conhecido para preencher o recebimento. */
export class Migration202610120006 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`ALTER TABLE wager_message_inbox ADD COLUMN received_at timestamptz;
      UPDATE wager_message_inbox SET received_at=coalesce(processed_at,now());
      ALTER TABLE wager_message_inbox ALTER COLUMN received_at SET DEFAULT now(), ALTER COLUMN received_at SET NOT NULL,
        ADD CONSTRAINT wager_inbox_processing_time CHECK (processed_at IS NULL OR processed_at>=received_at);
      CREATE OR REPLACE FUNCTION jwp_inbox_identity() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.consumer_name,NEW.message_id,NEW.envelope_hash,NEW.received_at)
           IS DISTINCT FROM (OLD.consumer_name,OLD.message_id,OLD.envelope_hash,OLD.received_at)
           OR (OLD.processed_at IS NOT NULL AND (NEW.processed_at,NEW.result_snapshot)
             IS DISTINCT FROM (OLD.processed_at,OLD.result_snapshot)) THEN
          RAISE EXCEPTION 'inbox identity and committed result are immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;`);
  }
  override async down(): Promise<void> {
    this.addSql(`CREATE OR REPLACE FUNCTION jwp_inbox_identity() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.consumer_name,NEW.message_id,NEW.envelope_hash)
           IS DISTINCT FROM (OLD.consumer_name,OLD.message_id,OLD.envelope_hash) THEN
          RAISE EXCEPTION 'inbox identity is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      ALTER TABLE wager_message_inbox DROP CONSTRAINT wager_inbox_processing_time, DROP COLUMN received_at;`);
  }
}
