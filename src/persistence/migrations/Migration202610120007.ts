import { Migration } from '@mikro-orm/migrations';

/** Escritas inserem sob lock da wallet, então seq segue a ordem de commit. O publisher reivindica apenas prefixos contíguos dessa ordem. */
export class Migration202610120007 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`ALTER TABLE outbox_events ADD COLUMN seq bigint GENERATED ALWAYS AS IDENTITY;
    ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_seq_key UNIQUE (seq);
    CREATE INDEX outbox_wallet_pending ON outbox_events(wallet_id,seq) WHERE published_at IS NULL;
    CREATE OR REPLACE FUNCTION jwp_outbox_identity() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF (NEW.event_id,NEW.wallet_id,NEW.transaction_id,NEW.currency,NEW.event_type,NEW.envelope,NEW.created_at,NEW.seq)
       IS DISTINCT FROM
         (OLD.event_id,OLD.wallet_id,OLD.transaction_id,OLD.currency,OLD.event_type,OLD.envelope,OLD.created_at,OLD.seq) THEN
        RAISE EXCEPTION 'outbox event is immutable' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$;`);
  }
  override async down(): Promise<void> {
    this.addSql(`CREATE OR REPLACE FUNCTION jwp_outbox_identity() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF (NEW.event_id,NEW.wallet_id,NEW.transaction_id,NEW.currency,NEW.event_type,NEW.envelope,NEW.created_at)
       IS DISTINCT FROM
         (OLD.event_id,OLD.wallet_id,OLD.transaction_id,OLD.currency,OLD.event_type,OLD.envelope,OLD.created_at) THEN
        RAISE EXCEPTION 'outbox event is immutable' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$;
    DROP INDEX outbox_wallet_pending;
    ALTER TABLE outbox_events DROP CONSTRAINT outbox_events_seq_key;
    ALTER TABLE outbox_events DROP COLUMN seq;`);
  }
}
