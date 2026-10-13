import { Migration } from '@mikro-orm/migrations';


export class Migration202610110003 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`ALTER TABLE wager_transactions ADD COLUMN processed_at timestamptz;
    CREATE TABLE outbox_events (
      event_id uuid PRIMARY KEY,
      wallet_id uuid NOT NULL REFERENCES wallets(id),
      transaction_id uuid NOT NULL,
      currency text NOT NULL,
      event_type text NOT NULL,
      envelope jsonb NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      next_attempt_at timestamptz NOT NULL DEFAULT now(),
      published_at timestamptz,
      attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
      claim_token uuid,
      lease_until timestamptz,
      FOREIGN KEY (transaction_id,wallet_id,currency) REFERENCES wager_transactions(id,wallet_id,currency),
      CHECK ((claim_token IS NULL) = (lease_until IS NULL)),
      CHECK (jsonb_typeof(envelope) = 'object' AND
        envelope->>'eventId' IS NOT NULL AND envelope->>'eventId' = event_id::text AND
        envelope->>'eventType' IS NOT NULL AND envelope->>'eventType' = event_type AND
        envelope->>'version' IS NOT NULL AND envelope->>'version' = '1')
    );
    CREATE INDEX outbox_ready ON outbox_events(next_attempt_at,event_id) WHERE published_at IS NULL;
    REVOKE ALL ON outbox_events FROM PUBLIC;
    GRANT SELECT,INSERT,UPDATE ON outbox_events TO jwp_runtime;
    CREATE FUNCTION jwp_outbox_identity() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF (NEW.event_id,NEW.wallet_id,NEW.transaction_id,NEW.currency,NEW.event_type,NEW.envelope,NEW.created_at)
       IS DISTINCT FROM
         (OLD.event_id,OLD.wallet_id,OLD.transaction_id,OLD.currency,OLD.event_type,OLD.envelope,OLD.created_at) THEN
        RAISE EXCEPTION 'outbox event is immutable' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER outbox_identity BEFORE UPDATE ON outbox_events FOR EACH ROW EXECUTE FUNCTION jwp_outbox_identity();`);
  }
  override async down(): Promise<void> {
    this.addSql('DROP TABLE outbox_events; DROP FUNCTION jwp_outbox_identity(); ALTER TABLE wager_transactions DROP COLUMN processed_at;');
  }
}
