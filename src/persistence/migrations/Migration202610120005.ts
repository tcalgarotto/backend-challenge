import { Migration } from '@mikro-orm/migrations';

/** Trigger e backfill preservam o TTL da aceitação; down remove mensageria sem alterar wallets ou apostas. */
export class Migration202610120005 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
    CREATE TABLE wager_message_inbox (
      consumer_name text NOT NULL CHECK (consumer_name <> ''),
      message_id text NOT NULL CHECK (message_id <> ''),
      envelope_hash text NOT NULL CHECK (envelope_hash ~ '^[0-9a-f]{64}$'),
      result_snapshot jsonb,
      processed_at timestamptz,
      PRIMARY KEY (consumer_name,message_id),
      CHECK ((processed_at IS NULL) = (result_snapshot IS NULL))
    );
    REVOKE ALL ON wager_message_inbox FROM PUBLIC;
    GRANT SELECT,INSERT,UPDATE ON wager_message_inbox TO jwp_runtime;
    CREATE FUNCTION jwp_inbox_identity() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF (NEW.consumer_name,NEW.message_id,NEW.envelope_hash)
         IS DISTINCT FROM (OLD.consumer_name,OLD.message_id,OLD.envelope_hash) THEN
        RAISE EXCEPTION 'inbox identity is immutable' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER wager_inbox_identity BEFORE UPDATE ON wager_message_inbox
      FOR EACH ROW EXECUTE FUNCTION jwp_inbox_identity();

    CREATE TABLE pending_reference_jobs (
      transaction_id uuid PRIMARY KEY REFERENCES wager_transactions(id) ON DELETE CASCADE,
      next_attempt_at timestamptz NOT NULL,
      expires_at timestamptz NOT NULL,
      attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
      claim_token uuid,
      lease_until timestamptz,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now(),
      CHECK (expires_at > created_at),
      CHECK ((claim_token IS NULL) = (lease_until IS NULL))
    );
    CREATE INDEX pending_reference_ready
      ON pending_reference_jobs(next_attempt_at,transaction_id);
    REVOKE ALL ON pending_reference_jobs FROM PUBLIC;
    GRANT SELECT,INSERT,UPDATE,DELETE ON pending_reference_jobs TO jwp_runtime;

    CREATE FUNCTION jwp_schedule_pending_reference() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE accepted_at timestamptz; expires_at timestamptz;
    BEGIN
      IF NEW.status = 'PENDING_REFERENCE' THEN
        accepted_at := (NEW.response_snapshot->>'pendingReferenceAt')::timestamptz;
        expires_at := (NEW.response_snapshot->>'referenceExpiresAt')::timestamptz;
        INSERT INTO pending_reference_jobs(transaction_id,next_attempt_at,expires_at,created_at,updated_at)
          VALUES (NEW.id,accepted_at + interval '1 second',expires_at,accepted_at,accepted_at)
          ON CONFLICT (transaction_id) DO NOTHING;
      END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER wager_schedule_pending_reference AFTER INSERT ON wager_transactions
      FOR EACH ROW EXECUTE FUNCTION jwp_schedule_pending_reference();

    INSERT INTO pending_reference_jobs(transaction_id,next_attempt_at,expires_at,created_at,updated_at)
      SELECT id,
        (response_snapshot->>'pendingReferenceAt')::timestamptz + interval '1 second',
        (response_snapshot->>'referenceExpiresAt')::timestamptz,
        (response_snapshot->>'pendingReferenceAt')::timestamptz,
        now()
      FROM wager_transactions
      WHERE status='PENDING_REFERENCE'
      ON CONFLICT (transaction_id) DO NOTHING;

    CREATE TABLE wager_event_inbox (
      consumer_name text NOT NULL CHECK (consumer_name <> ''),
      event_id uuid NOT NULL,
      envelope_hash char(64) NOT NULL CHECK (envelope_hash ~ '^[0-9a-f]{64}$'),
      received_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (consumer_name,event_id)
    );
    REVOKE ALL ON wager_event_inbox FROM PUBLIC;
    GRANT SELECT,INSERT ON wager_event_inbox TO jwp_runtime;
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
    DROP TRIGGER wager_schedule_pending_reference ON wager_transactions;
    DROP FUNCTION jwp_schedule_pending_reference();
    DROP TABLE pending_reference_jobs;
    DROP TABLE wager_event_inbox;
    DROP TABLE wager_message_inbox;
    DROP FUNCTION jwp_inbox_identity();
    `);
  }
}
