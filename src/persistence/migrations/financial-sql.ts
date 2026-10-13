// As invariantes também protegem escritas SQL diretas, além das feitas pelo ORM.
// jwp_runtime é global ao cluster: criar somente se ausente exige CREATEROLE;
// reutilizar papel provisionado requer validar seus atributos, permitindo migrar
// outro banco do cluster ou usar owner sem CREATEROLE.
export const financialUp = String.raw`
DO $$
DECLARE r pg_roles;
BEGIN
 SELECT * INTO r FROM pg_roles WHERE rolname='jwp_runtime';
 IF NOT FOUND THEN
  CREATE ROLE jwp_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
 ELSIF r.rolcanlogin OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolinherit OR r.rolreplication OR r.rolbypassrls
    OR EXISTS (SELECT 1 FROM pg_auth_members WHERE member=r.oid) THEN
  RAISE EXCEPTION 'existing role jwp_runtime must be NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS and member of no role'
   USING ERRCODE='42501';
 END IF;
END $$;
CREATE TABLE wallets (
 id uuid PRIMARY KEY,
 player_id text NOT NULL CHECK (player_id <> ''),
 currency text NOT NULL CHECK (currency IN ('BRL','USD','EUR')),
 balance numeric(20,2) NOT NULL CHECK (balance >= 0 AND balance <> 'NaN'::numeric),
 version bigint NOT NULL CHECK (version >= 1),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (player_id,currency), UNIQUE (id,player_id,currency)
);
CREATE TABLE wager_transactions (
 id uuid PRIMARY KEY,
 wallet_id uuid NOT NULL,
 player_id text NOT NULL,
 currency text NOT NULL,
 provider_id text NOT NULL CHECK (provider_id <> ''),
 external_transaction_id text NOT NULL CHECK (external_transaction_id <> ''),
 idempotency_key text NOT NULL CHECK (idempotency_key <> ''),
 payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
 round_id text NOT NULL,
 game_id text NOT NULL,
 kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
 status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
 amount numeric(20,2) NOT NULL CHECK (amount <> 'NaN'::numeric AND ((kind='LOSS' AND amount=0) OR (kind<>'LOSS' AND amount>0))),
 reference_external_transaction_id text,
 reference_transaction_id uuid REFERENCES wager_transactions(id),
 failure_code text,
 response_snapshot jsonb,
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK ((kind IN ('REFUND','ROLLBACK') AND reference_external_transaction_id IS NOT NULL) OR
        (kind='WIN') OR (kind IN ('OPENING','BET','LOSS') AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)),
 CHECK (status<>'PROCESSED' OR kind NOT IN ('REFUND','ROLLBACK') OR reference_transaction_id IS NOT NULL),
 CHECK (kind<>'OPENING' OR status='PROCESSED'),
 CHECK (status NOT IN ('REJECTED','FAILED') OR failure_code IS NOT NULL),
 UNIQUE(provider_id,idempotency_key), UNIQUE(provider_id,external_transaction_id),
 UNIQUE(id,wallet_id,currency),
 FOREIGN KEY(wallet_id,player_id,currency) REFERENCES wallets(id,player_id,currency)
);
CREATE UNIQUE INDEX wager_one_opening ON wager_transactions(wallet_id) WHERE kind='OPENING';
CREATE UNIQUE INDEX wager_one_reversal ON wager_transactions(reference_transaction_id,kind)
 WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');
CREATE TABLE ledger_entries (
 id uuid PRIMARY KEY,
 wallet_id uuid NOT NULL REFERENCES wallets(id),
 transaction_id uuid NOT NULL,
 currency text NOT NULL,
 wallet_version bigint NOT NULL CHECK (wallet_version>=1),
 direction text NOT NULL CHECK (direction IN ('CREDIT','DEBIT')),
 amount numeric(20,2) NOT NULL CHECK (amount>0 AND amount<>'NaN'::numeric),
 balance_before numeric(20,2) NOT NULL CHECK (balance_before>=0 AND balance_before<>'NaN'::numeric),
 balance_after numeric(20,2) NOT NULL CHECK (balance_after>=0 AND balance_after<>'NaN'::numeric),
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK ((direction='CREDIT' AND balance_after=balance_before+amount) OR
        (direction='DEBIT' AND balance_after=balance_before-amount)),
 UNIQUE(wallet_id,transaction_id), UNIQUE(wallet_id,wallet_version),
 FOREIGN KEY(transaction_id,wallet_id,currency) REFERENCES wager_transactions(id,wallet_id,currency)
);
CREATE FUNCTION jwp_ledger_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'ledger is append-only' USING ERRCODE='23514'; END $$;
CREATE TRIGGER ledger_no_mutation BEFORE UPDATE OR DELETE ON ledger_entries
 FOR EACH ROW EXECUTE FUNCTION jwp_ledger_immutable();
CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON ledger_entries
 FOR EACH STATEMENT EXECUTE FUNCTION jwp_ledger_immutable();
CREATE FUNCTION jwp_ledger_append() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE last_version bigint;
BEGIN
 -- Serializa cada append no lock da wallet, inclusive escritas SQL diretas.
 PERFORM 1 FROM wallets WHERE id=NEW.wallet_id FOR UPDATE;
 SELECT wallet_version INTO last_version FROM ledger_entries WHERE wallet_id=NEW.wallet_id ORDER BY wallet_version DESC LIMIT 1;
 IF last_version IS NOT NULL AND NEW.wallet_version<last_version THEN
  RAISE EXCEPTION 'ledger must append in version order' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER ledger_append BEFORE INSERT ON ledger_entries FOR EACH ROW EXECUTE FUNCTION jwp_ledger_append();
CREATE FUNCTION jwp_wallet_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.version<>1 THEN RAISE EXCEPTION 'wallet starts at version 1' USING ERRCODE='23514'; END IF;
 ELSIF (NEW.id,NEW.player_id,NEW.currency) IS DISTINCT FROM (OLD.id,OLD.player_id,OLD.currency) THEN
  RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_identity BEFORE INSERT OR UPDATE ON wallets FOR EACH ROW EXECUTE FUNCTION jwp_wallet_identity();
CREATE FUNCTION jwp_wager_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.id,NEW.wallet_id,NEW.player_id,NEW.currency,NEW.provider_id,NEW.external_transaction_id,NEW.idempotency_key,
     NEW.payload_hash,NEW.round_id,NEW.game_id,NEW.kind,NEW.amount,NEW.reference_external_transaction_id)
 IS DISTINCT FROM
    (OLD.id,OLD.wallet_id,OLD.player_id,OLD.currency,OLD.provider_id,OLD.external_transaction_id,OLD.idempotency_key,
     OLD.payload_hash,OLD.round_id,OLD.game_id,OLD.kind,OLD.amount,OLD.reference_external_transaction_id)
 OR (OLD.reference_transaction_id IS NOT NULL AND NEW.reference_transaction_id IS DISTINCT FROM OLD.reference_transaction_id)
 OR (OLD.status IN ('PROCESSED','REJECTED','FAILED') AND NEW IS DISTINCT FROM OLD) THEN
  RAISE EXCEPTION 'wager identity and terminal result are immutable' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wager_immutable BEFORE UPDATE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION jwp_wager_immutable();
CREATE FUNCTION jwp_validate_wallet(target uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE w wallets; last ledger_entries;
BEGIN
 SELECT * INTO w FROM wallets WHERE id=target;
 IF NOT FOUND THEN RETURN; END IF;
 SELECT * INTO last FROM ledger_entries WHERE wallet_id=target ORDER BY wallet_version DESC LIMIT 1;
 IF FOUND THEN
  IF (w.balance,w.version) IS DISTINCT FROM (last.balance_after,last.wallet_version) THEN
   RAISE EXCEPTION 'wallet final balance/version must match ledger' USING ERRCODE='23514';
  END IF;
 ELSIF w.balance<>0 OR w.version<>1 THEN
  RAISE EXCEPTION 'wallet without ledger must be zero at version 1' USING ERRCODE='23514';
 END IF;
END $$;
CREATE FUNCTION jwp_validate_wager(target uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE t wager_transactions; r wager_transactions; l ledger_entries; expected_direction text;
BEGIN
 SELECT * INTO t FROM wager_transactions WHERE id=target;
 IF NOT FOUND THEN RETURN; END IF;
 SELECT * INTO l FROM ledger_entries WHERE wallet_id=t.wallet_id AND transaction_id=t.id;
 IF t.status='PROCESSED' AND t.kind<>'LOSS' THEN
  IF NOT FOUND THEN RAISE EXCEPTION 'processed monetary wager requires ledger' USING ERRCODE='23514'; END IF;
 ELSE
  IF FOUND THEN RAISE EXCEPTION 'nonmonetary or unprocessed wager cannot have ledger' USING ERRCODE='23514'; END IF;
  RETURN;
 END IF;
 expected_direction := CASE WHEN t.kind='BET' THEN 'DEBIT' ELSE 'CREDIT' END;
 IF t.reference_transaction_id IS NOT NULL THEN
  SELECT * INTO r FROM wager_transactions WHERE id=t.reference_transaction_id;
  IF NOT FOUND OR r.status<>'PROCESSED' OR r.id=t.id OR
     (r.provider_id,r.wallet_id,r.player_id,r.currency,r.round_id,r.external_transaction_id) IS DISTINCT FROM
     (t.provider_id,t.wallet_id,t.player_id,t.currency,t.round_id,t.reference_external_transaction_id) OR
     (t.kind='WIN' AND r.kind<>'BET') OR
     (t.kind='REFUND' AND r.kind<>'BET') OR
     (t.kind='ROLLBACK' AND r.kind NOT IN ('BET','WIN','REFUND')) OR
     (t.kind IN ('REFUND','ROLLBACK') AND t.amount<>r.amount) THEN
   RAISE EXCEPTION 'invalid processed wager reference' USING ERRCODE='23514';
  END IF;
  IF t.kind='ROLLBACK' AND r.kind IN ('WIN','REFUND') THEN expected_direction := 'DEBIT'; END IF;
 ELSIF t.kind='WIN' AND t.reference_external_transaction_id IS NOT NULL THEN
  RAISE EXCEPTION 'processed referenced WIN must resolve its reference' USING ERRCODE='23514';
 END IF;
 IF l.amount<>t.amount OR l.direction<>expected_direction THEN
  RAISE EXCEPTION 'ledger amount/direction must match wager' USING ERRCODE='23514';
 END IF;
 IF (t.kind='OPENING') IS DISTINCT FROM (l.wallet_version=1) OR (t.kind='OPENING' AND l.balance_before<>0) THEN
  RAISE EXCEPTION 'OPENING must be first version from zero' USING ERRCODE='23514';
 END IF;
END $$;
CREATE FUNCTION jwp_financial_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE prior ledger_entries;
BEGIN
 IF TG_TABLE_NAME='wallets' THEN
  PERFORM jwp_validate_wallet(NEW.id);
 ELSIF TG_TABLE_NAME='wager_transactions' THEN
  PERFORM jwp_validate_wager(NEW.id);
 ELSE
  SELECT * INTO prior FROM ledger_entries WHERE wallet_id=NEW.wallet_id AND wallet_version=NEW.wallet_version-1;
  IF FOUND THEN
   IF NEW.balance_before<>prior.balance_after THEN RAISE EXCEPTION 'broken ledger chain' USING ERRCODE='23514'; END IF;
  ELSIF NEW.balance_before<>0 OR NEW.wallet_version NOT IN (1,2) THEN
   RAISE EXCEPTION 'missing ledger predecessor' USING ERRCODE='23514';
  END IF;
  PERFORM jwp_validate_wager(NEW.transaction_id);
  PERFORM jwp_validate_wallet(NEW.wallet_id);
 END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER wallet_consistency AFTER INSERT OR UPDATE ON wallets
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION jwp_financial_consistency();
CREATE CONSTRAINT TRIGGER wager_consistency AFTER INSERT OR UPDATE ON wager_transactions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION jwp_financial_consistency();
CREATE CONSTRAINT TRIGGER ledger_consistency AFTER INSERT ON ledger_entries
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION jwp_financial_consistency();
REVOKE ALL ON wallets,wager_transactions,ledger_entries FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO jwp_runtime;
GRANT SELECT,INSERT,UPDATE ON wallets,wager_transactions TO jwp_runtime;
GRANT SELECT,INSERT ON ledger_entries TO jwp_runtime;
`;

export const financialDown = String.raw`
DROP TABLE ledger_entries;
DROP TABLE wager_transactions;
DROP TABLE wallets;
DROP FUNCTION jwp_financial_consistency();
DROP FUNCTION jwp_validate_wager(uuid);
DROP FUNCTION jwp_validate_wallet(uuid);
DROP FUNCTION jwp_wager_immutable();
DROP FUNCTION jwp_wallet_identity();
DROP FUNCTION jwp_ledger_append();
DROP FUNCTION jwp_ledger_immutable();
REVOKE USAGE ON SCHEMA public FROM jwp_runtime;
`;
// Down não remove jwp_runtime: outros bancos e LOGINs podem depender dele.
// Remover o papel é tarefa do DBA (DROP ROLE), não da migration.
