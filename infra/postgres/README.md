# PostgreSQL

PostgreSQL 17.6 via `compose.db.yml` (database/owner `jwp`, host port `JWP_PG_PORT`, default 55432).
`compose.yml` runs the whole setup (migrate, runtime LOGIN, app replicas); manual steps are in the README.

## Roles and privileges

- **Migration owner** (`JWP_MIGRATION_DATABASE_URL`): owns every table and runs `bun run db:migrate`
  (MikroORM migrations 002–007, table `jwp_financial_migrations`). The application never runs DDL.
- **`jwp_runtime`**: NOLOGIN, NOINHERIT, no superuser/createdb/createrole/replication/bypassrls, member of no role.
  It receives only the DML grants of the schema (ledger is SELECT/INSERT only) and owns nothing.
  It is a cluster-wide role:
  - migration 002 creates it when absent, which requires an owner with `CREATEROLE`;
  - when a DBA provisioned it beforehand (or another database of the cluster already created it), the
    migration reuses it after validating the attributes above and fails with SQLSTATE 42501 otherwise.
    An owner without `CREATEROLE` therefore works once the role exists;
  - down removes its grants in this database but never drops the role, so other databases and the
    LOGIN memberships keep working across down/up. Dropping it is a DBA task.
- **Runtime LOGIN** (`JWP_DATABASE_URL`, e.g. `jwp_api`): a non-superuser LOGIN granted `jwp_runtime`;
  every transaction runs `SET LOCAL ROLE jwp_runtime`.

## Money in SQL

Amounts are `numeric(20,2)`. The driver returns them as strings; `FinancialDecimalType`
(`src/persistence/financial-orm.ts`) keeps strings end to end, compares them exactly and rejects any
non two-decimal value, so no amount passes through `Number`.

Invariants (balance >= 0, ledger append-only and chained, wallet = last ledger entry, one reversal per
reference) are CHECKs and deferred constraint triggers in `src/persistence/migrations/financial-sql.ts`,
so they also hold for direct SQL. `tests/integration/schema` proves them and the up/down/up cycle.
