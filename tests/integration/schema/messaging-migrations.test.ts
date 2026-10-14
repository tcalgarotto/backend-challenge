import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';
import { ProcessInboxWager, SqlInboxProcessWagerStore } from '../../../src/messaging/sql-inbox-process-wager-store.js';
import { SqlEventDeduplicator } from '../../../src/messaging/outbox/sqs-event-consumer.js';
import { ProcessWager } from '../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../src/wagering/infrastructure/sql-process-wager-store.js';
import { downToZeroAfterReversals } from '../support/reversal-policy-teardown.js';
import { SqlOutboxStore } from '../../../src/messaging/outbox/sql-outbox-store.js';

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let runtimeOrm: Awaited<ReturnType<typeof openFinancialOrm>> | undefined;
const role = `jwp_migration_test_${randomUUID().replaceAll('-', '')}`;
const tables = ['wager_message_inbox', 'pending_reference_jobs', 'wager_event_inbox'];
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<Record<string, unknown>[]>(sql, args);
async function relations(present: boolean) {
  for (const table of tables) {
    expect((await q('SELECT to_regclass(?)::text relation', [table]))[0]?.['relation']).toBe(present ? table : null);
  }
}
async function violation(sql: string, args: unknown[], code: string) {
  let caught: unknown;
  try {
    await runtimeOrm!.em.fork().transactional(async em => {
      await em.getConnection().execute('SET LOCAL ROLE jwp_runtime', [], 'all', em.getTransactionContext());
      await em.getConnection().execute(sql, args, 'all', em.getTransactionContext());
    });
  } catch (error) { caught = error; }
  expect((caught as { code?: string } | undefined)?.code).toBe(code);
}

beforeAll(async () => {
  const url = Bun.env['JWP_MESSAGING_SCHEMA_DATABASE_URL'];
  if (!url) throw new Error('JWP_MESSAGING_SCHEMA_DATABASE_URL required: disposable empty database with migration owner credentials.');
  orm = await openFinancialOrm(url);
  expect((await q("SELECT to_regclass('wallets') relation"))[0]?.['relation']).toBeNull();
  const migrate = async () => {
    const child = Bun.spawn(['bun', 'run', 'db:migrate'], { env: { ...Bun.env, JWP_MIGRATION_DATABASE_URL: url }, stdout: 'pipe', stderr: 'pipe' });
    const [exit, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
    if (exit !== 0) throw new Error(`db:migrate exited ${exit}: ${stdout} ${stderr}`);
    expect(exit).toBe(0);
  };
  await migrate();
  await relations(true);
  await orm.migrator.down({ to: 0 });
  await relations(false);
  expect((await q("SELECT count(*)::int n FROM pg_proc WHERE proname LIKE 'jwp_%'"))[0]?.['n']).toBe(0);
  await migrate();
  await relations(true);
  expect((await orm.migrator.getExecutedMigrations()).map(m => m.name)).toEqual([
    'Migration202610110002', 'Migration202610110003', 'Migration202610110004', 'Migration202610120005', 'Migration202610120006',
    'Migration202610120007', 'Migration202610120008', 'Migration202610130009', 'Migration202610140010',
  ]);
  await q(`CREATE ROLE ${role} LOGIN PASSWORD 'migration_test_only'`);
  await q(`GRANT jwp_runtime TO ${role}`);
  const runtimeUrl = new URL(url); runtimeUrl.username = role; runtimeUrl.password = 'migration_test_only';
  runtimeOrm = await openFinancialOrm(runtimeUrl.toString());
}, 30000);

afterAll(async () => {
  await runtimeOrm?.close(true);
  if (orm) try {
    // O último teste deixa REFUND e ROLLBACK aplicados à mesma BET: o down recusa e o banco descartável é esvaziado.
    await downToZeroAfterReversals(orm, true);
    await q(`DROP ROLE IF EXISTS ${role}`);
  } finally { await orm.close(true); }
});

test('up/down/up restores messaging keys, ready index, triggers and least-privilege grants', async () => {
  expect((await q("SELECT count(*)::int n FROM pg_trigger WHERE tgname IN ('wager_inbox_identity','wager_schedule_pending_reference') AND NOT tgisinternal"))[0]?.['n']).toBe(2);
  expect((await q("SELECT pg_get_indexdef('pending_reference_ready'::regclass) def"))[0]?.['def']).toContain('(next_attempt_at, transaction_id)');
  for (const table of tables) {
    expect((await q('SELECT has_table_privilege(?, ?, ?) allowed', ['jwp_runtime', table, 'SELECT,INSERT']))[0]?.['allowed']).toBe(true);
    expect((await q('SELECT has_table_privilege(?, ?, ?) allowed', ['jwp_runtime', table, 'TRUNCATE']))[0]?.['allowed']).toBe(false);
  }
  for (const [table, permission, allowed] of [
    ['wager_message_inbox', 'UPDATE', true], ['wager_message_inbox', 'DELETE', false],
    ['pending_reference_jobs', 'UPDATE', true], ['pending_reference_jobs', 'DELETE', true],
    ['wager_event_inbox', 'UPDATE', false], ['wager_event_inbox', 'DELETE', false],
  ] as const) expect((await q('SELECT has_table_privilege(?, ?, ?) allowed', ['jwp_runtime', table, permission]))[0]?.['allowed']).toBe(allowed);
  expect((await q("SELECT has_schema_privilege('jwp_runtime','public','CREATE') allowed"))[0]?.['allowed']).toBe(false);
  console.log(JSON.stringify({ proof: 'messaging-up-down-up', tables, migrations: (await orm.migrator.getExecutedMigrations()).map(m => m.name) }));
});

test('schema rejects invalid inbox state, mutable identity, duplicate events and invalid jobs', async () => {
  const message = randomUUID();
  await q('INSERT INTO wager_message_inbox(consumer_name,message_id,envelope_hash) VALUES (?,?,?)', ['test', message, 'a'.repeat(64)]);
  await violation('UPDATE wager_message_inbox SET envelope_hash=? WHERE message_id=?', ['b'.repeat(64), message], '23514');
  await violation('UPDATE wager_message_inbox SET processed_at=now() WHERE message_id=?', [message], '23514');
  await violation('INSERT INTO wager_message_inbox(consumer_name,message_id,envelope_hash) VALUES (?,?,?)', ['test', message, 'a'.repeat(64)], '23505');
  await violation('INSERT INTO wager_message_inbox(consumer_name,message_id,envelope_hash) VALUES (?,?,?)', ['', randomUUID(), 'bad'], '23514');
  const event = randomUUID();
  await q('INSERT INTO wager_event_inbox(consumer_name,event_id,envelope_hash) VALUES (?,?,?)', ['test', event, 'a'.repeat(64)]);
  await violation('INSERT INTO wager_event_inbox(consumer_name,event_id,envelope_hash) VALUES (?,?,?)', ['test', event, 'a'.repeat(64)], '23505');
  await violation('INSERT INTO pending_reference_jobs(transaction_id,next_attempt_at,expires_at) VALUES (?,now(),now()+interval \'1 hour\')', [randomUUID()], '23503');
});

test('non-owner runtime processes inbox, schedules references and deduplicates events after migrations only', async () => {
  const target = await new OpenWallet(new SqlWalletOpeningStore(runtimeOrm!)).execute({ playerId: randomUUID(), initialBalance: { amount: '0.00', currency: 'BRL' } });
  const suffix = randomUUID();
  const command = { providerId: 'migration', externalTransactionId: suffix, walletId: target.id, playerId: target.playerId,
    roundId: 'round', gameId: 'game', kind: 'REFUND', money: { amount: '1.00', currency: 'BRL' }, referenceExternalTransactionId: `missing-${suffix}` };
  const processor = new ProcessInboxWager(new SqlInboxProcessWagerStore(runtimeOrm!));
  const metadata = { consumerName: 'migration', messageId: randomUUID(), envelopeHash: 'a'.repeat(64) };
  const result = await processor.execute(command, suffix, metadata);
  expect(result).toMatchObject({ body: { status: 'PENDING_REFERENCE' } });
  expect(await processor.execute(command, suffix, metadata)).toEqual(result);
  const [job] = await q('SELECT transaction_id,expires_at,created_at FROM pending_reference_jobs WHERE transaction_id=?', [result.body.transactionId]);
  expect(job).toBeDefined();
  const store = new SqlOutboxStore(runtimeOrm!);
  const claimed = await store.claimBatch(10);
  expect(claimed).toHaveLength(1);
  const deduplicator = new SqlEventDeduplicator(runtimeOrm!);
  expect(await deduplicator.accept(claimed[0]!.envelope)).toEqual({ duplicate: false });
  expect(await deduplicator.accept(claimed[0]!.envelope)).toEqual({ duplicate: true });
  await violation('UPDATE pending_reference_jobs SET attempts=-1 WHERE transaction_id=?', [result.body.transactionId], '23514');
  await violation('UPDATE pending_reference_jobs SET claim_token=? WHERE transaction_id=?', [randomUUID(), result.body.transactionId], '23514');
  await violation('UPDATE pending_reference_jobs SET expires_at=created_at WHERE transaction_id=?', [result.body.transactionId], '23514');
  console.log(JSON.stringify({ proof: 'runtime-without-DDL', role, status: result.body.status }));
});

test('rolling back only 0005 preserves wagers and up backfills their original reference deadline', async () => {
  const before = await q('SELECT transaction_id,expires_at,created_at FROM pending_reference_jobs');
  expect(before).toHaveLength(1);
  await orm.migrator.down({ to: 'Migration202610110004' });
  await relations(false);
  expect((await q('SELECT count(*)::int n FROM wager_transactions'))[0]?.['n']).toBe(1);
  expect((await q("SELECT count(*)::int n FROM pg_proc WHERE proname IN ('jwp_inbox_identity','jwp_schedule_pending_reference')"))[0]?.['n']).toBe(0);
  await orm.migrator.up();
  await relations(true);
  expect(await q('SELECT transaction_id,expires_at,created_at FROM pending_reference_jobs')).toEqual(before);
  const target = await q('SELECT response_snapshot FROM wager_transactions');
  expect(new Date(before[0]!['expires_at'] as string).toISOString()).toBe((target[0]!['response_snapshot'] as { referenceExpiresAt: string }).referenceExpiresAt);
  console.log(JSON.stringify({ proof: 'messaging-only-down-up-backfill', pendingJobs: before.length }));
});

test('outbox order migration round-trips the seq column and its pending index', async () => {
  const shape = async () => (await q(`SELECT
    (SELECT count(*)::int FROM information_schema.columns WHERE table_name='outbox_events' AND column_name='seq') seq,
    (SELECT count(*)::int FROM pg_indexes WHERE indexname='outbox_wallet_pending') idx`))[0];
  expect(await shape()).toEqual({ seq: 1, idx: 1 });
  await orm.migrator.down({ migrations: ['Migration202610120007'] });
  expect(await shape()).toEqual({ seq: 0, idx: 0 });
  await orm.migrator.up({ migrations: ['Migration202610120007'] });
  expect(await shape()).toEqual({ seq: 1, idx: 1 });
});

test('scheduler correlation lookup migration round-trips its transaction/order index', async () => {
  const indexDef = async () => (await q(`SELECT pg_get_indexdef('outbox_transaction_origin_idx'::regclass) def`))[0]?.['def'];
  expect(await indexDef()).toContain('(transaction_id, created_at, event_id)');
  const plan = await orm.em.fork().transactional(async em => {
    const connection = em.getConnection();
    const context = em.getTransactionContext();
    await connection.execute('SET LOCAL enable_seqscan=off', [], 'all', context);
    return connection.execute(`EXPLAIN (COSTS OFF) SELECT envelope->>'correlationId' FROM outbox_events
      WHERE transaction_id=? ORDER BY created_at,event_id LIMIT 1`, [randomUUID()], 'all', context);
  });
  expect(plan.map(row => Object.values(row).join(' ')).join('\n')).toContain('outbox_transaction_origin_idx');
  await orm.migrator.down({ migrations: ['Migration202610120008'] });
  expect((await q("SELECT to_regclass('outbox_transaction_origin_idx') relation"))[0]?.['relation']).toBeNull();
  await orm.migrator.up({ migrations: ['Migration202610120008'] });
  expect(await indexDef()).toContain('(transaction_id, created_at, event_id)');
});

test('inbox receipt migration round-trips and backfills historical processing time without rewriting it', async () => {
  // Down inclui o alvo; selecionar só 007/006 preserva a inbox da 005.
  await orm.migrator.down({ migrations: ['Migration202610120007', 'Migration202610120006'] });
  const columnCount = async (table: string, column: string) =>
    (await q('SELECT count(*)::int n FROM information_schema.columns WHERE table_name=? AND column_name=?', [table, column]))[0]?.['n'];
  expect(await columnCount('wager_message_inbox', 'received_at')).toBe(0);
  expect(await columnCount('outbox_events', 'seq')).toBe(0);
  const id=randomUUID(), received='2026-01-01T00:00:00.000Z';
  await q(`INSERT INTO wager_message_inbox(consumer_name,message_id,envelope_hash,result_snapshot,processed_at)
    VALUES ('receipt-migration',?,'${'a'.repeat(64)}','{}'::jsonb,?)`,[id,received]);

  await orm.migrator.up({ migrations: ['Migration202610120006'] });
  expect(await columnCount('wager_message_inbox', 'received_at')).toBe(1);
  expect(await columnCount('outbox_events', 'seq')).toBe(0);
  const [row]=await q('SELECT received_at,processed_at FROM wager_message_inbox WHERE message_id=?',[id]);
  expect(new Date(row!['received_at'] as string).toISOString()).toBe(received);
  expect(new Date(row!['processed_at'] as string).toISOString()).toBe(received);
  await violation("UPDATE wager_message_inbox SET received_at=received_at-interval '1 second' WHERE message_id=?",[id],'23514');
  await violation("UPDATE wager_message_inbox SET processed_at=processed_at+interval '1 second' WHERE message_id=?",[id],'23514');
  await orm.migrator.up({ migrations: ['Migration202610120007'] });
  expect(await columnCount('outbox_events', 'seq')).toBe(1);
});

test('wallet timestamps migrate from the old schema, reverse and preserve historical balances',async()=>{
  const wallet=await new OpenWallet(new SqlWalletOpeningStore(runtimeOrm!)).execute({playerId:randomUUID(),
    initialBalance:{amount:'100.00',currency:'BRL'}});
  const [before]=await q('SELECT balance::text,version::text,created_at,updated_at FROM wallets WHERE id=?',[wallet.id]);
  await orm.migrator.down({migrations:['Migration202610130009']});
  expect((await q("SELECT count(*)::int n FROM information_schema.columns WHERE table_name='wallets' AND column_name='updated_at'"))[0]?.n).toBe(0);
  await orm.migrator.up();
  const [after]=await q('SELECT balance::text,version::text,created_at,updated_at FROM wallets WHERE id=?',[wallet.id]);
  expect(after).toEqual(before);
  await violation("UPDATE wallets SET updated_at=created_at-interval '1 second' WHERE id=?",[wallet.id],'23514');
  await violation("UPDATE wallets SET created_at=created_at+interval '1 second' WHERE id=?",[wallet.id],'23514');
});

test('reversal policy migration 0010 round-trips index and guard; down refuses while REFUND and ROLLBACK coexist', async () => {
  const indexDef = async () => (await q("SELECT pg_get_indexdef('wager_one_reversal'::regclass) def"))[0]?.['def'] as string;
  const guard = async () => (await q("SELECT count(*)::int n FROM pg_trigger WHERE tgname='wager_reversal_policy' AND NOT tgisinternal"))[0]?.['n'];
  expect(await indexDef()).toContain('(reference_transaction_id, kind) WHERE');
  expect(await guard()).toBe(1);
  await orm.migrator.down({ migrations: ['Migration202610140010'] });
  expect(await indexDef()).toContain('(reference_transaction_id) WHERE');
  expect(await guard()).toBe(0);
  expect((await q("SELECT count(*)::int n FROM pg_proc WHERE proname='jwp_reversal_policy_guard'"))[0]?.['n']).toBe(0);
  await orm.migrator.up();
  expect(await indexDef()).toContain('(reference_transaction_id, kind) WHERE');
  expect(await guard()).toBe(1);

  // Política padrão per_type: REFUND e ROLLBACK da mesma BET aplicados pelo papel de runtime.
  const wallet = await new OpenWallet(new SqlWalletOpeningStore(runtimeOrm!)).execute({ playerId: randomUUID(),
    initialBalance: { amount: '100.00', currency: 'BRL' } });
  const processor = new ProcessWager(new SqlProcessWagerStore(runtimeOrm!));
  const send = (kind: string, reference?: string) => { const id = randomUUID(); return processor.execute({ providerId: 'provider-m', externalTransactionId: id,
    walletId: wallet.id, playerId: wallet.playerId, roundId: 'r', gameId: 'g', kind, money: { amount: '10.00', currency: 'BRL' },
    ...(reference ? { referenceExternalTransactionId: reference } : {}) }, 'key:' + id).then(r => ({ ...r, id })); };
  const bet = await send('BET');
  expect((await send('REFUND', bet.id)).body).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' } });
  expect((await send('ROLLBACK', bet.id)).body).toMatchObject({ status: 'PROCESSED', balance: { amount: '110.00' } });
  await expect(orm.migrator.down({ migrations: ['Migration202610140010'] })).rejects.toThrow('REVERSAL_POLICY_DOWN_BLOCKED');
  // Down recusado é transacional: índice, gatilho e dados permanecem.
  expect(await indexDef()).toContain('(reference_transaction_id, kind) WHERE');
  expect(await guard()).toBe(1);
  expect((await q("SELECT balance::text b FROM wallets WHERE id=?", [wallet.id]))[0]?.['b']).toBe('110.00');
});
