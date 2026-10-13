import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { SqlEventDeduplicator, SqsEventConsumer, type EventConsumption } from '../../../src/messaging/outbox/sqs-event-consumer.js';
import { sendWagerTransactionRequested } from '../../../src/messaging/wager-message.js';
import { isolatedPair, queueAttributes, requireSqs, sqsClient, type QueuePair } from '../../integration/sqs/support/sqs.js';
import {
  Child, call, envelope, openWallet, reconcile, startApi, submit, wager, waitUntil, walletState,
  type ApiInstance, type Orm, type Wager,
} from '../../concurrency/multi-process/support.js';


function requiredInfrastructure(): string {
  const ownerUrl = process.env['JWP_RECOVERY_DATABASE_URL'];
  const container = process.env['JWP_RECOVERY_POSTGRES_CONTAINER'];
  if (!ownerUrl || !container?.startsWith('jwp-recovery-')) {
    throw new Error('Dedicated JWP_RECOVERY_DATABASE_URL and JWP_RECOVERY_POSTGRES_CONTAINER (jwp-recovery-*) required; recovery never skips infrastructure.');
  }
  return ownerUrl;
}

const role = `jwp_recovery_${randomUUID().replaceAll('-', '')}`;
const OUTBOX_LEASE_MS = 3_000;
let orm: Orm, client: SQSClient, commands: QueuePair, events: QueuePair, runtimeUrl: string, migrated = false;
const children: Child[] = [];
const apis: ApiInstance[] = [];
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<Record<string, any>[]>(sql, args);
const pgArray = (values: readonly string[]) => `{${values.join(',')}}`;
const proof = (name: string, data: Record<string, unknown>) => console.log(JSON.stringify({ proof: name, ...data }));

function spawnWorker(name: string, script: 'wager-worker.ts' | 'publisher-worker.ts', env: Record<string, string> = {}): Child {
  const queueUrl = script === 'wager-worker.ts' ? commands.url : events.url;
  const child = Child.spawn(name, ['bun', `tests/recovery/crash/${script}`], {
    JWP_WORKER_DATABASE_URL: runtimeUrl, JWP_WORKER_QUEUE_URL: queueUrl, JWP_OUTBOX_LEASE_MS: String(OUTBOX_LEASE_MS), ...env,
  });
  children.push(child);
  return child;
}
const ready = (child: Child) => child.waitFor(line => line['type'] === 'ready', 'ready');
async function startApiProcess(name: string): Promise<ApiInstance> {
  const api = await startApi(name, runtimeUrl);
  children.push(api.child);
  apis.push(api);
  return api;
}

/** SIGKILL real não executa handler, não devolve visibilidade e não libera lease. */
async function sigkill(child: Child): Promise<{ name: string; pid: number; signal: string | null }> {
  child.proc.kill('SIGKILL');
  await child.proc.exited;
  expect(child.proc.signalCode).toBe('SIGKILL');
  return { name: child.name, pid: child.pid, signal: child.proc.signalCode };
}

const sendCommands = (messages: ReturnType<typeof envelope>[]) =>
  Promise.all(messages.map(message => sendWagerTransactionRequested(client, commands.url, message)));
async function acknowledged(workers: readonly Child[], messageIds: readonly string[], timeoutMs = 75_000) {
  const wanted = new Set(messageIds);
  return waitUntil(async () => {
    const acks = new Map<string, { pid: number; receiveCount: number }>();
    for (const line of workers.flatMap(w => w.of('message'))) {
      if (line['disposition'] === 'ACKNOWLEDGED' && wanted.has(line['messageId'] as string)) {
        acks.set(line['messageId'] as string, { pid: line['pid'] as number, receiveCount: line['receiveCount'] as number });
      }
    }
    return acks.size === wanted.size ? acks : undefined;
  }, `${wanted.size} SQS acknowledgements`, timeoutMs);
}
async function queueDepth(url: string) {
  const attributes = await queueAttributes(client, url);
  return { visible: Number(attributes['ApproximateNumberOfMessages']), inFlight: Number(attributes['ApproximateNumberOfMessagesNotVisible']) };
}
/** SIGKILL pode ocorrer após DeleteMessage e antes do log de ack. Inbox confirmada e fila/DLQ vazias em três leituras consecutivas provam conclusão. */
async function settledCommands(messageIds: readonly string[], timeoutMs = 120_000) {
  const ids = [...new Set(messageIds)];
  let emptyObservations = 0;
  return waitUntil(async () => {
    const [inbox] = await q(`SELECT count(*)::int total,
      count(*) FILTER (WHERE processed_at IS NOT NULL)::int processed
      FROM wager_message_inbox WHERE message_id = ANY(?::text[])`, [pgArray(ids)]);
    const [queue, dlq] = await Promise.all([queueDepth(commands.url), queueDepth(commands.dlqUrl)]);
    const complete = inbox!['total'] === ids.length && inbox!['processed'] === ids.length;
    const empty = [queue, dlq].every(depth => depth.visible === 0 && depth.inFlight === 0);
    emptyObservations = complete && empty ? emptyObservations + 1 : 0;
    return emptyObservations >= 3 ? { inbox, queue, dlq, emptyObservations, timeoutMs } : undefined;
  }, `${ids.length} committed inbox results and empty command queue/DLQ`, timeoutMs);
}
const unpublished = async () => (await q('SELECT count(*)::int n FROM outbox_events WHERE published_at IS NULL'))[0]!['n'] as number;
const outboxRows = (eventIds: readonly string[]) => q(`SELECT event_id::text, attempts, published_at, claim_token
  FROM outbox_events WHERE event_id = ANY(?::uuid[]) ORDER BY event_id`, [pgArray(eventIds)]);
async function barrierEvents(publisher: Child, stage: string): Promise<string[]> {
  await publisher.waitFor(line => line['type'] === 'barrier' && line['stage'] === stage, `${stage} barrier`);
  await Bun.sleep(300);
  return publisher.of('barrier').map(line => line['eventId'] as string);
}

beforeAll(async () => {
  const ownerUrl = requiredInfrastructure();
  client = sqsClient();
  await requireSqs(client);
  orm = await openFinancialOrm(ownerUrl);
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]!['relation']).toBeNull();
  const migration = Bun.spawn(['bun', 'run', 'db:migrate'], { env: { ...process.env, JWP_MIGRATION_DATABASE_URL: ownerUrl }, stdout: 'pipe', stderr: 'pipe' });
  expect(await migration.exited).toBe(0);
  migrated = true;
  await q(`CREATE ROLE ${role} LOGIN PASSWORD 'recovery_test_only'`);
  await q(`GRANT jwp_runtime TO ${role}`);
  const url = new URL(ownerUrl);
  url.username = role;
  url.password = 'recovery_test_only';
  runtimeUrl = url.toString();

  commands = await isolatedPair(client, 'wager-transactions.fifo');
  events = await isolatedPair(client, 'wager-events.fifo');
  await startApiProcess('api-1');
}, 90_000);

afterAll(async () => {
  await Promise.all(children.map(child => child.stop()));
  await Promise.all([commands, events].filter(Boolean).map(pair => pair.dispose()));
  client?.destroy();
  if (!orm) return;
  try {
    if (migrated) await orm.migrator.down({ to: 0 });
    await q(`DROP ROLE IF EXISTS ${role}`);
  } finally {
    await orm.close(true);
  }
}, 90_000);

test('SIGKILL after the financial commit and before the SQS ack: redelivery is absorbed by the inbox, one debit', async () => {
  const wallets = await Promise.all(Array.from({ length: 4 }, () => openWallet(apis[0]!)));
  const bets = wallets.map(w => wager(w, 'BET', '25.00'));
  const [first, ...rest] = bets.map(bet => envelope(bet));

  const crashing = spawnWorker('wager-worker-barrier', 'wager-worker.ts', { JWP_BARRIER: 'after-commit' });
  await ready(crashing);
  await sendCommands([first!]);
  const parked = await crashing.waitFor(line => line['type'] === 'barrier', 'after-commit barrier');
  expect(parked['messageId']).toBe(first!.messageId);

  // Commit já confirmou ledger, inbox e outbox; o receipt segue retido, sem ack.
  const [inboxBefore] = await q(`SELECT processed_at IS NOT NULL processed, result_snapshot->'body'->>'transactionId' transaction_id
    FROM wager_message_inbox WHERE message_id=?`, [first!.messageId]);
  expect(inboxBefore).toMatchObject({ processed: true });
  expect(await walletState(orm, wallets[0]!.id)).toMatchObject({ balance: '75.00', version: '2', ledger: 2, debits: 1 });
  const outboxForBet = (await q('SELECT count(*)::int n FROM outbox_events WHERE transaction_id=?', [inboxBefore!['transaction_id']]))[0]!['n'];
  expect(outboxForBet).toBeGreaterThan(0);
  expect(await queueDepth(commands.url)).toEqual({ visible: 0, inFlight: 1 });
  const killed = await sigkill(crashing);
  const killedAt = Date.now();
  expect(crashing.of('message')).toEqual([]);


  const sameKey = envelope(bets[0]!);
  await sendCommands([...rest, sameKey]);
  const restarted = [spawnWorker('wager-worker-restart-1', 'wager-worker.ts'), spawnWorker('wager-worker-restart-2', 'wager-worker.ts')];
  await Promise.all(restarted.map(ready));
  const acks = await acknowledged(restarted, [first!.messageId, sameKey.messageId, ...rest.map(m => m.messageId)]);
  const redelivery = acks.get(first!.messageId)!;
  expect(redelivery.receiveCount).toBeGreaterThanOrEqual(2);
  expect(restarted.map(w => w.pid)).toContain(redelivery.pid);

  const inbox = await q(`SELECT message_id, processed_at IS NOT NULL processed,
      result_snapshot->'body'->>'transactionId' transaction_id, (result_snapshot->'body'->>'idempotentReplay')::boolean replay
    FROM wager_message_inbox WHERE message_id = ANY(?::text[])`, [pgArray([first!.messageId, sameKey.messageId])]);
  expect(inbox).toHaveLength(2);
  expect(inbox.every(row => row['processed'] && row['transaction_id'] === inboxBefore!['transaction_id'])).toBe(true);
  expect(inbox.find(row => row['message_id'] === sameKey.messageId)!['replay']).toBe(true);
  expect(inbox.find(row => row['message_id'] === first!.messageId)!['replay']).toBe(false);
  const [txCount] = await q(`SELECT count(*)::int n FROM wager_transactions WHERE external_transaction_id=?`, [bets[0]!.body.externalTransactionId]);
  expect(txCount!['n']).toBe(1);
  for (const w of wallets) expect(await walletState(orm, w.id)).toMatchObject({ balance: '75.00', version: '2', ledger: 2, debits: 1 });
  expect(await queueDepth(commands.dlqUrl)).toEqual({ visible: 0, inFlight: 0 });
  const reconciliation = await reconcile(orm, apis, wallets.map(w => w.id));
  await Promise.all(restarted.map(w => w.stop()));
  proof('sigkill-after-commit-before-ack', {
    killed, transactionId: inboxBefore!['transaction_id'], redelivery: { ...redelivery, afterKillMs: Date.now() - killedAt },
    sameKeyNewMessageReplay: true, debitsOnWallet: 1, reconciliation,
  });
}, 150_000);

test('publisher SIGKILLed before SendMessage: another instance publishes after the lease, nothing was sent twice', async () => {
  const pending = await unpublished();
  expect(pending).toBeGreaterThan(3);
  const crashing = spawnWorker('publisher-before-send', 'publisher-worker.ts', { JWP_BARRIER: 'before-send', JWP_OUTBOX_BATCH: '3' });
  await ready(crashing);
  const parked = await barrierEvents(crashing, 'before-send');
  expect(parked).toHaveLength(3);
  const killed = await sigkill(crashing);

  const claimed = await outboxRows(parked);
  expect(claimed.map(r => ({ attempts: r['attempts'], published: r['published_at'] !== null, leased: r['claim_token'] !== null })))
    .toEqual(Array(3).fill({ attempts: 1, published: false, leased: true }));
  expect(await queueDepth(events.url)).toEqual({ visible: 0, inFlight: 0 });

  const other = spawnWorker('publisher-other-instance', 'publisher-worker.ts');
  await ready(other);
  await waitUntil(async () => await unpublished() === 0 || undefined, 'outbox drained by the other instance', 30_000);
  const after = await outboxRows(parked);
  expect(after.map(r => ({ attempts: r['attempts'], published: r['published_at'] !== null, leased: r['claim_token'] !== null })))
    .toEqual(Array(3).fill({ attempts: 2, published: true, leased: false }));
  const published = other.of('publication').filter(line => line['status'] === 'PUBLISHED').map(line => line['eventId'] as string);
  expect(new Set(published).size).toBe(published.length);
  expect(published).toEqual(expect.arrayContaining(parked));
  const total = (await q('SELECT count(*)::int n FROM outbox_events'))[0]!['n'] as number;
  await waitUntil(async () => (await queueDepth(events.url)).visible === total || undefined, 'events visible on wager-events', 10_000);
  expect(await queueDepth(events.url)).toEqual({ visible: total, inFlight: 0 });
  await other.stop();
  proof('publisher-crash-before-send', { killed, parkedEvents: parked.length, outboxEvents: total, queueVisible: total, reclaimedBy: other.pid });
}, 90_000);

test('publisher SIGKILLed after SendMessage and before markPublished: restart re-sends, SQS FIFO dedup delivers once', async () => {
  const wallets = await Promise.all(Array.from({ length: 3 }, () => openWallet(apis[0]!)));
  const results = await Promise.all(wallets.map(w => submit(apis[0]!, wager(w, 'BET', '10.00'))));
  expect(results.map(r => r.status)).toEqual([200, 200, 200]);
  const before = await queueDepth(events.url);
  const crashing = spawnWorker('publisher-after-send', 'publisher-worker.ts', { JWP_BARRIER: 'after-send', JWP_OUTBOX_BATCH: '3' });
  await ready(crashing);
  const parked = await barrierEvents(crashing, 'after-send');
  expect(parked).toHaveLength(3);
  const killed = await sigkill(crashing);

  // SQS já recebeu os eventos, mas a outbox permanece pendente sob lease do processo encerrado.
  const claimed = await outboxRows(parked);
  expect(claimed.map(r => ({ attempts: r['attempts'], published: r['published_at'] !== null, leased: r['claim_token'] !== null })))
    .toEqual(Array(3).fill({ attempts: 1, published: false, leased: true }));
  expect(await queueDepth(events.url)).toEqual({ visible: before.visible + 3, inFlight: 0 });

  const restarted = spawnWorker('publisher-restart', 'publisher-worker.ts');
  await ready(restarted);
  await waitUntil(async () => await unpublished() === 0 || undefined, 'outbox drained after restart', 30_000);
  const after = await outboxRows(parked);
  expect(after.map(r => ({ attempts: r['attempts'], published: r['published_at'] !== null }))).toEqual(Array(3).fill({ attempts: 2, published: true }));
  expect(restarted.of('publication').filter(l => l['status'] === 'PUBLISHED').map(l => l['eventId'])).toEqual(expect.arrayContaining(parked));
  const total = (await q('SELECT count(*)::int n FROM outbox_events'))[0]!['n'] as number;
  await Bun.sleep(500);
  // Reenvio preserva eventId; MessageDeduplicationId impede nova entrega dentro da janela FIFO.
  expect(await queueDepth(events.url)).toEqual({ visible: total, inFlight: 0 });
  await restarted.stop();
  proof('publisher-crash-after-send', { killed, parkedEvents: parked.length, resentAttempts: 2, outboxEvents: total, queueVisible: total });
}, 90_000);

test('SIGKILL of every process mid-traffic, then restart: each operation applied once and balance equals ledger', async () => {
  await startApiProcess('api-2');
  const generation1 = {
    workers: [spawnWorker('wager-worker-g1-1', 'wager-worker.ts'), spawnWorker('wager-worker-g1-2', 'wager-worker.ts')],
    publisher: spawnWorker('publisher-g1', 'publisher-worker.ts'),
  };
  await Promise.all([...generation1.workers, generation1.publisher].map(ready));
  const wallets = await Promise.all(Array.from({ length: 10 }, (_, i) => openWallet(apis[i % 2]!)));
  const operations: Wager[] = wallets.flatMap(w => [
    ...Array.from({ length: 4 }, () => wager(w, 'BET', '10.00')),
    ...Array.from({ length: 2 }, () => wager(w, 'WIN', '5.00')),
  ]);
  const viaSqs = operations.filter((_, i) => i % 2 === 1).map(op => envelope(op));
  const viaHttp = operations.filter((_, i) => i % 2 === 0);
  await sendCommands(viaSqs);

  const firstApis = [...apis];
  const inflight = viaHttp.map((op, i) => submit(firstApis[i % 2]!, op).then(r => r.status, () => 'connection-lost' as const));
  const externalIds = pgArray(operations.map(op => op.body.externalTransactionId));
  const applied = async () => (await q('SELECT count(*)::int n FROM wager_transactions WHERE external_transaction_id = ANY(?::text[])',
    [externalIds]))[0]!['n'] as number;
  await waitUntil(async () => await applied() >= 6 || undefined, 'traffic in progress', 20_000);
  const killed = await Promise.all([...firstApis.map(api => api.child), ...generation1.workers, generation1.publisher].map(sigkill));
  apis.length = 0;
  const firstOutcomes = countBy(await Promise.all(inflight));
  const atKill = { applied: await applied(), unpublished: await unpublished() };
  expect(atKill.applied).toBeLessThan(operations.length);


  await Promise.all([startApiProcess('api-3'), startApiProcess('api-4')]);
  const generation2 = {
    workers: [spawnWorker('wager-worker-g2-1', 'wager-worker.ts'), spawnWorker('wager-worker-g2-2', 'wager-worker.ts')],
    publisher: spawnWorker('publisher-g2', 'publisher-worker.ts'),
  };
  await Promise.all([...generation2.workers, generation2.publisher].map(ready));
  const retried = await Promise.all(viaHttp.map((op, i) => submit(apis[i % 2]!, op)));
  expect(retried.map(r => r.status)).toEqual(Array(viaHttp.length).fill(200));
  const settlement = await settledCommands(viaSqs.map(m => m.messageId));
  expect(settlement.inbox).toEqual({ total: viaSqs.length, processed: viaSqs.length });
  expect(settlement.queue).toEqual({ visible: 0, inFlight: 0 });
  expect(settlement.dlq).toEqual({ visible: 0, inFlight: 0 });
  await waitUntil(async () => await unpublished() === 0 || undefined, 'outbox drained after restart', 45_000);

  for (const w of wallets) expect(await walletState(orm, w.id)).toMatchObject({ balance: '70.00', version: '7', ledger: 7, debits: 4 });
  const [perOperation] = await q(`SELECT count(*)::int transactions, count(DISTINCT external_transaction_id)::int distinct_external
    FROM wager_transactions WHERE external_transaction_id = ANY(?::text[])`, [externalIds]);
  expect(perOperation).toEqual({ transactions: operations.length, distinct_external: operations.length });
  const replayedAfterRestart = retried.filter(r => r.body.idempotentReplay).length;
  const allWallets = (await q('SELECT id FROM wallets')).map(r => r['id'] as string);
  const reconciliation = await reconcile(orm, apis, allWallets);
  const [totals] = await q(`SELECT
      (SELECT count(*)::int FROM wallets WHERE balance < 0) negative,
      (SELECT count(*)::int FROM (SELECT transaction_id FROM ledger_entries GROUP BY transaction_id HAVING count(*) > 1) d) duplicated_entries,
      (SELECT count(*)::int FROM wager_message_inbox WHERE processed_at IS NULL) unfinished_inbox,
      (SELECT count(*)::int FROM wager_transactions WHERE status NOT IN ('PROCESSED','REJECTED')) non_terminal`);
  expect(totals).toEqual({ negative: 0, duplicated_entries: 0, unfinished_inbox: 0, non_terminal: 0 });
  await Promise.all([...generation2.workers, generation2.publisher].map(w => w.stop()));
  proof('sigkill-everything-and-restart', {
    killed, httpBeforeKill: Object.fromEntries(firstOutcomes), atKill, operations: operations.length,
    httpReplayedAfterRestart: replayedAfterRestart, sqsRedeliveredAfterRestart: generation2.workers.flatMap(w => w.of('message'))
      .filter(l => l['disposition'] === 'ACKNOWLEDGED' && (l['receiveCount'] as number) >= 2).length,
    walletsReconciled: allWallets.length, reconciliation, totals, settlement,
  });
}, 180_000);

test('after every crash, each outbox event reaches wager-events exactly once', async () => {
  const total = (await q('SELECT count(*)::int n FROM outbox_events'))[0]!['n'] as number;
  expect(await unpublished()).toBe(0);
  const consumer = new SqsEventConsumer(client, events.url, new SqlEventDeduplicator(orm), { waitTimeSeconds: 1 });
  const consumed: EventConsumption[] = [];
  await waitUntil(async () => {
    consumed.push(...await consumer.pollOnce());
    return new Set(consumed.filter(c => c.disposition === 'ACKNOWLEDGED').map(c => c.eventId)).size === total ? true : undefined;
  }, `${total} events on wager-events`, 60_000);
  // Outra leitura vazia verifica que não restou duplicata tardia na fila.
  consumed.push(...await consumer.pollOnce());
  expect(consumed.filter(c => c.disposition !== 'ACKNOWLEDGED')).toEqual([]);
  expect(consumed).toHaveLength(total);
  expect(consumed.filter(c => c.disposition === 'ACKNOWLEDGED' && c.duplicate)).toEqual([]);
  const [inbox] = await q(`SELECT count(*)::int n,
      count(*) FILTER (WHERE event_id NOT IN (SELECT event_id FROM outbox_events))::int unknown FROM wager_event_inbox`);
  expect(inbox).toEqual({ n: total, unknown: 0 });
  expect(await queueDepth(events.url)).toEqual({ visible: 0, inFlight: 0 });
  expect(await queueDepth(events.dlqUrl)).toEqual({ visible: 0, inFlight: 0 });
  expect(await queueDepth(commands.dlqUrl)).toEqual({ visible: 0, inFlight: 0 });
  proof('events-delivered-once', { outboxEvents: total, delivered: consumed.length, duplicates: 0 });
}, 90_000);

const countBy = <T>(values: readonly T[]) => values.reduce((map, value) => map.set(value, (map.get(value) ?? 0) + 1), new Map<T, number>());
