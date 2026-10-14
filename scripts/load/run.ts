#!/usr/bin/env bun
/** Separar HTTP e workers evita disputar pool e CPU do mesmo processo. A carga usa PostgreSQL/SQS reais; desempenho não reprova, mas violar invariantes financeiras reprova. */
import { SQL } from 'bun';
import { DeleteMessageBatchCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageBatchCommand, SQSClient } from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { cpus, loadavg, release, tmpdir, totalmem } from 'node:os';
import { join, resolve } from 'node:path';

const ROOT = resolve(import.meta.dir, '../..');
const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const int = (name: string, fallback: number): number => {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`INVALID_${name}`);
  return value;
};
const cfg = {
  replicas: int('LOAD_REPLICAS', 3),
  workers: int('LOAD_WORKERS', int('LOAD_REPLICAS', 3) || 1),
  concurrency: int('LOAD_CONCURRENCY', 32),
  durationS: int('LOAD_DURATION_S', 20),
  warmupS: int('LOAD_WARMUP_S', 5),
  wallets: int('LOAD_WALLETS', 200),
  hotDrainBets: int('LOAD_HOT_DRAIN_BETS', 500),
  hotDrainBalance: '100.00',
  sqsMessages: int('LOAD_SQS_MESSAGES', 1000),
  sqsWallets: int('LOAD_SQS_WALLETS', 100),
  drainTimeoutS: int('LOAD_DRAIN_TIMEOUT_S', 900),
  pgPort: int('LOAD_PG_PORT', 56432),
  sqsPort: int('LOAD_SQS_PORT', 56566),
  project: process.env['LOAD_COMPOSE_PROJECT'] ?? 'jwp-load',
  keepInfra: process.env['LOAD_KEEP_INFRA'] === '1',
  outDir: resolve(process.env['LOAD_OUT_DIR'] ?? join(tmpdir(), `jwp-load-${stamp}`)),
};
const PROVIDER = 'load-provider';
const OWNER_URL = `postgresql://jwp:jwp_local@127.0.0.1:${cfg.pgPort}/jwp`;
const RUNTIME_URL = `postgresql://jwp_load_api:jwp_load_local@127.0.0.1:${cfg.pgPort}/jwp`;
const SQS_ENDPOINT = `http://127.0.0.1:${cfg.sqsPort}`;
const queueUrl = (name: string) => `${SQS_ENDPOINT}/queue/us-east-1/000000000000/${name}`;
const QUEUES = {
  commands: queueUrl('wager-transactions.fifo'),
  events: queueUrl('wager-events.fifo'),
  commandsDlq: queueUrl('wager-transactions-dlq.fifo'),
  eventsDlq: queueUrl('wager-events-dlq.fifo'),
};

const say = (message: string) => process.stderr.write(`[load] ${message}\n`);

async function run(cmd: string[], env: Record<string, string> = {}, allowFail = false): Promise<string> {
  const proc = Bun.spawn(cmd, { cwd: ROOT, env: { ...process.env, ...env }, stdout: 'pipe', stderr: 'pipe' });
  const [out, err, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  if (code !== 0 && !allowFail) throw new Error(`${cmd.join(' ')} exited ${code}\n${err || out}`);
  return out.trim();
}
const compose = (...args: string[]) => run(['docker', 'compose', '-p', cfg.project, '-f', 'compose.db.yml', '-f', 'compose.sqs.yml', ...args],
  { JWP_PG_PORT: String(cfg.pgPort), JWP_SQS_PORT: String(cfg.sqsPort) });

async function mapLimit<T, R>(items: readonly T[], limit: number, work: (item: T, index: number) => Promise<R>): Promise<R[]> {
  const results = new Array<R>(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const index = next++;
      results[index] = await work(items[index]!, index);
    }
  }));
  return results;
}

/** Formata centavos inteiros (bigint) em decimal com 2 casas; valor monetário nunca passa por number. */
const cents = (value: bigint): string => {
  if (value < 0n) throw new RangeError('NEGATIVE_AMOUNT');
  return `${value / 100n}.${(value % 100n).toString().padStart(2, '0')}`;
};
/** Sorteio entre 1.00 e 9.99: o índice aleatório é inteiro e vira bigint antes de qualquer conta monetária. */
/** Valor fixo de cada aposta do cenário hot-drain. */
const HOT_DRAIN_BET = '1.00';
const randomAmount = () => cents(100n + BigInt(Math.floor(Math.random() * 900)));
/** Converte "100.00" em centavos bigint sem ponto flutuante; recusa formato fora de dígitos com 2 casas. */
const parseCents = (value: string): bigint => {
  const match = /^(\d+)\.(\d{2})$/.exec(value);
  if (!match) throw new RangeError(`INVALID_AMOUNT:${value}`);
  return BigInt(match[1]!) * 100n + BigInt(match[2]!);
};
const randomKind = () => (Math.random() < 0.7 ? 'BET' : 'WIN');



interface Replica { proc: Bun.Subprocess; base: string; logPath: string }

async function startReplica(index: number, logDir: string, roles = 'http,workers'): Promise<Replica> {
  const http = roles.split(',').includes('http');
  const logPath = join(logDir, `${http ? 'replica' : 'replica-worker'}-${index}.log`);
  const proc = Bun.spawn(['bun', 'dist/main.js'], {
    cwd: ROOT,
    env: {
      ...process.env,
      APP_ROLES: roles, PORT: '0', HOST: '127.0.0.1',
      JWP_DATABASE_URL: RUNTIME_URL,
      AWS_REGION: 'us-east-1', AWS_ACCESS_KEY_ID: 'test', AWS_SECRET_ACCESS_KEY: 'test',
      JWP_SQS_ENDPOINT: SQS_ENDPOINT,
      JWP_SQS_COMMAND_QUEUE_URL: QUEUES.commands,
      JWP_SQS_EVENT_QUEUE_URL: QUEUES.events,
      JWP_SQS_QUEUE_URLS: `${QUEUES.commandsDlq},${QUEUES.eventsDlq}`,
    },
    stdout: Bun.file(logPath),
    stderr: Bun.file(`${logPath}.stderr`),
  });
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null) throw new Error(`replica ${index} exited ${proc.exitCode}: ${readFileSync(`${logPath}.stderr`, 'utf8')}`);
    // Workers não expõem HTTP; falha na composição ORM/SQS encerra main.ts,
    // por isso um processo vivo após iniciar confirma a composição.
    if (!http) { await Bun.sleep(2000); if (proc.exitCode === null) return { proc, base: '', logPath }; continue; }
    const port = /"type":"listening","port":(\d+)/.exec(await Bun.file(logPath).text().catch(() => ''))?.[1];
    if (port) {
      const base = `http://127.0.0.1:${port}`;
      const ready = await fetch(`${base}/health/ready`).then(r => r.status, () => 0);
      if (ready === 200) return { proc, base, logPath };
    }
    await Bun.sleep(200);
  }
  proc.kill('SIGKILL');
  throw new Error(`replica ${index} not ready in 30s`);
}

async function stopReplica(replica: Replica): Promise<number | string> {
  replica.proc.kill('SIGTERM');
  const code = await Promise.race([replica.proc.exited, Bun.sleep(35000).then(() => 'timeout' as const)]);
  if (code === 'timeout') replica.proc.kill('SIGKILL');
  return code;
}



interface Wallet { id: string; playerId: string }
interface Sample { t: number; ms: number; http: number; outcome: string; kind: string; externalTransactionId: string; roundId: string }
interface Reference { wallet: Wallet; externalTransactionId: string; roundId: string; amount: string }

async function openWallet(base: string, amount: string): Promise<Wallet> {
  const playerId = randomUUID();
  const response = await fetch(`${base}/wallets`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ playerId, initialBalance: { amount, currency: 'BRL' } }),
  });
  if (response.status !== 201) throw new Error(`POST /wallets -> ${response.status} ${await response.text()}`);
  return { id: ((await response.json()) as { id: string }).id, playerId };
}

async function wager(base: string, prefix: string, wallet: Wallet, kind: string, amount: string, reference?: Reference): Promise<Omit<Sample, 't'>> {
  const external = `${prefix}-${randomUUID()}`;
  const roundId = reference?.roundId ?? `round-${external}`;
  const identity = { kind, externalTransactionId: external, roundId };
  const body = {
    providerId: PROVIDER, externalTransactionId: external, playerId: wallet.playerId, walletId: wallet.id,
    roundId, gameId: 'load-game', kind, money: { amount, currency: 'BRL' },
    ...(reference ? { referenceExternalTransactionId: reference.externalTransactionId } : {}),
  };
  const started = performance.now();
  try {
    const response = await fetch(`${base}/wagering/transactions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': `${PROVIDER}:${external}` },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(30000),
    });
    const text = await response.text();
    const ms = performance.now() - started;
    let outcome: string;
    try {
      const json = JSON.parse(text) as { status?: string; failureCode?: string; error?: { code?: string } };
      outcome = json.status ? (json.failureCode ? `${json.status}:${json.failureCode}` : json.status) : `ERROR:${json.error?.code ?? 'UNKNOWN'}`;
    } catch { outcome = 'ERROR:NON_JSON'; }
    return { ...identity, ms, http: response.status, outcome };
  } catch (error) {
    return { ...identity, ms: performance.now() - started, http: 0, outcome: `NETWORK:${(error as Error).name}` };
  }
}

type Selection = { wallet: Wallet; kind: string; amount: string; reference?: Reference;
  after?: (sample: Omit<Sample, 't'>) => void };
type Pick = (lane: number) => Selection;

/** Cada VU reverte BET confirmada própria; resposta incerta não fornece referência. LOSS usa zero sem referência. */
function mixedPick(chooseWallet: () => Wallet): Pick {
  const lanes = new Map<number, { step: number; first?: Reference; second?: Reference }>();
  return lane => {
    const state = lanes.get(lane) ?? { step: 0 };
    lanes.set(lane, state);
    const step = state.step++ % 6;
    const reference = step === 3 ? state.first : step === 5 ? state.second : undefined;
    if (step === 3) state.first = undefined;
    if (step === 5) state.second = undefined;
    const kind = step === 1 ? 'WIN' : step === 2 ? 'LOSS'
      : reference ? (step === 3 ? 'REFUND' : 'ROLLBACK') : 'BET';
    const wallet = reference?.wallet ?? chooseWallet();
    const amount = kind === 'LOSS' ? '0.00' : reference?.amount ?? randomAmount();
    return { wallet, kind, amount, reference, after: sample => {
      if (sample.http === 200 && sample.outcome === 'PROCESSED' && (step === 0 || step === 4)) {
        const confirmed = { wallet, amount, externalTransactionId: sample.externalTransactionId, roundId: sample.roundId };
        if (step === 0) state.first = confirmed; else state.second = confirmed;
      }
    } };
  };
}

/** Laço fechado só envia a próxima requisição após a resposta, limitando a interpretação da vazão. */
async function closedLoop(bases: string[], prefix: string, concurrency: number, pick: Pick,
  stop: { durationMs: number } | { total: number }): Promise<{ samples: Sample[]; elapsedMs: number }> {
  const samples: Sample[] = [];
  const t0 = performance.now();
  let issued = 0;
  const more = () => ('durationMs' in stop ? performance.now() - t0 < stop.durationMs : issued < stop.total);
  await Promise.all(Array.from({ length: concurrency }, async (_, lane) => {
    while (more()) {
      const base = bases[issued++ % bases.length]!;
      const { wallet, kind, amount, reference, after } = pick(lane);
      const sample = await wager(base, prefix, wallet, kind, amount, reference);
      after?.(sample);
      samples.push({ ...sample, t: performance.now() - t0 });
    }
  }));
  return { samples, elapsedMs: performance.now() - t0 };
}

const percentile = (sorted: readonly number[], p: number): number | null =>
  sorted.length ? sorted[Math.max(0, Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1))]! : null;
const round = (value: number | null, digits = 1) => (value === null ? null : Number(value.toFixed(digits)));
const countBy = <T>(items: readonly T[], key: (item: T) => string) =>
  items.reduce<Record<string, number>>((acc, item) => { acc[key(item)] = (acc[key(item)] ?? 0) + 1; return acc; }, {});
const latency = (values: number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  const mean = sorted.length ? sorted.reduce((a, b) => a + b, 0) / sorted.length : null;
  return { p50: round(percentile(sorted, 50)), p95: round(percentile(sorted, 95)), p99: round(percentile(sorted, 99)),
    max: round(sorted.at(-1) ?? null), mean: round(mean) };
};
/** 200/202/422 são respostas do contrato; transporte, 5xx e outros 4xx contam como erro. */
const isError = (sample: Sample) => ![200, 202, 422].includes(sample.http);

function summarizeHttp(samples: Sample[], elapsedMs: number) {
  const errors = samples.filter(isError).length;
  const perSecond: number[] = [];
  for (const sample of samples) {
    const bucket = Math.floor(sample.t / 1000);
    perSecond[bucket] = (perSecond[bucket] ?? 0) + 1;
  }
  return {
    requests: samples.length,
    elapsedS: round(elapsedMs / 1000, 2),
    throughputRps: round(samples.length / (elapsedMs / 1000)),
    latencyMs: latency(samples.map(s => s.ms)),
    latencyMsHttp200: latency(samples.filter(s => s.http === 200).map(s => s.ms)),
    errors,
    errorRatePct: round(samples.length ? (100 * errors) / samples.length : 0, 2),
    httpStatus: countBy(samples, s => String(s.http)),
    kinds: countBy(samples, s => s.kind),
    kindOutcomes: countBy(samples, s => `${s.kind}:${s.outcome}`),
    outcomes: countBy(samples, s => s.outcome),
    completedPerSecond: Array.from(perSecond, v => v ?? 0),
  };
}



const CLK_TCK = Number(Bun.spawnSync(['getconf', 'CLK_TCK']).stdout.toString().trim()) || 100;

function processCpuS(pid: number): number | null {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, 'utf8');
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    return (Number(fields[11]) + Number(fields[12])) / CLK_TCK;
  } catch { return null; }
}

async function containerCpuReader(container: string): Promise<() => number | null> {
  const pid = Number(await run(['docker', 'inspect', '-f', '{{.State.Pid}}', container], {}, true));
  const path = readFileSync(`/proc/${pid}/cgroup`, 'utf8').split('\n').find(line => line.startsWith('0::'))?.slice(3);
  return () => {
    try {
      const usage = /usage_usec (\d+)/.exec(readFileSync(`/sys/fs/cgroup${path}/cpu.stat`, 'utf8'))?.[1];
      return usage ? Number(usage) / 1e6 : null;
    } catch { return null; }
  };
}
type CpuSnapshot = Record<string, number | null>;
function cpuSnapshot(ctx: Ctx): CpuSnapshot {
  const usage = process.cpuUsage();
  return {
    replicas: ctx.pids.reduce<number | null>((sum, pid) => { const s = processCpuS(pid); return sum === null || s === null ? null : sum + s; }, 0),
    postgres: ctx.cpu.postgres(), localstack: ctx.cpu.localstack(), loadGenerator: (usage.user + usage.system) / 1e6,
  };
}
/** Segundos de CPU por tempo de parede expressam núcleos ocupados, não porcentagem por processo. */
function coresBusy(before: CpuSnapshot, after: CpuSnapshot, wallMs: number) {
  return Object.fromEntries(Object.keys(after).map(key => {
    const a = after[key], b = before[key];
    return [key, a === null || b === null || a === undefined || b === undefined ? null : round((a - b) / (wallMs / 1000), 2)];
  }));
}



type Db = InstanceType<typeof SQL>;

function sampler(db: Db) {
  const series: Record<string, number>[] = [];
  let active = true;
  const loop = (async () => {
    const t0 = Date.now();
    while (active) {
      const [row] = await db.unsafe(`SELECT
        (SELECT count(*)::int FROM outbox_events WHERE published_at IS NULL) AS outbox_pending,
        (SELECT count(*)::int FROM pg_locks WHERE NOT granted) AS lock_waiters,
        (SELECT count(*)::int FROM pg_stat_activity WHERE datname='jwp' AND state='active') AS active_sessions,
        (SELECT count(*)::int FROM pg_stat_activity WHERE datname='jwp' AND wait_event_type='Lock') AS waiting_on_lock`);
      series.push({ t: Date.now() - t0, ...row });
      await Bun.sleep(1000);
    }
  })();
  return {
    async stop() {
      active = false;
      await loop;
      const max = (key: string) => Math.max(0, ...series.map(s => s[key] ?? 0));
      return { samples: series.length, max: { outbox_pending: max('outbox_pending'), lock_waiters: max('lock_waiters'),
        active_sessions: max('active_sessions'), waiting_on_lock: max('waiting_on_lock') }, series };
    },
  };
}

async function scrapeMetrics(bases: string[]): Promise<Map<string, number>> {
  const total = new Map<string, number>();
  for (const base of bases) {
    const text = await (await fetch(`${base}/metrics`)).text();
    for (const line of text.split('\n')) {
      const match = /^([a-z_]+(?:\{[^}]*\})?)\s+(\S+)$/.exec(line);
      if (!match || !Number.isFinite(Number(match[2]))) continue;
      total.set(match[1]!, (total.get(match[1]!) ?? 0) + Number(match[2]));
    }
  }
  return total;
}
const COUNTERS = ['jwp_retries_total', 'jwp_sql_transient_failures_total', 'jwp_lock_conflicts_total', 'jwp_duplicates_total', 'jwp_wager_transactions_total',
  'jwp_outbox_publications_total', 'jwp_sqs_messages_total', 'jwp_http_requests_total'];
function counterDelta(before: Map<string, number>, after: Map<string, number>) {
  const delta: Record<string, number> = {};
  for (const [key, value] of after) {
    if (!COUNTERS.some(name => key === name || key.startsWith(`${name}{`))) continue;
    const diff = value - (before.get(key) ?? 0);
    if (diff !== 0) delta[key] = diff;
  }
  return delta;
}

async function waitOutboxDrained(db: Db): Promise<number | null> {
  const t0 = Date.now();
  while (Date.now() - t0 < cfg.drainTimeoutS * 1000) {
    const [row] = await db.unsafe('SELECT count(*)::int AS n FROM outbox_events WHERE published_at IS NULL');
    if (row.n === 0) return Date.now() - t0;
    await Bun.sleep(250);
  }
  return null;
}

/** created_at nasce na TX financeira; o lag mede até published_at. */
async function outboxLag(db: Db, prefix: string) {
  const [row] = await db.unsafe(`SELECT count(*)::int AS events,
      count(*) FILTER (WHERE o.published_at IS NULL)::int AS unpublished,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM o.published_at - o.created_at)) * 1000 AS p50,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM o.published_at - o.created_at)) * 1000 AS p95,
      percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM o.published_at - o.created_at)) * 1000 AS p99,
      max(extract(epoch FROM o.published_at - o.created_at)) * 1000 AS max,
      extract(epoch FROM max(o.published_at) - min(o.published_at)) AS publish_window_s
    FROM outbox_events o JOIN wager_transactions t ON t.id = o.transaction_id
    WHERE t.external_transaction_id LIKE $1`, [`${prefix}-%`]);
  const n = (value: unknown) => (value === null || value === undefined ? null : round(Number(value)));
  const window = Number(row.publish_window_s);
  return { events: row.events, unpublished: row.unpublished, lagMs: { p50: n(row.p50), p95: n(row.p95), p99: n(row.p99), max: n(row.max) },
    publishedPerS: window > 0 ? round(row.events / window) : null };
}

async function dbOutcomes(db: Db, prefix: string): Promise<Record<string, number>> {
  const rows = await db.unsafe(`SELECT status || coalesce(':' || failure_code, '') AS outcome, count(*)::int AS n
    FROM wager_transactions WHERE external_transaction_id LIKE $1 GROUP BY 1 ORDER BY 1`, [`${prefix}-%`]);
  return Object.fromEntries(rows.map((r: { outcome: string; n: number }) => [r.outcome, r.n]));
}

/** Resposta definitiva deve corresponder à linha durável; sem resposta, o commit permanece incerto. */
function clientMatchesDb(samples: Sample[], db: Record<string, number>) {
  const definitive = countBy(samples.filter(s => !isError(s)), s => s.outcome);
  const uncertain = samples.filter(isError).length;
  const keys = [...new Set([...Object.keys(definitive), ...Object.keys(db)])];
  const mismatches = keys.filter(key => {
    const seen = definitive[key] ?? 0;
    const stored = db[key] ?? 0;
    return stored < seen || stored > seen + uncertain;
  });
  return { definitive, uncertain, db, ok: mismatches.length === 0, mismatches };
}



async function httpScenario(name: string, ctx: Ctx, concurrency: number, pick: Pick, stop: { durationMs: number } | { total: number }) {
  say(`scenario ${name}: concurrency=${concurrency} ${'durationMs' in stop ? `${stop.durationMs / 1000}s` : `${stop.total} requests`}`);
  const before = await scrapeMetrics(ctx.bases);
  const watch = sampler(ctx.db);
  const startedAt = new Date().toISOString();
  const cpuBefore = cpuSnapshot(ctx);
  const { samples, elapsedMs } = await closedLoop(ctx.bases, name, concurrency, pick, stop);
  const cpuDuringLoad = coresBusy(cpuBefore, cpuSnapshot(ctx), elapsedMs);
  const observed = await watch.stop();
  const drainMs = await waitOutboxDrained(ctx.db);
  const after = await scrapeMetrics(ctx.bases);
  const db = await dbOutcomes(ctx.db, name);
  const summary = summarizeHttp(samples, elapsedMs);
  say(`  ${summary.requests} req in ${summary.elapsedS}s -> ${summary.throughputRps} req/s, p50/p95/p99 ${summary.latencyMs.p50}/${summary.latencyMs.p95}/${summary.latencyMs.p99} ms, errors ${summary.errorRatePct}%`);
  return {
    name, startedAt, concurrency, replicas: ctx.bases.length, ...summary,
    db: { outcomes: db, clientVsDb: clientMatchesDb(samples, db) },
    outbox: { drainAfterLoadMs: drainMs, ...(await outboxLag(ctx.db, name)) },
    cpuCoresBusyDuringLoad: cpuDuringLoad,
    counters: counterDelta(before, after),
    dbSampling: { max: observed.max, samples: observed.samples, series: observed.series },
  };
}

async function sqsScenario(ctx: Ctx, wallets: Wallet[]) {
  const name = 'sqs';
  say(`scenario ${name}: ${cfg.sqsMessages} commands over ${wallets.length} wallets (MessageGroupId=walletId)`);
  const before = await scrapeMetrics(ctx.bases);
  const sqs = new SQSClient({ region: 'us-east-1', endpoint: SQS_ENDPOINT, credentials: { accessKeyId: 'test', secretAccessKey: 'test' } });
  const envelopes = Array.from({ length: cfg.sqsMessages }, (_, i) => {
    const wallet = wallets[i % wallets.length]!;
    const external = `${name}-${randomUUID()}`;
    return { messageId: randomUUID(), type: 'WagerTransactionRequested', occurredAt: '', data: {
      providerId: PROVIDER, externalTransactionId: external, idempotencyKey: `${PROVIDER}:${external}`, playerId: wallet.playerId,
      walletId: wallet.id, roundId: `round-${external}`, gameId: 'load-game', kind: randomKind(), money: { amount: randomAmount(), currency: 'BRL' },
    } };
  });
  const batches = Array.from({ length: Math.ceil(envelopes.length / 10) }, (_, i) => envelopes.slice(i * 10, i * 10 + 10));
  const sentAt = new Map<string, number>();
  const watch = sampler(ctx.db);
  const cpuBefore = cpuSnapshot(ctx);
  const t0 = Date.now();
  await mapLimit(batches, 8, async batch => {
    const at = Date.now();
    for (const envelope of batch) { envelope.occurredAt = new Date(at).toISOString(); sentAt.set(envelope.messageId, at); }
    const result = await sqs.send(new SendMessageBatchCommand({ QueueUrl: QUEUES.commands, Entries: batch.map((envelope, i) => ({
      Id: String(i), MessageBody: JSON.stringify(envelope), MessageGroupId: envelope.data.walletId, MessageDeduplicationId: envelope.messageId,
    })) }));
    if (result.Failed?.length) throw new Error(`SendMessageBatch failed: ${JSON.stringify(result.Failed)}`);
  });
  const sendMs = Date.now() - t0;
  let processed = 0;
  while (Date.now() - t0 < cfg.drainTimeoutS * 1000) {
    [{ n: processed }] = await ctx.db.unsafe('SELECT count(*)::int AS n FROM wager_message_inbox WHERE processed_at IS NOT NULL');
    if (processed >= envelopes.length) break;
    await Bun.sleep(250);
  }
  const cpuDuringLoad = coresBusy(cpuBefore, cpuSnapshot(ctx), Date.now() - t0);
  const observed = await watch.stop();
  const rows = await ctx.db.unsafe(`SELECT message_id, (extract(epoch FROM processed_at) * 1000)::float8 AS at
    FROM wager_message_inbox WHERE processed_at IS NOT NULL`);
  const latencies: number[] = [];
  let last = t0;
  for (const row of rows as { message_id: string; at: number }[]) {
    const sent = sentAt.get(row.message_id);
    if (sent === undefined) continue;
    latencies.push(row.at - sent);
    last = Math.max(last, row.at);
  }
  const drainMs = await waitOutboxDrained(ctx.db);
  const after = await scrapeMetrics(ctx.bases);
  sqs.destroy();
  const elapsedS = (last - t0) / 1000;
  const result = {
    name, messages: envelopes.length, wallets: wallets.length, replicas: ctx.bases.length,
    sendS: round(sendMs / 1000, 2), processed, elapsedS: round(elapsedS, 2),
    throughputMsgPerS: round(latencies.length / elapsedS),
    endToEndLatencyMs: latency(latencies),
    db: { outcomes: await dbOutcomes(ctx.db, name) },
    outbox: { drainAfterLoadMs: drainMs, ...(await outboxLag(ctx.db, name)) },
    cpuCoresBusyDuringLoad: cpuDuringLoad,
    counters: counterDelta(before, after),
    dbSampling: { max: observed.max, samples: observed.samples, series: observed.series },
  };
  say(`  ${processed}/${envelopes.length} processed in ${result.elapsedS}s -> ${result.throughputMsgPerS} msg/s, e2e p50/p95/p99 ${result.endToEndLatencyMs.p50}/${result.endToEndLatencyMs.p95}/${result.endToEndLatencyMs.p99} ms`);
  return result;
}




/** O assinante acompanha toda a carga e reprova evento duplicado ou ausente, mesmo após deduplicação FIFO. */
function startDownstream(lanes = 4) {
  const sqs = new SQSClient({ region: 'us-east-1', endpoint: SQS_ENDPOINT, credentials: { accessKeyId: 'test', secretAccessKey: 'test' } });
  const seen = new Map<string, number>();
  let deliveries = 0;
  let malformed = 0;
  let lastReceiveAt = Date.now();
  let stopping = false;
  const loop = async () => {
    while (!stopping) {
      try {
        const { Messages = [] } = await sqs.send(new ReceiveMessageCommand({ QueueUrl: QUEUES.events,
          MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 60 }));
        if (!Messages.length) continue;
        lastReceiveAt = Date.now();
        for (const message of Messages) {
          deliveries++;
          let eventId: unknown;
          try { eventId = (JSON.parse(message.Body ?? '') as { eventId?: unknown }).eventId; } catch { eventId = undefined; }
          if (typeof eventId !== 'string') { malformed++; continue; }
          seen.set(eventId, (seen.get(eventId) ?? 0) + 1);
        }
        await sqs.send(new DeleteMessageBatchCommand({ QueueUrl: QUEUES.events,
          Entries: Messages.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle! })) }));
      } catch { if (!stopping) await Bun.sleep(200); }
    }
  };
  const running = Array.from({ length: lanes }, loop);
  return {

    async finish(db: Db) {
      const [{ n: expected }] = await db.unsafe('SELECT count(*)::int AS n FROM outbox_events');
      const deadline = Date.now() + cfg.drainTimeoutS * 1000;
      while (seen.size < expected && Date.now() < deadline && Date.now() - lastReceiveAt < 30000) await Bun.sleep(250);
      stopping = true;
      await Promise.all(running);
      sqs.destroy();
      const ids = await db.unsafe('SELECT event_id::text AS id FROM outbox_events') as { id: string }[];
      const missing = ids.filter(r => !seen.has(r.id)).length;
      const known = new Set(ids.map(r => r.id));
      const unknown = [...seen.keys()].filter(id => !known.has(id)).length;
      const duplicates = [...seen.values()].filter(c => c > 1).length;
      return { outboxEvents: expected, deliveries, distinctEventIds: seen.size, duplicateEventIds: duplicates,
        missingEventIds: missing, unknownEventIds: unknown, malformed };
    },
  };
}



async function invariants(ctx: Ctx, wallets: Wallet[], hotDrain: Wallet) {
  const [balances] = await ctx.db.unsafe(`WITH l AS (
      SELECT wallet_id, sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END) AS s FROM ledger_entries GROUP BY 1)
    SELECT count(*)::int AS wallets,
      count(*) FILTER (WHERE w.balance <> coalesce(l.s, 0))::int AS balance_ne_ledger_sum,
      count(*) FILTER (WHERE w.balance < 0)::int AS negative_balances
    FROM wallets w LEFT JOIN l ON l.wallet_id = w.id`);
  const [lastEntry] = await ctx.db.unsafe(`SELECT count(*)::int AS n FROM wallets w
    JOIN LATERAL (SELECT balance_after FROM ledger_entries e WHERE e.wallet_id = w.id ORDER BY wallet_version DESC LIMIT 1) last ON true
    WHERE last.balance_after <> w.balance`);
  const [ledgerShape] = await ctx.db.unsafe(`SELECT
      (SELECT count(*)::int FROM (SELECT transaction_id FROM ledger_entries GROUP BY 1 HAVING count(*) > 1) d) AS duplicate_ledger_tx,
      (SELECT count(*)::int FROM wager_transactions t WHERE t.status = 'PROCESSED' AND t.kind IN ('BET','WIN','REFUND','ROLLBACK')
         AND NOT EXISTS (SELECT 1 FROM ledger_entries e WHERE e.transaction_id = t.id)) AS processed_without_ledger,
      (SELECT count(*)::int FROM ledger_entries e JOIN wager_transactions t ON t.id = e.transaction_id
         WHERE t.status <> 'PROCESSED') AS ledger_for_unprocessed,
      (SELECT count(*)::int FROM ledger_entries e JOIN wager_transactions t ON t.id=e.transaction_id WHERE t.kind='LOSS') AS loss_with_ledger,
      (SELECT count(*)::int FROM wager_transactions WHERE kind='LOSS' AND (amount<>0 OR reference_external_transaction_id IS NOT NULL)) AS invalid_loss,
      (SELECT count(*)::int FROM wager_transactions t LEFT JOIN wager_transactions r ON r.id=t.reference_transaction_id
        WHERE t.status='PROCESSED' AND t.kind IN ('REFUND','ROLLBACK')
          AND (r.id IS NULL OR r.kind<>'BET' OR r.status<>'PROCESSED' OR r.wallet_id<>t.wallet_id
            OR r.provider_id<>t.provider_id OR r.round_id<>t.round_id OR r.amount<>t.amount)) AS bad_load_reversal,
      (SELECT count(*)::int FROM wager_transactions WHERE status IN ('PENDING','PENDING_REFERENCE')) AS non_terminal,
      (SELECT count(*)::int FROM ledger_entries) AS ledger_entries,
      (SELECT count(*)::int FROM wager_transactions) AS transactions,
      (SELECT count(*)::int FROM outbox_events WHERE published_at IS NULL) AS outbox_unpublished`);
  const [drain] = await ctx.db.unsafe(`SELECT balance::text AS balance,
      (SELECT count(*)::int FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET' AND status = 'PROCESSED') AS processed_bets
    FROM wallets WHERE id = $1`, [hotDrain.id]);
  const reconciliation = await mapLimit(wallets, 8, async (wallet, i) => {
    const response = await fetch(`${ctx.bases[i % ctx.bases.length]}/wallets/${wallet.id}/reconciliation`, { method: 'POST' });
    return response.status === 200 && ((await response.json()) as { consistent: boolean }).consistent === true;
  });
  const sqs = new SQSClient({ region: 'us-east-1', endpoint: SQS_ENDPOINT, credentials: { accessKeyId: 'test', secretAccessKey: 'test' } });
  const depth = async (url: string) => Number((await sqs.send(new GetQueueAttributesCommand({ QueueUrl: url,
    AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'] }))).Attributes?.['ApproximateNumberOfMessages'] ?? NaN);
  const queues = { commandsDlq: await depth(QUEUES.commandsDlq), eventsDlq: await depth(QUEUES.eventsDlq),
    commands: await depth(QUEUES.commands), events: await depth(QUEUES.events) };
  sqs.destroy();
  // Saldo e aposta em centavos bigint: a contagem esperada é divisão inteira exata, sem number.
  const drainBalanceCents = parseCents(cfg.hotDrainBalance);
  const drainBetCents = parseCents(HOT_DRAIN_BET);
  if (drainBalanceCents % drainBetCents !== 0n) throw new RangeError('HOT_DRAIN_BALANCE_NOT_DIVISIBLE');
  const expectedDrainBets = drainBalanceCents / drainBetCents;
  const checks = {
    balanceEqualsLedgerSum: balances.balance_ne_ledger_sum === 0,
    balanceEqualsLastLedgerEntry: lastEntry.n === 0,
    noNegativeBalance: balances.negative_balances === 0,
    noDuplicateLedgerEntry: ledgerShape.duplicate_ledger_tx === 0,
    everyProcessedHasLedger: ledgerShape.processed_without_ledger === 0,
    noLedgerForUnprocessed: ledgerShape.ledger_for_unprocessed === 0,
    lossZeroWithoutReferenceOrLedger: ledgerShape.loss_with_ledger === 0 && ledgerShape.invalid_loss === 0,
    loadReversalsMatchConfirmedBet: ledgerShape.bad_load_reversal === 0,
    noNonTerminalLeft: ledgerShape.non_terminal === 0,
    outboxFullyPublished: ledgerShape.outbox_unpublished === 0,
    reconciliationEndpointConsistent: reconciliation.every(Boolean),
    hotDrainExactlyBalance: drain.balance === '0.00' && BigInt(drain.processed_bets) === expectedDrainBets,
    noDeadLetters: queues.commandsDlq === 0 && queues.eventsDlq === 0,
    commandQueueDrained: queues.commands === 0,
  };
  return { checks, passed: Object.values(checks).every(Boolean), balances, lastEntryMismatch: lastEntry.n, ledger: ledgerShape,
    hotDrain: { ...drain, expectedProcessedBets: expectedDrainBets.toString() }, reconciliation: { called: reconciliation.length,
      consistent: reconciliation.filter(Boolean).length }, queues };
}



interface Ctx { db: Db; bases: string[]; pids: number[]; cpu: { postgres: () => number | null; localstack: () => number | null } }

function scanLogs(paths: string[]) {
  const counts: Record<string, number> = {};
  const sample: string[] = [];
  let lines = 0;
  for (const path of paths) {
    for (const line of readFileSync(path, 'utf8').split('\n')) {
      if (!line) continue;
      lines++;
      const level = /"level":"(warn|error)"/.exec(line)?.[1];
      if (!level) continue;
      const key = `${level}:${/"type":"([^"]+)"/.exec(line)?.[1] ?? '?'}:${/"errorCode":"([^"]+)"/.exec(line)?.[1] ?? ''}`;
      counts[key] = (counts[key] ?? 0) + 1;
      if (sample.length < 50) sample.push(line);
    }
  }
  return { lines, warnErrorByTypeAndCode: counts, sample };
}

async function environment(db: Db) {
  const [pg] = await db.unsafe('SHOW server_version');
  return {
    gitCommit: await run(['git', 'rev-parse', 'HEAD']),
    gitDirtyFiles: (await run(['git', 'status', '--porcelain'])).split('\n').filter(Boolean).length,
    // O build usa a árvore atual; mudanças de produto fora de HEAD precisam constar da evidência.
    srcDirty: (await run(['git', 'status', '--porcelain', '--', 'src'])).split('\n').filter(Boolean),
    bun: Bun.version, kernel: release(), cpuModel: cpus()[0]?.model, logicalCpus: cpus().length,
    memoryGiB: round(totalmem() / 2 ** 30), loadavg1mAtStart: round(loadavg()[0]!, 2),
    docker: await run(['docker', 'version', '--format', '{{.Server.Version}}'], {}, true),
    otherRunningContainers: (await run(['docker', 'ps', '-q'], {}, true)).split('\n').filter(Boolean).length,
    postgres: pg.server_version, localstackImage: 'localstack/localstack:4.9.2',
    dbPoolPerReplica: 4, workerIntervalMs: 250,
  };
}

const cores = (c: Record<string, number | null>) => [c['replicas'], c['postgres'], c['localstack'], c['loadGenerator']].map(v => v ?? '?').join(' / ');
function markdown(report: Record<string, any>): string {
  const rows = (report.scenarios.http as any[]).map(s => `| ${s.name} | ${s.concurrency} | ${s.requests} | ${s.elapsedS} | ${s.throughputRps} | ${s.latencyMs.p50} | ${s.latencyMs.p95} | ${s.latencyMs.p99} | ${s.latencyMs.max} | ${s.errorRatePct}% | ${s.outbox.lagMs.p50 ?? '-'} / ${s.outbox.lagMs.p95 ?? '-'} / ${s.outbox.lagMs.p99 ?? '-'} | ${s.outbox.drainAfterLoadMs ?? 'timeout'} | ${s.outbox.publishedPerS ?? '-'} | ${cores(s.cpuCoresBusyDuringLoad)} |`);
  const q = report.scenarios.sqs;
  return [
    `# Load run ${report.startedAt}`, '',
    `Commit ${report.environment.gitCommit} (${report.environment.gitDirtyFiles} dirty files), ${report.environment.logicalCpus} CPUs, Bun ${report.environment.bun}, PostgreSQL ${report.environment.postgres}, ${report.config.replicas} HTTP replicas + ${report.config.workers} worker processes${report.config.workers > 0 ? '' : ' (combined http,workers)'}.`, '',
    '| Scenario | VUs | Requests | s | req/s | p50 ms | p95 ms | p99 ms | max ms | errors | outbox lag p50/p95/p99 ms | outbox drain after load ms | outbox ev/s | CPU cores replicas/pg/sqs/gen |',
    '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|', ...rows, '',
    q ? `SQS: ${q.processed}/${q.messages} commands in ${q.elapsedS}s -> ${q.throughputMsgPerS} msg/s; end-to-end p50/p95/p99 ${q.endToEndLatencyMs.p50}/${q.endToEndLatencyMs.p95}/${q.endToEndLatencyMs.p99} ms; outbox lag p95 ${q.outbox.lagMs.p95} ms; CPU cores replicas/pg/sqs/gen ${cores(q.cpuCoresBusyDuringLoad)}.` : '', '',
    `Downstream subscriber: ${report.downstream.deliveries} deliveries, ${report.downstream.distinctEventIds}/${report.downstream.outboxEvents} distinct eventIds, ${report.downstream.duplicateEventIds} duplicates, ${report.downstream.missingEventIds} missing.`, '',
    `Invariants: ${report.invariants.passed ? 'PASSED' : 'FAILED'}`, '',
    ...Object.entries(report.invariants.checks).map(([k, v]) => `- ${k}: ${v ? 'ok' : 'FAILED'}`), '',
  ].join('\n');
}

async function main(): Promise<number> {
  mkdirSync(cfg.outDir, { recursive: true });
  const logDir = join(tmpdir(), `jwp-load-logs-${stamp}`);
  mkdirSync(logDir, { recursive: true });
  const startedAt = new Date().toISOString();
  say(`config ${JSON.stringify(cfg)}`);
  say('infra: compose down -v; up -d --wait (PostgreSQL + LocalStack SQS)');
  await compose('down', '-v', '--remove-orphans');
  await compose('up', '-d', '--wait');
  const replicas: Replica[] = [];
  const db: Db = new SQL({ url: OWNER_URL, max: 4 });
  let exitCodes: (number | string)[] = [];
  try {
    say('migrations + runtime LOGIN');
    await run(['bun', 'src/persistence/migrate.ts'], { JWP_MIGRATION_DATABASE_URL: OWNER_URL });
    await db.unsafe(`CREATE ROLE jwp_load_api LOGIN PASSWORD 'jwp_load_local'`);
    await db.unsafe('GRANT jwp_runtime TO jwp_load_api');
    const env = await environment(db);
    const httpRoles = cfg.workers > 0 ? 'http' : 'http,workers';
    say(`starting ${cfg.replicas} replicas (APP_ROLES=${httpRoles}) + ${cfg.workers} worker processes (APP_ROLES=workers)`);
    for (let i = 0; i < cfg.replicas; i++) replicas.push(await startReplica(i, logDir, httpRoles));
    for (let i = 0; i < cfg.workers; i++) replicas.push(await startReplica(i, logDir, 'workers'));
    const downstream = startDownstream();
    const ctx: Ctx = { db, bases: replicas.filter(r => r.base).map(r => r.base), pids: replicas.map(r => r.proc.pid), cpu: {
      postgres: await containerCpuReader(`${cfg.project}-postgres-1`), localstack: await containerCpuReader(`${cfg.project}-sqs-1`) } };

    say(`opening ${cfg.wallets} + ${cfg.sqsWallets} + 2 wallets`);
    const distributed = await mapLimit(Array.from({ length: cfg.wallets }), 16, (_, i) => openWallet(ctx.bases[i % ctx.bases.length]!, '1000000.00'));
    const sqsWallets = await mapLimit(Array.from({ length: cfg.sqsWallets }), 16, (_, i) => openWallet(ctx.bases[i % ctx.bases.length]!, '1000000.00'));
    const hot = await openWallet(ctx.bases[0]!, '10000000.00');
    const hotDrain = await openWallet(ctx.bases[0]!, cfg.hotDrainBalance);
    await waitOutboxDrained(db);

    const anyWallet: Pick = () => ({ wallet: distributed[Math.floor(Math.random() * distributed.length)]!, kind: randomKind(), amount: randomAmount() });
    if (cfg.warmupS > 0) {
      say(`warm-up ${cfg.warmupS}s (excluded from results)`);
      await closedLoop(ctx.bases, 'warmup', Math.min(8, cfg.concurrency), anyWallet, { durationMs: cfg.warmupS * 1000 });
      await waitOutboxDrained(db);
    }
    const http = [
      await httpScenario('distributed', ctx, cfg.concurrency, mixedPick(() => distributed[Math.floor(Math.random() * distributed.length)]!), { durationMs: cfg.durationS * 1000 }),
      await httpScenario('hot-wallet', ctx, cfg.concurrency, mixedPick(() => hot), { durationMs: cfg.durationS * 1000 }),
      await httpScenario('hot-drain', ctx, cfg.concurrency, () => ({ wallet: hotDrain, kind: 'BET', amount: HOT_DRAIN_BET }), { total: cfg.hotDrainBets }),
    ];
    const sqs = cfg.sqsMessages > 0 ? await sqsScenario(ctx, sqsWallets) : null;
    say('invariants: balance == ledger, reconciliation endpoint, DLQs');
    const checked = await invariants(ctx, [...distributed, ...sqsWallets, hot, hotDrain], hotDrain);
    for (const scenario of http) if (!scenario.db.clientVsDb.ok) checked.passed = false;
    const kinds = ['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'];
    const mixCovered = http.filter(s => ['distributed', 'hot-wallet'].includes(s.name))
      .every(s => kinds.every(kind => (s.kindOutcomes[`${kind}:PROCESSED`] ?? 0) > 0));
    Object.assign(checked.checks, { httpMixedKindsProcessed: mixCovered });
    checked.passed = checked.passed && mixCovered;
    say('downstream: waiting for every outbox eventId on wager-events.fifo');
    const subscriber = await downstream.finish(db);
    Object.assign(checked.checks, {
      downstreamNoDuplicateEventId: subscriber.duplicateEventIds === 0,
      downstreamReceivedEveryEvent: subscriber.missingEventIds === 0 && subscriber.unknownEventIds === 0 && subscriber.malformed === 0,
    });
    checked.passed = checked.passed && subscriber.duplicateEventIds === 0 && subscriber.missingEventIds === 0
      && subscriber.unknownEventIds === 0 && subscriber.malformed === 0;
    say(`  downstream ${JSON.stringify(subscriber)}`);
    say('stopping replicas (SIGTERM, drain)');
    exitCodes = await Promise.all(replicas.map(stopReplica));
    replicas.length = 0;
    const report = {
      startedAt, finishedAt: new Date().toISOString(), loadavg1mAtEnd: round(loadavg()[0]!, 2),
      config: { ...cfg, outDir: undefined }, environment: env,
      scenarios: { http, sqs }, downstream: subscriber, invariants: checked, replicaExitCodes: exitCodes,
      logs: scanLogs(readdir(logDir)),
    };
    writeFileSync(join(cfg.outDir, 'results.json'), `${JSON.stringify(report, null, 2)}\n`);
    writeFileSync(join(cfg.outDir, 'summary.md'), markdown(report));
    process.stdout.write(markdown(report));
    say(`results: ${cfg.outDir}`);
    return checked.passed && exitCodes.every(code => code === 0) ? 0 : 1;
  } finally {
    await Promise.all(replicas.map(stopReplica));
    await db.close();
    if (!cfg.keepInfra) await compose('down', '-v', '--remove-orphans').catch(() => undefined);
  }
}

function readdir(dir: string): string[] {
  return [...new Bun.Glob('replica-*.log').scanSync({ cwd: dir })].map(name => join(dir, name));
}

process.exit(await main());
