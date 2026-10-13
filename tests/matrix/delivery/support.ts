import { randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { expect } from 'bun:test';
import type { Subprocess } from 'bun';
import { ListQueuesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';

export type Orm = Awaited<ReturnType<typeof openFinancialOrm>>;
export type Line = Record<string, unknown>;

export const root = resolve(import.meta.dir, '../../..');
const decoder = new TextDecoder();






export function requiredInfrastructure(): { ownerUrl: string; sqsEndpoint: string } {
  const ownerUrl = process.env['JWP_MATRIX_DATABASE_URL'];
  const sqsEndpoint = process.env['JWP_MATRIX_SQS_ENDPOINT'];
  if (!ownerUrl || !sqsEndpoint) {
    throw new Error('JWP_MATRIX_DATABASE_URL (schema owner, empty database) and JWP_MATRIX_SQS_ENDPOINT are required; ' +
      'run scripts/reproduce/reproduce.sh. The delivery matrix never skips infrastructure.');
  }
  if (!existsSync(resolve(root, 'dist/main.js'))) throw new Error('dist/main.js missing; run "bun run build" before the matrix.');
  return { ownerUrl, sqsEndpoint };
}

export function sqsClient(endpoint: string): SQSClient {
  return new SQSClient({
    endpoint, region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' }, maxAttempts: 1,
  });
}

export async function requireSqs(client: SQSClient, endpoint: string): Promise<string[]> {
  try {
    const { QueueUrls } = await client.send(new ListQueuesCommand({}), { abortSignal: AbortSignal.timeout(5000) });
    return QueueUrls ?? [];
  } catch (error) {
    throw new Error(`SQS unreachable at ${endpoint}; the matrix requires LocalStack from compose.sqs.yml.`, { cause: error });
  }
}

export async function waitUntil<T>(probe: () => Promise<T | undefined | false>, what: string, timeoutMs = 30_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined && value !== false) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}.`);
    await Bun.sleep(50);
  }
}


export class Child {
  readonly lines: Line[] = [];
  stderr = '';
  private buffer = '';
  private readonly drained: Promise<unknown>;

  constructor(readonly name: string, readonly proc: Subprocess<'ignore', 'pipe', 'pipe'>) {
    const out = (async () => {
      for await (const chunk of proc.stdout) {
        this.buffer += decoder.decode(chunk);
        for (let end = this.buffer.indexOf('\n'); end >= 0; end = this.buffer.indexOf('\n')) {
          const line = this.buffer.slice(0, end);
          this.buffer = this.buffer.slice(end + 1);
          try { this.lines.push(JSON.parse(line) as Line); } catch { this.lines.push({ type: 'raw', line }); }
        }
      }
    })();
    const err = (async () => { for await (const chunk of proc.stderr) this.stderr += decoder.decode(chunk); })();
    this.drained = Promise.all([out, err]);
  }

  get pid(): number { return this.proc.pid; }

  static spawn(name: string, command: string[], env: Record<string, string>): Child {
    return new Child(name, Bun.spawn(command, {
      cwd: root, env: { ...process.env, ...env }, stdin: 'ignore', stdout: 'pipe', stderr: 'pipe',
    }));
  }

  async waitFor(predicate: (line: Line) => boolean, what: string, timeoutMs = 20_000): Promise<Line> {
    return waitUntil(async () => {
      const found = this.lines.find(predicate);
      if (found) return found;
      if (this.proc.exitCode !== null) throw new Error(`${this.name} exited ${this.proc.exitCode} before ${what}; stderr=${this.stderr}`);
      return undefined;
    }, `${this.name}: ${what}`, timeoutMs);
  }

  of(type: string): Line[] { return this.lines.filter(line => line['type'] === type); }

  /** SIGTERM ignorado aparece como SIGKILL na prova, sem ocultar a falha. */
  async stop(): Promise<{ name: string; pid: number; exitCode: number | null; signal: string | null }> {
    if (this.proc.exitCode === null && this.proc.signalCode === null) this.proc.kill('SIGTERM');
    const exited = await Promise.race([this.proc.exited.then(() => true), Bun.sleep(20_000).then(() => false)]);
    if (!exited) this.proc.kill('SIGKILL');
    await this.proc.exited;
    await this.drained;
    return { name: this.name, pid: this.pid, exitCode: this.proc.exitCode, signal: exited ? this.proc.signalCode : 'SIGKILL' };
  }
}

export interface ApiInstance { readonly child: Child; readonly base: string }


export async function startApi(name: string, env: Record<string, string>): Promise<ApiInstance> {
  const child = Child.spawn(name, ['bun', 'dist/main.js'], { PORT: '0', HOST: '127.0.0.1', ...env });
  const listening = await child.waitFor(line => line['type'] === 'listening', 'listening');
  return { child, base: `http://127.0.0.1:${String(listening['port'])}` };
}

export const httpStats = { requests: 0, retried503: 0 };





export async function call(api: ApiInstance, method: string, path: string, body?: unknown, headers: Record<string, string> = {}):
Promise<{ status: number; body: any; instance: string }> {
  for (let attempt = 1; ; attempt++) {
    httpStats.requests++;
    const response = await fetch(api.base + path, {
      method, headers: { 'Content-Type': 'application/json', ...headers },
      ...(body === undefined ? {} : { body: typeof body === 'string' ? body : JSON.stringify(body) }),
    });
    const text = await response.text();
    let parsed: any;
    try { parsed = JSON.parse(text); } catch { parsed = text; }
    if (response.status === 503 && parsed?.error?.retryable === true && attempt < 20) {
      httpStats.retried503++;
      await Bun.sleep(20 * attempt);
      continue;
    }
    return { status: response.status, body: parsed, instance: api.child.name };
  }
}

export interface WalletRef { readonly id: string; readonly playerId: string }

export async function openWallet(api: ApiInstance, amount = '100.00'): Promise<WalletRef> {
  const result = await call(api, 'POST', '/wallets', { playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' } });
  expect(result.status).toBe(201);
  expect(result.body).toMatchObject({ balance: { amount, currency: 'BRL' }, version: 1 });
  return { id: result.body.id, playerId: result.body.playerId };
}

export interface Wager {
  readonly key: string;
  readonly body: {
    providerId: string; externalTransactionId: string; playerId: string; walletId: string;
    roundId: string; gameId: string; kind: string; money: { amount: string; currency: string };
    referenceExternalTransactionId?: string;
  };
}

export const PROVIDER = 'matrix-provider';

export function wager(wallet: WalletRef, kind: string, amount: string, reference?: string, round = 'matrix-round'): Wager {
  const id = randomUUID();
  return {
    key: `${PROVIDER}:matrix-tx-${id}`,
    body: {
      providerId: PROVIDER, externalTransactionId: `matrix-tx-${id}`, playerId: wallet.playerId, walletId: wallet.id,
      roundId: round, gameId: 'matrix-game', kind, money: { amount, currency: 'BRL' },
      ...(reference === undefined ? {} : { referenceExternalTransactionId: reference }),
    },
  };
}

export const submit = (api: ApiInstance, w: Wager) =>
  call(api, 'POST', '/wagering/transactions', w.body, { 'Idempotency-Key': w.key });

export function envelope(w: Wager, messageId = `matrix-msg-${randomUUID()}`) {
  return {
    messageId, type: 'WagerTransactionRequested' as const, occurredAt: new Date().toISOString(),
    data: { ...w.body, idempotencyKey: w.key },
  };
}

export async function walletState(orm: Orm, walletId: string) {
  const [row] = await orm.em.fork().getConnection().execute<Record<string, unknown>[]>(`SELECT balance::text balance, version::text version,
    (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger,
    (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id AND direction='DEBIT') debits,
    (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id AND direction='CREDIT') credits
    FROM wallets w WHERE id=?`, [walletId]);
  return row!;
}






export async function reconcile(orm: Orm, apis: readonly ApiInstance[], walletIds: readonly string[]) {
  expect(walletIds.length).toBeGreaterThan(0);
  const reports = await Promise.all(walletIds.map(async (id, index) => {
    const result = await call(apis[index % apis.length]!, 'POST', `/wallets/${id}/reconciliation`);
    expect(result.status).toBe(200);
    expect(result.body).toMatchObject({ walletId: id, consistent: true, difference: { amount: '0.00' } });
    expect(result.body.storedBalance).toEqual(result.body.calculatedBalance);
    return result.body as { checkedEntries: number };
  }));
  const ids = `{${walletIds.join(',')}}`;
  const rows = await orm.em.fork().getConnection().execute<Record<string, unknown>[]>(`
    WITH chain AS (
      SELECT wallet_id, wallet_version, direction, amount, balance_before, balance_after,
        lag(balance_after) OVER (PARTITION BY wallet_id ORDER BY wallet_version) previous_after,
        lag(wallet_version) OVER (PARTITION BY wallet_id ORDER BY wallet_version) previous_version,
        row_number() OVER (PARTITION BY wallet_id ORDER BY wallet_version DESC) reverse_rank
      FROM ledger_entries WHERE wallet_id = ANY(?::uuid[]))
    SELECT w.id, w.balance::text balance,
      (SELECT coalesce(sum(CASE WHEN c.direction='CREDIT' THEN c.amount ELSE -c.amount END),0)::numeric(20,2)::text
        FROM chain c WHERE c.wallet_id=w.id) rebuilt,
      (SELECT count(*)::int FROM chain c WHERE c.wallet_id=w.id AND c.previous_after IS NOT NULL
        AND (c.balance_before<>c.previous_after OR c.wallet_version<>c.previous_version+1)) broken_links,
      (SELECT count(*)::int FROM chain c WHERE c.wallet_id=w.id AND c.reverse_rank=1 AND c.balance_after<>w.balance) bad_tail
    FROM wallets w WHERE w.id = ANY(?::uuid[])`, [ids, ids]);
  expect(rows).toHaveLength(walletIds.length);
  for (const row of rows) {
    expect({ id: row['id'], rebuilt: row['rebuilt'], broken: row['broken_links'], tail: row['bad_tail'] })
      .toEqual({ id: row['id'], rebuilt: row['balance'], broken: 0, tail: 0 });
  }
  return { wallets: walletIds.length, checkedEntries: reports.reduce((sum, r) => sum + r.checkedEntries, 0) };
}

export const countBy = <T>(values: readonly T[]) => values.reduce((map, value) => map.set(value, (map.get(value) ?? 0) + 1), new Map<T, number>());
