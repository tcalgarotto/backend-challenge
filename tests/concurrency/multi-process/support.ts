import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { expect } from 'bun:test';
import type { Subprocess } from 'bun';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';

export type Orm = Awaited<ReturnType<typeof openFinancialOrm>>;
export type Line = Record<string, unknown>;

export const root = resolve(import.meta.dir, '../../..');
const decoder = new TextDecoder();


export function requiredInfrastructure(): { ownerUrl: string; container: string } {
  const ownerUrl = process.env['JWP_CONCURRENCY_DATABASE_URL'];
  const container = process.env['JWP_CONCURRENCY_POSTGRES_CONTAINER'];
  if (!ownerUrl || !container?.startsWith('jwp-concurrency-')) {
    throw new Error('Dedicated JWP_CONCURRENCY_DATABASE_URL and JWP_CONCURRENCY_POSTGRES_CONTAINER (jwp-concurrency-*) required; concurrency never skips infrastructure.');
  }
  return { ownerUrl, container };
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

  /** SIGTERM ignorado deve reprovar, não ser ocultado pela limpeza do teste. */
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

export async function startApi(name: string, runtimeUrl: string): Promise<ApiInstance> {
  const child = Child.spawn(name, ['bun', 'src/main.ts'], { JWP_DATABASE_URL: runtimeUrl, PORT: '0', HOST: '127.0.0.1' });
  const listening = await child.waitFor(line => line['type'] === 'listening', 'listening');
  return { child, base: `http://127.0.0.1:${String(listening['port'])}` };
}

export interface HttpStats { requests: number; retried503: number }
export const httpStats: HttpStats = { requests: 0, retried503: 0 };





export async function call(api: ApiInstance, method: string, path: string, body?: unknown, headers: Record<string, string> = {}):
Promise<{ status: number; body: any; instance: string }> {
  for (let attempt = 1; ; attempt++) {
    httpStats.requests++;
    const response = await fetch(api.base + path, {
      method, headers: { 'Content-Type': 'application/json', ...headers },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const parsed = await response.json() as any;
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

export function wager(wallet: WalletRef, kind: string, amount: string, reference?: string): Wager {
  const id = randomUUID();
  return {
    key: `concurrency-key-${id}`,
    body: {
      providerId: 'concurrency-provider', externalTransactionId: `concurrency-tx-${id}`, playerId: wallet.playerId, walletId: wallet.id,
      roundId: 'concurrency-round', gameId: 'concurrency-game', kind, money: { amount, currency: 'BRL' },
      ...(reference === undefined ? {} : { referenceExternalTransactionId: reference }),
    },
  };
}

export const submit = (api: ApiInstance, w: Wager) =>
  call(api, 'POST', '/wagering/transactions', w.body, { 'Idempotency-Key': w.key });

export function envelope(w: Wager, messageId = `concurrency-msg-${randomUUID()}`) {
  return {
    messageId, type: 'WagerTransactionRequested' as const, occurredAt: new Date().toISOString(),
    data: { ...w.body, idempotencyKey: w.key },
  };
}






export async function reconcile(orm: Orm, apis: readonly ApiInstance[], walletIds: readonly string[]) {
  expect(walletIds.length).toBeGreaterThan(0);
  const reports = await Promise.all(walletIds.map(async (id, index) => {
    const result = await call(apis[index % apis.length]!, 'POST', `/wallets/${id}/reconciliation`);
    expect(result.status).toBe(200);
    expect(result.body).toMatchObject({ walletId: id, consistent: true, difference: { amount: '0.00', currency: 'BRL' } });
    return result.body as { checkedEntries: number };
  }));
  const rows = await orm.em.fork().getConnection().execute<Record<string, unknown>[]>(`
    WITH chain AS (
      SELECT wallet_id, wallet_version, balance_before, balance_after,
        lag(balance_after) OVER (PARTITION BY wallet_id ORDER BY wallet_version) previous_after,
        lag(wallet_version) OVER (PARTITION BY wallet_id ORDER BY wallet_version) previous_version,
        row_number() OVER (PARTITION BY wallet_id ORDER BY wallet_version DESC) reverse_rank
      FROM ledger_entries WHERE wallet_id = ANY(?::uuid[]))
    SELECT w.id,
      (SELECT count(*)::int FROM chain c WHERE c.wallet_id=w.id AND c.previous_after IS NOT NULL
        AND (c.balance_before<>c.previous_after OR c.wallet_version<>c.previous_version+1)) broken_links,
      (SELECT count(*)::int FROM chain c WHERE c.wallet_id=w.id AND c.reverse_rank=1 AND c.balance_after<>w.balance) bad_tail,
      w.balance::text balance
    FROM wallets w WHERE w.id = ANY(?::uuid[])`, [`{${walletIds.join(',')}}`, `{${walletIds.join(',')}}`]);
  expect(rows).toHaveLength(walletIds.length);
  for (const row of rows) expect({ broken: row['broken_links'], tail: row['bad_tail'] }).toEqual({ broken: 0, tail: 0 });
  return { wallets: walletIds.length, checkedEntries: reports.reduce((sum, r) => sum + r.checkedEntries, 0) };
}

export async function walletState(orm: Orm, walletId: string) {
  const [row] = await orm.em.fork().getConnection().execute<Record<string, unknown>[]>(`SELECT balance::text balance, version::text version,
    (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id) ledger,
    (SELECT count(*)::int FROM ledger_entries WHERE wallet_id=w.id AND direction='DEBIT') debits
    FROM wallets w WHERE id=?`, [walletId]);
  return row!;
}
