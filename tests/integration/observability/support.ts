import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import { GetQueueUrlCommand, ListQueuesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import { AppModule } from '../../../src/app.module.js';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { installHttpObservability } from '../../../src/observability/http.js';
import { JsonLogger } from '../../../src/observability/log.js';
import { Observability, type ObservedQueue } from '../../../src/observability/observability.js';
import { InvalidJsonFilter } from '../../../src/shared/http/invalid-json-filter.js';

export type Orm = Awaited<ReturnType<typeof openFinancialOrm>>;


export function infrastructure() {
  const databaseUrl = process.env['JWP_OBSERVABILITY_DATABASE_URL'];
  const postgresContainer = process.env['JWP_OBSERVABILITY_POSTGRES_CONTAINER'];
  const sqsEndpoint = process.env['JWP_OBSERVABILITY_SQS_ENDPOINT'];
  const sqsContainer = process.env['JWP_OBSERVABILITY_SQS_CONTAINER'];
  if (!databaseUrl || !sqsEndpoint || !postgresContainer?.startsWith('jwp-observability-') || !sqsContainer?.startsWith('jwp-observability-')) {
    throw new Error('Dedicated JWP_OBSERVABILITY_DATABASE_URL, JWP_OBSERVABILITY_POSTGRES_CONTAINER (jwp-observability-*), ' +
      'JWP_OBSERVABILITY_SQS_ENDPOINT and JWP_OBSERVABILITY_SQS_CONTAINER (jwp-observability-*) are required. Integration never skips infrastructure.');
  }
  return { databaseUrl, postgresContainer, sqsEndpoint, sqsContainer };
}

export function sqsClient(endpoint: string): SQSClient {
  return new SQSClient({ endpoint, region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' }, maxAttempts: 1 });
}

export async function requireSqs(client: SQSClient): Promise<void> {
  await client.send(new ListQueuesCommand({}), { abortSignal: AbortSignal.timeout(5000) });
}

export async function contractQueue(client: SQSClient, name: string): Promise<ObservedQueue> {
  const { QueueUrl } = await client.send(new GetQueueUrlCommand({ QueueName: name }));
  if (!QueueUrl) throw new Error(`queue ${name} missing in dedicated SQS`);
  return { name, url: QueueUrl };
}


export async function migratedOrm(databaseUrl: string): Promise<Orm> {
  const orm = await openFinancialOrm(databaseUrl);
  const [row] = await orm.em.fork().getConnection().execute<Record<string, unknown>[]>("SELECT to_regclass('public.wallets') relation");
  if (row?.['relation'] !== null) { await orm.close(true); throw new Error('Dedicated observability database is not empty.'); }
  await orm.migrator.up();
  return orm;
}

export function docker(...args: string[]): void {
  const result = Bun.spawnSync(['docker', ...args]);
  if (result.exitCode !== 0) throw new Error(`docker ${args.join(' ')} failed: ${result.stderr.toString()}`);
}

export async function waitPostgres(container: string): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt++) {
    if (Bun.spawnSync(['docker', 'exec', container, 'pg_isready', '-U', 'jwp']).exitCode === 0) return;
    await Bun.sleep(100);
  }
  throw new Error('Dedicated PostgreSQL did not come back.');
}

export async function until<T>(read: () => Promise<T>, accept: (value: T) => boolean, timeoutMs = 15000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await read();
    if (accept(value) || Date.now() > deadline) return value;
    await Bun.sleep(100);
  }
}

export function logCapture() {
  const lines: string[] = [];
  const logger = new JsonLogger(line => { lines.push(line); });
  return { lines, logger, entries: () => lines.map(line => JSON.parse(line) as Record<string, unknown>) };
}


export async function observedApp(orm: Orm, observability: Observability): Promise<{ app: INestApplication; base: string }> {
  const app = await NestFactory.create(AppModule.register(orm, observability), { logger: false });
  installHttpObservability(app, observability);
  app.useGlobalFilters(new InvalidJsonFilter());
  await app.listen(0, '127.0.0.1');
  return { app, base: await app.getUrl() };
}

export async function call(base: string, method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
  const started = performance.now();
  const response = await fetch(base + path, { method, headers: { 'Content-Type': 'application/json', ...headers },
    ...(body === undefined ? {} : { body: typeof body === 'string' ? body : JSON.stringify(body) }) });
  const text = await response.text();
  let parsed: any = text;
  try { parsed = JSON.parse(text); } catch {  }
  return { status: response.status, body: parsed, text, ms: performance.now() - started };
}


export function sample(text: string, series: string): number {
  const line = text.split('\n').find(candidate => candidate.startsWith(`${series} `));
  return line ? Number(line.slice(series.length + 1)) : 0;
}

export const wager = (wallet: { id: string; playerId: string }, kind: string, amount: string, extra: Record<string, unknown> = {}) => {
  const id = randomUUID();
  return { providerId: 'obs-provider', externalTransactionId: `obs-${id}`, playerId: wallet.playerId, walletId: wallet.id,
    roundId: `round-${id}`, gameId: 'obs-game', kind, money: { amount, currency: 'BRL' }, ...extra };
};
