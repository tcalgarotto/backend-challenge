import 'reflect-metadata';
import { afterEach, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { MikroORM } from '@mikro-orm/postgresql';
import { AppModule } from '../../src/app.module.js';
import { Observability } from '../../src/observability/observability.js';
import type { ProviderIdentityPort, ProviderIdentityRequest } from '../../src/shared/http/provider-identity.js';


const orm = { async close() {} } as unknown as MikroORM;
const invalid = 'not-a-uuid';
const financial: [string, string, unknown?][] = [
  ['POST', '/wallets', {}],
  ['GET', `/wallets/${invalid}`],
  ['GET', `/wallets/${invalid}/ledger`],
  ['POST', `/wallets/${invalid}/reconciliation`, {}],
  ['POST', '/wagering/transactions', {}],
  ['GET', `/wagering/transactions/${invalid}`],
  ['GET', `/providers/p/wagering/transactions/${'x'.repeat(129)}`],
];

let app: INestApplication | undefined;
afterEach(async () => { await app?.close(); app = undefined; });

async function serve(identity?: ProviderIdentityPort): Promise<string> {
  app = await NestFactory.create(AppModule.register(orm, new Observability(), undefined, identity), { logger: false });
  await app.listen(0, '127.0.0.1');
  return `http://127.0.0.1:${(app.getHttpServer().address() as { port: number }).port}`;
}
const call = (base: string, [method, path, body]: [string, string, unknown?]) => fetch(`${base}${path}`, {
  method, headers: { 'content-type': 'application/json', authorization: 'Bearer test' },
  ...(body === undefined ? {} : { body: JSON.stringify(body) }),
});

test('local mode: financial routes reach domain validation and health stays open', async () => {
  const base = await serve();
  for (const route of financial) {
    const response = await call(base, route);
    expect({ route, status: response.status }).toEqual({ route, status: 400 });
    expect(await response.json()).toMatchObject({ error: { code: expect.any(String) } });
  }
  expect((await fetch(`${base}/health/live`)).status).toBe(200);
});

test('a denying port blocks every financial route but never health or metrics', async () => {
  const seen: ProviderIdentityRequest[] = [];
  const base = await serve({ async identify(request) { seen.push(request); return undefined; } });
  for (const route of financial) expect({ route, status: (await call(base, route)).status }).toEqual({ route, status: 403 });
  expect(seen).toHaveLength(financial.length);
  expect(seen[0]!.headers['authorization']).toBe('Bearer test');
  const live = await fetch(`${base}/health/live`);
  expect(live.status).toBe(200);
  expect(await live.json()).toEqual({ status: 'ok' });
  expect((await fetch(`${base}/health/ready`)).status).toBe(503);
  expect((await fetch(`${base}/metrics`)).status).toBe(200);
  expect(seen).toHaveLength(financial.length);
});
