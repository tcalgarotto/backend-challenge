import { EventEmitter } from 'node:events';
import { expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { httpObservability } from '../../../src/observability/http.js';
import { JsonLogger } from '../../../src/observability/log.js';
import { Observability, observeLockConflicts } from '../../../src/observability/observability.js';
import { recordInfrastructureFailure, respond } from '../../../src/shared/http/typed-response.js';
import { ProcessWagerError } from '../../../src/wagering/application/process-wager.js';

const quiet = () => new Observability({ logger: new JsonLogger(() => {}) });

// Resposta mínima no formato que o middleware e o respond usam.
function fakeResponse() {
  const emitter = new EventEmitter();
  const response = {
    statusCode: 200, locals: {} as Record<string, unknown>,
    status(code: number) { response.statusCode = code; return response; },
    json(_body: unknown) { return undefined; },
    on(event: 'finish', listener: () => void) { emitter.on(event, listener); return response; },
    finish() { emitter.emit('finish'); },
  };
  return response;
}
const lockTimeout = () => Object.assign(new Error('lock'), { code: '55P03' });

test('HTTP 503 caused by lock_timeout increments jwp_sql_transient_failures_total{channel="http"}', () => {
  const observability = quiet();
  const middleware = httpObservability(observability);
  const response = fakeResponse();
  middleware({ method: 'POST', route: { path: '/wagering/transactions' }, body: { kind: 'BET' } }, response, () => {});
  recordInfrastructureFailure(response, new ProcessWagerError('INFRASTRUCTURE_UNAVAILABLE', 503, { cause: lockTimeout() }));
  response.status(503).json({ error: { code: 'INFRASTRUCTURE_UNAVAILABLE' } });
  response.finish();
  expect(observability.metrics.sqlTransient.value({ channel: 'http', reason: 'lock_timeout' })).toBe(1);
});

test('typed HTTP routes record the transient cause; non-transient 503 and 4xx do not count', async () => {
  const observability = quiet();
  const middleware = httpObservability(observability);
  const transient = fakeResponse();
  middleware({ method: 'GET', route: { path: '/wallets/:walletId' } }, transient, () => {});
  await respond(transient, async () => { throw Object.assign(new Error('deadlock'), { code: '40P01' }); }, () => undefined);
  transient.finish();
  expect(transient.statusCode).toBe(503);
  expect(observability.metrics.sqlTransient.value({ channel: 'http', reason: 'deadlock' })).toBe(1);

  const unclassified = fakeResponse();
  middleware({ method: 'POST', route: { path: '/wagering/transactions' } }, unclassified, () => {});
  recordInfrastructureFailure(unclassified, new ProcessWagerError('INFRASTRUCTURE_UNAVAILABLE', 503));
  unclassified.status(503).json({});
  unclassified.finish();
  const rejected = fakeResponse();
  middleware({ method: 'POST', route: { path: '/wagering/transactions' } }, rejected, () => {});
  rejected.status(422).json({ status: 'REJECTED' });
  rejected.finish();
  expect(observability.metrics.sqlTransient.value({ channel: 'http', reason: 'deadlock' })).toBe(1);
  expect(observability.metrics.sqlTransient.value({ channel: 'http', reason: 'none' })).toBe(0);
});

test('lock conflict counter is monotonic per aborted statement, ignores other SQL errors and does not double-register', async () => {
  const knex = new EventEmitter();
  const orm = { em: { getConnection: () => ({ getKnex: () => knex }) } } as unknown as MikroORM;
  const observability = quiet();
  observeLockConflicts(orm, observability.metrics);
  observeLockConflicts(orm, observability.metrics);
  knex.emit('query-error', lockTimeout());
  knex.emit('query-error', new Error('wrapper', { cause: Object.assign(new Error('x'), { code: '40P01' }) }));
  knex.emit('query-error', Object.assign(new Error('unique'), { code: '23505' }));
  knex.emit('query-error', lockTimeout());
  expect(observability.metrics.lockConflicts.value({ reason: 'lock_timeout' })).toBe(2);
  expect(observability.metrics.lockConflicts.value({ reason: 'deadlock' })).toBe(1);
  const text = await observability.registry.render();
  expect(text).toContain('# TYPE jwp_lock_conflicts_total counter');
  expect(text).toContain('jwp_lock_conflicts_total{reason="lock_timeout"} 2');
});
