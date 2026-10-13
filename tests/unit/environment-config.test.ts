import { expect, test } from 'bun:test';
import { EnvironmentConfigError, loadEnvironment, loadRuntimeConfig } from '../../src/config/env.js';
import { runtimeConfig } from '../../src/runtime/workers.js';

const databaseUrl = 'postgresql://runtime:local@127.0.0.1:55432/jwp';
const commandsUrl = 'http://127.0.0.1:54566/queue/commands.fifo';
const eventsUrl = 'http://127.0.0.1:54566/queue/events.fifo';

test('environment schema preserves HTTP defaults and ignores unrelated variables', () => {
  expect(loadEnvironment({ JWP_DATABASE_URL: databaseUrl, UNRELATED: 'ignored' })).toEqual({
    http: true, port: 3037, host: '127.0.0.1', databaseUrl, readinessTimeoutMs: 1000,
    queueUrls: [], region: 'us-east-1',
  });
  expect(runtimeConfig({})).toEqual({ http: true });
});

test('environment schema preserves valid worker roles, numeric coercion and queue order', () => {
  const input = { APP_ROLES: ' workers, http ', JWP_DATABASE_URL: databaseUrl, PORT: '0', HOST: '0.0.0.0',
    JWP_READINESS_TIMEOUT_MS: '30000', JWP_WORKER_INTERVAL_MS: '1000', AWS_REGION: 'sa-east-1',
    JWP_SQS_ENDPOINT: 'http://localhost:54566', JWP_SQS_COMMAND_QUEUE_URL: commandsUrl,
    JWP_SQS_EVENT_QUEUE_URL: eventsUrl, JWP_SQS_QUEUE_URLS: ` ${eventsUrl}, , ${commandsUrl}, ${eventsUrl}` };
  expect(loadEnvironment(input)).toEqual({ http: true, port: 0, host: '0.0.0.0', databaseUrl,
    readinessTimeoutMs: 30000, region: 'sa-east-1', endpoint: 'http://localhost:54566',
    queueUrls: [eventsUrl, commandsUrl], workers: { commandsUrl, eventsUrl, intervalMs: 1000 } });
  expect(runtimeConfig(input)).toEqual(loadRuntimeConfig(input));
  expect(loadEnvironment({ JWP_DATABASE_URL: databaseUrl, PORT: '0x10', JWP_SQS_ENDPOINT: '' }).port).toBe(16);
});

test('boot reports missing and invalid environment fields together without their values', () => {
  let failure: unknown;
  try { loadEnvironment({ APP_ROLES: 'workers,bad-role', PORT: 'bad-port', HOST: '',
    JWP_READINESS_TIMEOUT_MS: '0', JWP_WORKER_INTERVAL_MS: '0', AWS_REGION: '',
    JWP_SQS_ENDPOINT: 'invalid-endpoint', JWP_SQS_QUEUE_URLS: 'invalid-queue' }); }
  catch (error) { failure = error; }
  expect(failure).toBeInstanceOf(EnvironmentConfigError);
  const error = failure as EnvironmentConfigError;
  expect(error.issues.map(issue => issue.field).sort()).toEqual([
    'APP_ROLES', 'AWS_REGION', 'HOST', 'JWP_DATABASE_URL', 'JWP_READINESS_TIMEOUT_MS',
    'JWP_SQS_COMMAND_QUEUE_URL', 'JWP_SQS_ENDPOINT', 'JWP_SQS_EVENT_QUEUE_URL', 'JWP_SQS_QUEUE_URLS',
    'JWP_WORKER_INTERVAL_MS', 'PORT',
  ].sort());
  for (const value of ['bad-role', 'bad-port', 'invalid-endpoint', 'invalid-queue']) expect(error.message).not.toContain(value);
});

test('worker schema rejects identical/non-FIFO queues and preserves HTTP role isolation', () => {
  expect(() => runtimeConfig({ APP_ROLES: 'workers', JWP_SQS_COMMAND_QUEUE_URL: commandsUrl,
    JWP_SQS_EVENT_QUEUE_URL: commandsUrl })).toThrow('WORKER_QUEUES_MUST_BE_SEPARATE');
  expect(() => runtimeConfig({ APP_ROLES: 'workers', JWP_SQS_COMMAND_QUEUE_URL: 'ftp://localhost/c.fifo',
    JWP_SQS_EVENT_QUEUE_URL: 'http://localhost/e' })).toThrow('WORKER_FIFO_QUEUE_URL_REQUIRED');
  expect(runtimeConfig({ APP_ROLES: 'http', JWP_WORKER_INTERVAL_MS: 'invalid' })).toEqual({ http: true });
  expect(loadEnvironment({ APP_ROLES: 'workers', JWP_DATABASE_URL: databaseUrl,
    JWP_SQS_COMMAND_QUEUE_URL: commandsUrl, JWP_SQS_EVENT_QUEUE_URL: eventsUrl }).workers?.intervalMs).toBe(250);
});

test('environment schema validates bounds and never exposes credentials in boot diagnostics', () => {
  const credential = ['private', 'credential', 'fixture'].join('-');
  for (const input of [{ PORT: '65536' }, { PORT: '-1' }, { PORT: '1.1' }, { JWP_READINESS_TIMEOUT_MS: '30001' },
    { JWP_READINESS_TIMEOUT_MS: 'NaN' }, { APP_ROLES: 'workers', JWP_WORKER_INTERVAL_MS: '60001' }]) {
    expect(() => loadEnvironment({ JWP_DATABASE_URL: databaseUrl, ...input })).toThrow(EnvironmentConfigError);
  }
  try { loadEnvironment({ JWP_DATABASE_URL: `invalid://user:${credential}@host/db` }); }
  catch (error) {
    expect(error).toBeInstanceOf(EnvironmentConfigError);
    expect((error as Error).message).toContain('JWP_DATABASE_URL=INVALID_DATABASE_URL');
    expect((error as Error).message).not.toContain(credential);
    return;
  }
  throw new Error('Expected invalid database URL');
});
