import { expect, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import { observePostgresConnection } from '../../src/persistence/postgres-connection-failure.js';
import { respond, type HttpResponse } from '../../src/shared/http/typed-response.js';
import { InfrastructureUnavailableError, isTransientInfrastructureFailure, sqlTransactionPolicy } from '../../src/shared/infrastructure/sql-transaction.js';
import { TransactionQueries } from '../../src/wagering/application/transaction-queries.js';
import { WalletReadError } from '../../src/wallet/application/wallet-queries.js';

function capture() {
  const sent: { status?: number; body?: unknown } = {};
  const response: HttpResponse = { status(code) { sent.status = code; return response; }, json(body) { sent.body = body; } };
  return { response, sent };
}
const typed = (error: unknown) => error instanceof WalletReadError ? error : undefined;

test('shared classifier: SQLSTATE, socket codes, pool timeout and transport provenance only', () => {
  for (const code of ['40001', '40P01', '55P03', '57014', '57P01', '08006', 'ECONNREFUSED'])
    expect(isTransientInfrastructureFailure(Object.assign(new Error('x'), { code }))).toBe(true);
  const timeout = new Error('x'); timeout.name = 'KnexTimeoutError';
  expect(isTransientInfrastructureFailure(timeout)).toBe(true);
  const connection = new EventEmitter(), transport = new Error('x');
  observePostgresConnection(connection, () => undefined);
  connection.emit('error', transport);
  expect(isTransientInfrastructureFailure(transport)).toBe(true);
  for (const failure of [new Error('connection terminated'), Object.assign(new Error('x'), { code: '23514' }), Object.assign(new Error('x'), { code: 23 }), null, undefined])
    expect(isTransientInfrastructureFailure(failure)).toBe(false);
  expect(() => sqlTransactionPolicy({ lockTimeoutMs: 6000 })).toThrow();
  expect(() => sqlTransactionPolicy({ maxAttempts: 0 })).toThrow();
});

test('typed errors keep status, infrastructure is a retryable 503, unexpected errors propagate', async () => {
  const ok = capture();
  await respond(ok.response, async () => ({ httpStatus: 200, body: { amount: '1.00' } }), typed);
  expect(ok.sent).toEqual({ status: 200, body: { amount: '1.00' } });
  const cursor = capture();
  await respond(cursor.response, async () => { throw new WalletReadError('INVALID_CURSOR'); }, typed);
  expect(cursor.sent).toMatchObject({ status: 400, body: { error: { code: 'INVALID_CURSOR', retryable: false } } });
  expect((cursor.sent.body as { correlationId: string }).correlationId).toMatch(/^[0-9a-f-]{36}$/);
  for (const failure of [new InfrastructureUnavailableError(), Object.assign(new Error('x'), { code: '57P01' })]) {
    const unavailable = capture();
    await respond(unavailable.response, async () => { throw failure; }, typed);
    expect(unavailable.sent).toMatchObject({ status: 503, body: { error: { code: 'INFRASTRUCTURE_UNAVAILABLE', retryable: true } } });
  }
  const unexpected = capture(), bug = Object.assign(new Error('x'), { code: '23514' });
  await expect(respond(unexpected.response, async () => { throw bug; }, typed)).rejects.toBe(bug);
  expect(unexpected.sent).toEqual({});
});

test('transaction lookups validate identity shape before the store', async () => {
  const calls: unknown[][] = [];
  const queries = new TransactionQueries({
    async findById(id) { calls.push([id]); return undefined; },
    async findByProviderIdentity(provider, external) { calls.push([provider, external]); return undefined; },
  });
  for (const id of ['x', '', `${'0'.repeat(8)}-0000-0000-0000-000000000000 `])
    await expect(queries.byId(id)).rejects.toMatchObject({ code: 'INVALID_PAYLOAD', httpStatus: 400 });
  for (const [provider, external] of [['', 'e'], ['p', ' '], ['p', 'a\u0000b'], ['p', 'x'.repeat(129)], ['p\uD800', 'e']])
    await expect(queries.byProviderIdentity(provider, external)).rejects.toMatchObject({ code: 'INVALID_PAYLOAD' });
  expect(calls).toEqual([]);
  await expect(queries.byId('0192F298-345E-7E38-AF88-E43F851A819D')).rejects.toMatchObject({ code: 'TRANSACTION_NOT_FOUND', httpStatus: 404 });
  await expect(queries.byProviderIdentity('Provider-A', 'Tx-1')).rejects.toMatchObject({ code: 'TRANSACTION_NOT_FOUND' });

  expect(calls).toEqual([['0192f298-345e-7e38-af88-e43f851a819d'], ['Provider-A', 'Tx-1']]);
});
