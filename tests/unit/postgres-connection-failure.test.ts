import { DriverException } from '@mikro-orm/core';
import { expect, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import { isPostgresConnectionFailure, observePostgresConnection, transactionFailureCause } from '../../src/persistence/postgres-connection-failure.js';

test('only uncoded errors emitted by the connection carry transport provenance', () => {
  const connection = new EventEmitter();
  observePostgresConnection(connection, (error, received) => {
    expect(error).toBeNull();
    expect(received).toBe(connection);
  });
  const transport = new Error('arbitrary text');
  expect(isPostgresConnectionFailure(transport)).toBe(false);
  connection.emit('error', transport);
  expect(isPostgresConnectionFailure(transport)).toBe(true);
  expect(isPostgresConnectionFailure(new Error('arbitrary text'))).toBe(false);
  expect(isPostgresConnectionFailure(null)).toBe(false);

  const sql = Object.assign(new Error('arbitrary text'), { code: '23514' });
  connection.emit('error', sql);
  expect(isPostgresConnectionFailure(sql)).toBe(false);
});

test('transaction cause is read only from a completed transaction and never waits on a pending one', async () => {
  const driver = new Error('arbitrary text');
  const rejected = Promise.reject(driver);
  rejected.catch(() => undefined);
  const trx = (completed: boolean, executionPromise: Promise<unknown>) => ({ isCompleted: () => completed, executionPromise });
  expect(await transactionFailureCause(undefined)).toBeUndefined();
  expect(await transactionFailureCause(trx(false, rejected))).toBeUndefined();
  expect(await transactionFailureCause(trx(true, rejected))).toBe(driver);
  expect(await transactionFailureCause(trx(true, Promise.resolve()))).toBeUndefined();
  expect(await transactionFailureCause(trx(true, new Promise(() => undefined)))).toBeUndefined();
});

test('MikroORM driver wrapping preserves observed transport origin without classifying application errors',()=>{
  const connection=new EventEmitter();observePostgresConnection(connection,()=>undefined);
  const original=new Error('arbitrary');connection.emit('error',original);
  expect(isPostgresConnectionFailure(new DriverException(original))).toBe(true);
  expect(isPostgresConnectionFailure(new DriverException(new Error('arbitrary')))).toBe(false);
  const impostor=Object.assign(new Error('arbitrary'),{__jwpPostgresTransportOrigin:{}});
  expect(isPostgresConnectionFailure(impostor)).toBe(false);
  const coded=Object.assign(new Error('arbitrary'),{code:'23514'});connection.emit('error',coded);
  expect(isPostgresConnectionFailure(new DriverException(coded))).toBe(false);
});
