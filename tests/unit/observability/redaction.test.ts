import { expect, test } from 'bun:test';
import { JsonLogger, redactLogString, sanitizeLogFields } from '../../../src/observability/log.js';

test('JSON logger masks sensitive keys across case and separator variants', () => {
  const fields = { Authorization: 'Bearer credential', password: 'credential', token: 'credential', secret: 'credential',
    'access-token': 'credential', client_secret: { nested: 'credential' }, apiKey: 'credential', AWS_SECRET_ACCESS_KEY: 'credential',
    route: '/wallets/:walletId', payload: { token: 'credential' } };
  const lines: string[] = [];
  new JsonLogger(line => { lines.push(line); }).info('test.redaction', fields as never);
  const parsed = JSON.parse(lines[0]!);
  for (const key of Object.keys(fields).filter(key => !['route', 'payload'].includes(key))) expect(parsed[key]).toBe('[REDACTED]');
  expect(parsed.route).toBe('/wallets/:walletId');
  expect(parsed.payload).toBeUndefined();
  expect(lines[0]).not.toContain('credential');
});

test('allowed strings mask URL userinfo, including encoded and multiple credentials', () => {
  expect(redactLogString('postgresql://user:credential@host/db https://user%40name:p%3Ass@host/path')).toBe(
    'postgresql://[REDACTED]@host/db https://[REDACTED]@host/path');
  expect(sanitizeLogFields({ reason: 'postgres://user:credential@host/db', status: 'PROCESSED', durationMs: 3 })).toEqual({
    reason: 'postgres://[REDACTED]@host/db', status: 'PROCESSED', durationMs: 3,
  });
});

test('redaction precedes truncation and does not mutate caller fields', () => {
  const input = { reason: `postgresql://user:${'x'.repeat(180)}@host/db`, correlationId: 'trace-1' };
  expect(sanitizeLogFields(input)).toEqual({ reason: 'postgresql://[REDACTED]@host/db', correlationId: 'trace-1' });
  expect(input.reason).toContain('x'.repeat(180));
  expect(sanitizeLogFields({ reason: 'x'.repeat(180) })['reason']).toHaveLength(128);
});

test('credentials embedded in permitted diagnostic strings are masked', () => {
  const input = 'https://host/db?token=credential&password="other credential"; authorization: Bearer credential';
  const output = redactLogString(input);
  expect(output).not.toContain('credential');
  expect(output).toContain('token=[REDACTED]');
  expect(output).toContain('password=[REDACTED]');
  expect(redactLogString('PROCESSED /wallets/:walletId ECONNREFUSED')).toBe('PROCESSED /wallets/:walletId ECONNREFUSED');
});

test('inline keys with prefix or suffix around secret words are masked without over-masking', () => {
  expect(redactLogString('boot failed AWS_SECRET_ACCESS_KEY=credential region=us-east-1')).toBe(
    'boot failed AWS_SECRET_ACCESS_KEY=[REDACTED] region=us-east-1');
  expect(redactLogString('db_password=credential; Db_Password: "other credential"')).toBe(
    'db_password=[REDACTED]; Db_Password=[REDACTED]');
  for (const [sample, key] of [['x-api-key: hunter2', 'x-api-key'], ['refresh_token=hunter2', 'refresh_token'],
    ['aws_credentials=hunter2', 'aws_credentials'], ['OAuthState=hunter2', 'OAuthState']])
    expect(redactLogString(sample!)).toBe(`${key}=[REDACTED]`);
  expect(sanitizeLogFields({ reason: 'AWS_SECRET_ACCESS_KEY=credential db_password=credential', correlationId: 'trace-1',
    route: '/wallets/:walletId/transactions', durationMs: 12 })).toEqual({
    reason: 'AWS_SECRET_ACCESS_KEY=[REDACTED] db_password=[REDACTED]', correlationId: 'trace-1',
    route: '/wallets/:walletId/transactions', durationMs: 12,
  });
});

test('idempotency key names stay visible as object keys in any case and separator', () => {
  const fields = { idempotencyKey: 'idem-123', idempotency_key: 'idem-456', 'Idempotency-Key': 'idem-789', IDEMPOTENCY_KEY: 'idem-000' };
  expect(sanitizeLogFields(fields)).toEqual(fields);
  const lines: string[] = [];
  new JsonLogger(line => { lines.push(line); }).info('test.idempotency', { idempotencyKey: 'idem-123', correlationId: 'trace-1' });
  expect(JSON.parse(lines[0]!)).toMatchObject({ idempotencyKey: 'idem-123', correlationId: 'trace-1' });
});

test('inline idempotency keys stay visible, including inside reason', () => {
  for (const sample of ['idempotencyKey=idem-123', 'idempotency_key: idem-123', 'Idempotency-Key: idem-123',
    'IDEMPOTENCY-KEY=idem-123', 'idempotencykey="idem-123"'])
    expect(redactLogString(sample)).toBe(sample);
  expect(sanitizeLogFields({ reason: 'replay Idempotency-Key: idem-123 conflict' })).toEqual({ reason: 'replay Idempotency-Key: idem-123 conflict' });
});

test('real secrets in the same string as an idempotency key are still masked', () => {
  const output = redactLogString('Idempotency-Key: idem-123 api_key=credential password="other credential" authorization: Bearer credential x-idempotency-key-secret=credential');
  expect(output).toContain('Idempotency-Key: idem-123');
  expect(output).toContain('api_key=[REDACTED]');
  expect(output).toContain('password=[REDACTED]');
  expect(output).toContain('x-idempotency-key-secret=[REDACTED]');
  expect(output).not.toContain('credential');
  expect(sanitizeLogFields({ reason: 'idempotencyKey=idem-123 db_password=credential', token: 'credential' })).toEqual({
    reason: 'idempotencyKey=idem-123 db_password=[REDACTED]', token: '[REDACTED]' });
});

// Casos da QA cf0b0a2 (053): valores opacos válidos com cara de atribuição, Bearer ou URL.
const QA_VALUE = '0f1e2d3c4b5a69788796a5b4c3d2e1f0';
const QA_SHAPES: readonly (readonly [string, string])[] = [
  ['plain', `audit-${QA_VALUE}`], ['bearer-shaped', `Bearer ${QA_VALUE}`], ['assignment-shaped', `token=${QA_VALUE}`],
  ['uri-shaped', `postgresql://audit:${QA_VALUE}@host/db`], ['password-assignment', 'password=x'],
];
const QA_BODY = { providerId: 'qa-provider', externalTransactionId: 'qa-external', playerId: '7d3c3c1e-9a53-4c0e-9a0b-0d5c3c1e9a53',
  walletId: '8e4d4d2f-ab64-4d1f-8b1c-1e6d4d2fab64', roundId: 'qa-round', gameId: 'qa-game', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };

test('QA cf0b0a2: every opaque idempotency value is accepted by parseWager and logged verbatim as a field', async () => {
  const { parseWager } = await import('../../../src/wagering/application/process-wager.js');
  for (const key of ['idempotencyKey', 'Idempotency-Key']) for (const [, value] of QA_SHAPES) {
    expect(parseWager(QA_BODY, value).idempotencyKey).toBe(value);
    expect(sanitizeLogFields({ [key]: value })[key]).toBe(value);
  }
  for (const [, value] of QA_SHAPES) {
    const lines: string[] = [];
    new JsonLogger(line => { lines.push(line); }).info('test.idempotency', { idempotencyKey: value, correlationId: 'trace-1' });
    expect(JSON.parse(lines[0]!)).toMatchObject({ idempotencyKey: value, correlationId: 'trace-1' });
  }
});

test('QA cf0b0a2: inline idempotency values stay verbatim, including inside reason through the logger', () => {
  for (const key of ['idempotencyKey', 'Idempotency-Key']) for (const [, value] of QA_SHAPES) for (const sep of ['=', ': ']) {
    const inline = `${key}${sep}${value}`;
    expect(redactLogString(inline)).toBe(inline);
    expect(redactLogString(`${key}${sep}"${value}"`)).toBe(`${key}${sep}"${value}"`);
    const lines: string[] = [];
    new JsonLogger(line => { lines.push(line); }).warn('test.idempotency', { reason: `replay ${inline} conflict` });
    expect(JSON.parse(lines[0]!).reason).toBe(`replay ${inline} conflict`);
  }
  expect(redactLogString('idempotencyKey="key with spaces, & ; =x"')).toBe('idempotencyKey="key with spaces, & ; =x"');
});

test('QA cf0b0a2: secrets in other fields and elsewhere in the same string are still masked', () => {
  for (const [, value] of QA_SHAPES) {
    const output = redactLogString(`Idempotency-Key: ${value} api_key=credential authorization: Bearer credential postgres://u:credential@h/db x-idempotency-key-secret=credential`);
    expect(output).toContain(`Idempotency-Key: ${value} `);
    expect(output).not.toContain('credential');
    expect(sanitizeLogFields({ idempotencyKey: value, reason: 'db_password=credential', token: 'credential' })).toEqual({
      idempotencyKey: value, reason: 'db_password=[REDACTED]', token: '[REDACTED]' });
  }
  expect(redactLogString('url?idempotencyKey=k1&token=credential')).toBe('url?idempotencyKey=k1&token=[REDACTED]');
});
