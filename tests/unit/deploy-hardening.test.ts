import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { loadEnvironment } from '../../src/config/env.js';
import { assertLocalIdentityAllowed, InsecureLocalAuthError, LocalProviderIdentity,
  type ProviderIdentityPort } from '../../src/shared/http/provider-identity.js';

const databaseUrl = 'postgresql://runtime:local@127.0.0.1:55432/jwp';
const local = new LocalProviderIdentity();
const idp: ProviderIdentityPort = { async identify() { return { providerId: 'p' }; } };

test('listen host defaults to loopback and only changes when HOST is set explicitly', () => {
  expect(loadEnvironment({ JWP_DATABASE_URL: databaseUrl }).host).toBe('127.0.0.1');
  expect(loadEnvironment({ JWP_DATABASE_URL: databaseUrl, HOST: '0.0.0.0' }).host).toBe('0.0.0.0');
});

test('image keeps the loopback default and Compose sets the container bind explicitly', () => {
  const dockerfile = readFileSync(new URL('../../Dockerfile', import.meta.url), 'utf8');
  expect(dockerfile.split('\n').filter(line => /^\s*ENV\b/.test(line)).join('\n')).not.toMatch(/\bHOST=/);
  const compose = readFileSync(new URL('../../compose.yml', import.meta.url), 'utf8');
  expect(compose).toMatch(/^\s+HOST: 0\.0\.0\.0$/m);
  expect(compose).toContain('"127.0.0.1:${JWP_APP_PORT_RANGE:-3037-3046}:3037"');
});

test('production boot refuses the local identity adapter without explicit opt-in', () => {
  expect(() => assertLocalIdentityAllowed({ NODE_ENV: 'production' }, local)).toThrow(InsecureLocalAuthError);
  for (const value of ['1', 'TRUE', 'yes', ''])
    expect(() => assertLocalIdentityAllowed({ NODE_ENV: 'production', JWP_ALLOW_INSECURE_LOCAL_AUTH: value }, local))
      .toThrow('INSECURE_LOCAL_AUTH_IN_PRODUCTION');
});

test('opt-in, non-production environments and real IdP adapters are allowed', () => {
  expect(() => assertLocalIdentityAllowed({ NODE_ENV: 'production', JWP_ALLOW_INSECURE_LOCAL_AUTH: 'true' }, local)).not.toThrow();
  expect(() => assertLocalIdentityAllowed({}, local)).not.toThrow();
  expect(() => assertLocalIdentityAllowed({ NODE_ENV: 'development' }, local)).not.toThrow();
  expect(() => assertLocalIdentityAllowed({ NODE_ENV: 'production' }, idp)).not.toThrow();
});
