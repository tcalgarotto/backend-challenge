import { afterEach, expect, test } from 'bun:test';
import {
  DEFAULT_REVERSAL_POLICY, REVERSAL_POLICY_VARIABLE, ReversalPolicyConfigError, configureReversalPolicy, parseReversalPolicy,
  reversalBlocked, reversalPolicy,
} from '../../src/wagering/domain/reversal-policy.js';

afterEach(() => { configureReversalPolicy({}); });

test('default policy follows the challenge: per_type when absent or blank', () => {
  expect(DEFAULT_REVERSAL_POLICY).toBe('per_type');
  expect(REVERSAL_POLICY_VARIABLE).toBe('JWP_REVERSAL_POLICY');
  expect(configureReversalPolicy({})).toBe('per_type');
  expect(configureReversalPolicy({ JWP_REVERSAL_POLICY: '  ' })).toBe('per_type');
  expect(reversalPolicy()).toBe('per_type');
});

test('per_reference is opt-in by configuration only', () => {
  expect(configureReversalPolicy({ JWP_REVERSAL_POLICY: 'per_reference' })).toBe('per_reference');
  expect(reversalPolicy()).toBe('per_reference');
  expect(parseReversalPolicy(' per_type ')).toBe('per_type');
});

test('invalid policy fails without echoing the raw value', () => {
  const secret = 'PER_REFERENCE-s3cr3t-value';
  for (const raw of [secret, 'per-type', 'PER_TYPE', 'per_type,per_reference']) {
    let error: unknown;
    try { configureReversalPolicy({ JWP_REVERSAL_POLICY: raw }); } catch (caught) { error = caught; }
    expect(error).toBeInstanceOf(ReversalPolicyConfigError);
    const e = error as ReversalPolicyConfigError;
    expect(e.code).toBe('INVALID_REVERSAL_POLICY');
    expect(e.length).toBe(raw.trim().length);
    expect(`${e.message} ${e.stack ?? ''} ${JSON.stringify(e)}`).not.toContain(raw);
  }
  // Falha não altera a política vigente.
  expect(reversalPolicy()).toBe('per_type');
});

test('collision rule per policy', () => {
  expect(reversalBlocked('per_type', 'ROLLBACK', ['REFUND'])).toBe(false);
  expect(reversalBlocked('per_type', 'REFUND', ['REFUND'])).toBe(true);
  expect(reversalBlocked('per_reference', 'ROLLBACK', ['REFUND'])).toBe(true);
  expect(reversalBlocked('per_reference', 'ROLLBACK', [])).toBe(false);
});
