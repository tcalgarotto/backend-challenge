import { describe, expect, test } from 'bun:test';
import commandSchema from '../../../contracts/messaging/wager-transaction-requested.schema.json' with { type: 'json' };
import eventSchema from '../../../contracts/messaging/integration-event.schema.json' with { type: 'json' };
import commandExamples from '../../../contracts/messaging/examples/commands.json' with { type: 'json' };
import eventExamples from '../../../contracts/messaging/examples/events.json' with { type: 'json' };
import { validate } from './support/json-schema.js';

interface Examples {
  readonly valid: ReadonlyArray<{ readonly name: string; readonly message: unknown }>;
  readonly invalid: ReadonlyArray<{ readonly name: string; readonly base: string; readonly patch: Readonly<Record<string, unknown>> }>;
}


function patched(examples: Examples, base: string, patch: Readonly<Record<string, unknown>>): unknown {
  const source = examples.valid.find(example => example.name === base);
  if (!source) throw new Error(`unknown base example ${base}`);
  const copy = structuredClone(source.message) as Record<string, unknown>;
  for (const [path, value] of Object.entries(patch)) {
    const keys = path.split('.');
    const last = keys.pop()!;
    let target = copy;
    for (const key of keys) target = target[key] as Record<string, unknown>;
    if (value === null) delete target[last];
    else target[last] = value;
  }
  return copy;
}

const suites = [
  { name: 'WagerTransactionRequested', schema: commandSchema, examples: commandExamples as Examples },
  { name: 'IntegrationEvent', schema: eventSchema, examples: eventExamples as Examples },
];

for (const suite of suites) {
  describe(`${suite.name} contract`, () => {
    for (const example of suite.examples.valid) {
      test(`accepts ${example.name}`, () => {
        expect(validate(suite.schema, example.message)).toEqual([]);
      });
    }
    for (const example of suite.examples.invalid) {
      test(`rejects ${example.name}`, () => {
        const message = patched(suite.examples, example.base, example.patch);
        expect(validate(suite.schema, message).length).toBeGreaterThan(0);
      });
    }
  });
}

describe('integration event coverage', () => {
  test('every minimum event type has a valid example', () => {
    const covered = new Set(eventExamples.valid.map(example => example.message.eventType));
    expect([...covered].sort()).toEqual([...eventSchema.properties.eventType.enum].sort());
  });

  test('every command kind except OPENING is submittable', () => {
    expect(commandSchema.$defs.data.properties.kind.enum).toEqual(['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']);
  });
});
