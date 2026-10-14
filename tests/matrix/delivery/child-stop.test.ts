import { expect, test } from 'bun:test';
import type { Subprocess } from 'bun';
import { Child } from './support.js';

// Reproduz de forma determinística o pipe sem EOF após o exit que travou M14 e o afterAll (058).
test('Child.stop is bounded and idempotent when the pipe never reaches EOF after exit', async () => {
  const open = () => new ReadableStream<Uint8Array>({ start() {} });
  const proc = { pid: 4242, exitCode: 0, signalCode: null, exited: Promise.resolve(0), stdout: open(), stderr: open(), kill() {} };
  const child = new Child('stuck-pipe', proc as unknown as Subprocess<'ignore', 'pipe', 'pipe'>);
  const started = Date.now();
  const first = child.stop();
  expect(child.stop()).toBe(first);
  expect(await first).toEqual({ name: 'stuck-pipe', pid: 4242, exitCode: 0, signal: null, drained: false });
  expect(Date.now() - started).toBeLessThan(10_000);
}, 15_000);
