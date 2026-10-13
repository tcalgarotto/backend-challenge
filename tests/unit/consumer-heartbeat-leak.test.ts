// Falha ao devolver um receipt aborta o lote; os demais devem retornar à fila
// com visibilidade zero e parar o heartbeat, evitando ocultação indefinida
// que bloquearia o grupo FIFO e a DLQ.
import { expect, test } from 'bun:test';
import { SqsWagerConsumer } from '../../src/messaging/sqs-wager-consumer.js';

test('defer failure returns visibility for the rest of the batch and stops heartbeats', async () => {
  const calls: { name: string; receipt?: string; visibility?: number }[] = [];
  let deferFailures = 1;
  let received = false;
  const client = {
    send: async (command: any) => {
      const name = command.constructor.name as string;
      const input = command.input;
      if (name === 'GetQueueAttributesCommand') return { Attributes: {} };
      if (name === 'ReceiveMessageCommand') {
        if (received) return { Messages: [] };
        received = true;
        return { Messages: [
          { ReceiptHandle: 'r1', Body: 'not-json', Attributes: { ApproximateReceiveCount: '1' } },
          { ReceiptHandle: 'r2', Body: 'not-json', Attributes: { ApproximateReceiveCount: '1' } },
        ] };
      }
      if (name === 'ChangeMessageVisibilityCommand') {
        calls.push({ name, receipt: input.ReceiptHandle, visibility: input.VisibilityTimeout });
        if (input.VisibilityTimeout !== 5 && input.VisibilityTimeout !== 0 && deferFailures-- > 0) {
          throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
        }
        return {};
      }
      if (name === 'DeleteMessageCommand') { calls.push({ name, receipt: input.ReceiptHandle }); return {}; }
      throw new Error('unexpected ' + name);
    },
  };
  const consumer = new SqsWagerConsumer(client as any, 'http://q/x.fifo', { execute: async () => { throw new Error('never'); } } as any,
    { heartbeatIntervalMs: 20, visibilityTimeoutSeconds: 5, waitTimeSeconds: 0 });

  await expect(consumer.pollOnce()).rejects.toThrow('socket hang up');
  const r2 = () => calls.filter(c => c.receipt === 'r2');
  expect(r2().at(-1)?.visibility).toBe(0);
  const settled = r2().length;
  await Bun.sleep(150);
  expect(r2().length).toBe(settled);
  expect(await consumer.stop(10)).toBe('DRAINED');
});

test('ack failure after commit does not abort the batch nor leave a heartbeat running', async () => {
  const calls: { name: string; receipt?: string }[] = [];
  const body = (messageId: string) => JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: '2026-10-12T00:00:00Z',
    data: { providerId: 'p', externalTransactionId: messageId, idempotencyKey: messageId, playerId: 'x', walletId: 'w',
      roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } } });
  let received = false;
  const client = {
    send: async (command: any) => {
      const name = command.constructor.name as string;
      const input = command.input;
      if (name === 'GetQueueAttributesCommand') return { Attributes: {} };
      if (name === 'ReceiveMessageCommand') {
        if (received) return { Messages: [] };
        received = true;
        return { Messages: [
          { ReceiptHandle: 'a1', Body: body('m1'), Attributes: { ApproximateReceiveCount: '1' } },
          { ReceiptHandle: 'a2', Body: body('m2'), Attributes: { ApproximateReceiveCount: '1' } },
        ] };
      }
      calls.push({ name, receipt: input.ReceiptHandle });
      if (name === 'DeleteMessageCommand' && input.ReceiptHandle === 'a1') throw Object.assign(new Error('ack lost'), { code: 'ECONNRESET' });
      return {};
    },
  };
  const processed: string[] = [];
  const consumer = new SqsWagerConsumer(client as any, 'http://q/x.fifo',
    { execute: async (_input: unknown, key: unknown) => { processed.push(String(key)); return { httpStatus: 200, body: {} }; } } as any,
    { heartbeatIntervalMs: 20, visibilityTimeoutSeconds: 5, waitTimeSeconds: 0 });

  const observations = await consumer.pollOnce();
  expect(processed).toEqual(['m1', 'm2']);
  expect(observations.map(o => o.disposition)).toEqual(['RETRY', 'ACKNOWLEDGED']);
  const settled = calls.length;
  await Bun.sleep(150);
  expect(calls.length).toBe(settled);
  expect(await consumer.stop(10)).toBe('DRAINED');
});
