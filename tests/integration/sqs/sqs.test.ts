import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import topology from '../../../contracts/messaging/topology.json' with { type: 'json' };
import commandSchema from '../../../contracts/messaging/wager-transaction-requested.schema.json' with { type: 'json' };
import commandExamples from '../../../contracts/messaging/examples/commands.json' with { type: 'json' };
import eventExamples from '../../../contracts/messaging/examples/events.json' with { type: 'json' };
import { validate } from './support/json-schema.js';
import { contractFor, isolatedPair, queueAttributes, queueUrl, requireSqs, sqsClient, type QueuePair } from './support/sqs.js';

const COMMANDS = 'wager-transactions.fifo';
const EVENTS = 'wager-events.fifo';

interface CommandEnvelope {
  readonly messageId: string;
  readonly type: string;
  readonly occurredAt: string;
  readonly data: { readonly walletId: string } & Readonly<Record<string, unknown>>;
}

const bet = commandExamples.valid.find(example => example.name === 'bet')!.message as CommandEnvelope;
const event = eventExamples.valid.find(example => example.name === 'balance-changed')!.message;

let client: SQSClient;
const pairs: QueuePair[] = [];

beforeAll(async () => {
  client = sqsClient();
  await requireSqs(client);
});

afterAll(async () => {
  await Promise.all(pairs.map(pair => pair.dispose()));
  client?.destroy();
});

async function pair(name: string): Promise<QueuePair> {
  const created = await isolatedPair(client, name);
  pairs.push(created);
  return created;
}

function command(messageId: string, walletId = bet.data.walletId): CommandEnvelope {
  return { ...bet, messageId, data: { ...bet.data, walletId, externalTransactionId: `tx-${messageId}`, idempotencyKey: `provider-a:tx-${messageId}` } };
}


async function sendCommand(url: string, envelope: CommandEnvelope): Promise<void> {
  await client.send(new SendMessageCommand({
    QueueUrl: url,
    MessageBody: JSON.stringify(envelope),
    MessageGroupId: envelope.data.walletId,
    MessageDeduplicationId: envelope.messageId,
  }));
}

async function receive(url: string, max = 10, waitSeconds = 1): Promise<Message[]> {
  const { Messages } = await client.send(new ReceiveMessageCommand({
    QueueUrl: url,
    MaxNumberOfMessages: max,
    WaitTimeSeconds: waitSeconds,
    MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId', 'MessageDeduplicationId'],
  }));
  return Messages ?? [];
}

async function returnVisibility(url: string, message: Message): Promise<void> {
  await client.send(new ChangeMessageVisibilityCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0 }));
}

async function ack(url: string, message: Message): Promise<void> {
  await client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
}

describe('provisioned topology (compose.sqs.yml)', () => {
  for (const queue of topology.queues) {
    test(`${queue.name} matches contracts/messaging/topology.json`, async () => {
      const attributes = await queueAttributes(client, await queueUrl(client, queue.name));
      const { maxReceiveCount, ...expected } = queue.attributes as Record<string, string>;
      for (const [name, value] of Object.entries(expected)) expect({ name, value: attributes[name] }).toEqual({ name, value });
      if ('deadLetterQueue' in queue) {
        const dlq = await queueAttributes(client, await queueUrl(client, queue.deadLetterQueue));
        expect(JSON.parse(attributes['RedrivePolicy'] ?? '{}')).toEqual({ deadLetterTargetArn: dlq['QueueArn'], maxReceiveCount });
      } else {
        expect(attributes['RedrivePolicy']).toBeUndefined();
      }
    });
  }
});

describe('command queue behaviour (isolated copy of the contract)', () => {
  test('round-trips a schema-valid envelope with walletId group and messageId deduplication', async () => {
    const { url } = await pair(COMMANDS);
    await sendCommand(url, command('roundtrip-1'));

    const [message] = await receive(url);
    expect(message).toBeDefined();
    const body = JSON.parse(message!.Body!);
    expect(validate(commandSchema, body)).toEqual([]);
    expect(body).toEqual(command('roundtrip-1'));
    expect(message!.Attributes?.MessageGroupId).toBe(bet.data.walletId);
    expect(message!.Attributes?.MessageDeduplicationId).toBe('roundtrip-1');
    expect(message!.MessageId).not.toBe(body.messageId);
    await ack(url, message!);
    expect(await receive(url)).toEqual([]);
  });

  test('the SQS deduplication window drops a resent envelope (optimisation only; inbox stays authoritative)', async () => {
    const { url } = await pair(COMMANDS);
    await sendCommand(url, command('dedup-1'));
    await sendCommand(url, command('dedup-1'));

    const messages = await receive(url);
    expect(messages).toHaveLength(1);
    await ack(url, messages[0]!);
    expect(await receive(url)).toEqual([]);
  });

  test('an unacked message is redelivered with a new receipt handle and incremented receive count', async () => {
    const { url } = await pair(COMMANDS);
    await sendCommand(url, command('redeliver-1'));

    const [first] = await receive(url);
    await returnVisibility(url, first!);
    const [second] = await receive(url);
    expect(second!.Body).toBe(first!.Body!);
    expect(second!.ReceiptHandle).not.toBe(first!.ReceiptHandle);
    expect(second!.Attributes?.ApproximateReceiveCount).toBe('2');
    await ack(url, second!);
  });

  test('FIFO keeps per-wallet order and blocks a group while its head is in flight, without blocking other wallets', async () => {
    const { url } = await pair(COMMANDS);
    const walletA = '0192f291-27dd-7d3f-8071-00000000000a';
    const walletB = '0192f291-27dd-7d3f-8071-00000000000b';
    for (const id of ['a-1', 'a-2', 'a-3']) await sendCommand(url, command(id, walletA));
    await sendCommand(url, command('b-1', walletB));

    const [headA] = await receive(url, 1);
    expect(JSON.parse(headA!.Body!).messageId).toBe('a-1');
    const whileInFlight = (await receive(url)).map(message => JSON.parse(message.Body!).messageId);
    expect(whileInFlight).toEqual(['b-1']);

    await ack(url, headA!);
    const rest = await receive(url);
    expect(rest.map(message => JSON.parse(message.Body!).messageId)).toEqual(['a-2', 'a-3']);
  });

  test(`after maxReceiveCount=${contractFor(COMMANDS).attributes.maxReceiveCount} receives without ack the message is redriven to the DLQ intact`, async () => {
    const { url, dlqUrl } = await pair(COMMANDS);
    const poison = { ...command('poison-1'), data: { ...command('poison-1').data, money: { amount: 25, currency: 'BRL' } } };
    await client.send(new SendMessageCommand({
      QueueUrl: url,
      MessageBody: JSON.stringify(poison),
      MessageGroupId: poison.data.walletId,
      MessageDeduplicationId: poison.messageId,
    }));

    const maxReceiveCount = Number(contractFor(COMMANDS).attributes.maxReceiveCount);
    for (let attempt = 1; attempt <= maxReceiveCount; attempt += 1) {
      const [message] = await receive(url);
      expect(message?.Attributes?.ApproximateReceiveCount).toBe(String(attempt));
      expect(validate(commandSchema, JSON.parse(message!.Body!)).length).toBeGreaterThan(0);
      await returnVisibility(url, message!);
    }

    expect(await receive(url)).toEqual([]);
    const [deadLetter] = await receive(dlqUrl);
    expect(JSON.parse(deadLetter!.Body!)).toEqual(poison);
  });
});

describe('event isolation (JWP-REQ-034)', () => {
  test('events published to wager-events.fifo never reach the command queue', async () => {
    const commands = await pair(COMMANDS);
    const events = await pair(EVENTS);
    await client.send(new SendMessageCommand({
      QueueUrl: events.url,
      MessageBody: JSON.stringify(event),
      MessageGroupId: event.data.walletId,
      MessageDeduplicationId: event.eventId,
    }));

    expect(await receive(commands.url)).toEqual([]);
    const [published] = await receive(events.url);
    expect(JSON.parse(published!.Body!)).toEqual(event);
    expect(published!.Attributes?.MessageDeduplicationId).toBe(event.eventId);
  });

  test('the provisioned command and event queues are distinct destinations', async () => {
    const commandsArn = (await queueAttributes(client, await queueUrl(client, COMMANDS)))['QueueArn'];
    const eventsArn = (await queueAttributes(client, await queueUrl(client, EVENTS)))['QueueArn'];
    expect(commandsArn).toBeDefined();
    expect(commandsArn).not.toBe(eventsArn);
  });
});
