import { createHash } from 'node:crypto';
import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { canonicalJson } from '../wagering/application/process-wager.js';

export interface WagerTransactionRequested {
  readonly messageId: string;
  readonly type: 'WagerTransactionRequested';
  readonly occurredAt: string;
  readonly data: Readonly<Record<string, unknown>> & {
    readonly idempotencyKey: string;
    readonly walletId: string;
  };
}

export class InvalidWagerMessageError extends Error {
  readonly code = 'INVALID_WAGER_MESSAGE' as const;
  constructor() {
    super('INVALID_WAGER_MESSAGE');
    this.name = 'InvalidWagerMessageError';
  }
}

const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);
const exactKeys = (value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []) =>
  required.every(key => Object.hasOwn(value, key)) &&
  Object.keys(value).every(key => required.includes(key) || optional.includes(key));
const messageIdPattern = /^[\x21-\x7e]{1,128}$/;
const instantPattern = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z$/;
const dataKeys = ['providerId', 'externalTransactionId', 'idempotencyKey', 'playerId', 'walletId', 'roundId', 'gameId', 'kind', 'money'] as const;

export function parseWagerMessageBody(body: unknown): WagerTransactionRequested {
  let parsed: unknown;
  try {
    parsed = typeof body === 'string' ? JSON.parse(body) : body;
  } catch {
    throw new InvalidWagerMessageError();
  }
  if (!record(parsed) || !exactKeys(parsed, ['messageId', 'type', 'occurredAt', 'data']) ||
      typeof parsed['messageId'] !== 'string' || !messageIdPattern.test(parsed['messageId']) ||
      parsed['type'] !== 'WagerTransactionRequested' ||
      typeof parsed['occurredAt'] !== 'string' || !instantPattern.test(parsed['occurredAt']) ||
      !Number.isFinite(Date.parse(parsed['occurredAt'])) || !record(parsed['data']) ||
      !exactKeys(parsed['data'], dataKeys, ['referenceExternalTransactionId']) ||
      typeof parsed['data']['idempotencyKey'] !== 'string' ||
      typeof parsed['data']['walletId'] !== 'string') {
    throw new InvalidWagerMessageError();
  }
  return parsed as unknown as WagerTransactionRequested;
}

export function messageCommand(envelope: WagerTransactionRequested): {
  readonly input: Readonly<Record<string, unknown>>;
  readonly idempotencyKey: string;
  readonly envelopeHash: string;
} {
  const { idempotencyKey, ...input } = envelope.data;
  return {
    input,
    idempotencyKey,
    envelopeHash: createHash('sha256')
      .update(canonicalJson({ type: envelope.type, data: envelope.data }), 'utf8')
      .digest('hex'),
  };
}

/** Produtores controlados ordenam por walletId e deduplicam o envio FIFO por messageId. */
export async function sendWagerTransactionRequested(
  client: SQSClient,
  queueUrl: string,
  envelope: WagerTransactionRequested,
): Promise<void> {
  await client.send(new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: canonicalJson(envelope),
    MessageGroupId: envelope.data.walletId,
    MessageDeduplicationId: envelope.messageId,
  }));
}
