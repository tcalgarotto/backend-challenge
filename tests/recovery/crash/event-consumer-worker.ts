import { ReceiveMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { SqsEventConsumer, SqlEventDeduplicator } from '../../../src/messaging/outbox/sqs-event-consumer.js';
import type { OutboxEnvelope } from '../../../src/messaging/outbox/sql-outbox-store.js';
import { sqsClient } from '../../integration/sqs/support/sqs.js';

const databaseUrl = process.env['JWP_WORKER_DATABASE_URL'];
const queueUrl = process.env['JWP_WORKER_QUEUE_URL'];
if (!databaseUrl || !queueUrl) throw new Error('Infraestrutura do consumidor de eventos não configurada.');
const barrier = process.env['JWP_EVENT_BARRIER'] === 'after-inbox';
const log = (line: Record<string, unknown>) => console.log(JSON.stringify({ ...line, pid: process.pid }));
const orm = await openFinancialOrm(databaseUrl);
const real = sqsClient();

// Guarda os IDs físicos recebidos, sem imprimir envelope, receipt ou conexão.
const client = {
  async send(command: unknown, options?: unknown) {
    const response = await real.send(command as never, options as never);
    if (command instanceof ReceiveMessageCommand) {
      for (const message of (response as { Messages?: { MessageId?: string; Body?: string }[] }).Messages ?? []) {
        const eventId = (JSON.parse(message.Body ?? '{}') as { eventId?: string }).eventId;
        log({ type: 'received', eventId, sqsMessageId: message.MessageId });
      }
    }
    return response;
  },
} as unknown as SQSClient;

class InboxWithBarrier extends SqlEventDeduplicator {
  override async accept(event: OutboxEnvelope) {
    const result = await super.accept(event);
    log({ type: 'accepted', eventId: event.eventId, duplicate: result.duplicate });
    if (barrier && !result.duplicate) {
      // O INSERT da inbox já confirmou; DeleteMessage ainda não foi chamado.
      log({ type: 'barrier', stage: 'after-inbox-before-ack', eventId: event.eventId });
      await new Promise<void>(() => {});
    }
    return result;
  }
}

const consumer = new SqsEventConsumer(client, queueUrl, new InboxWithBarrier(orm),
  { waitTimeSeconds: 1, maxMessages: barrier ? 1 : 10 });
let stopping = false;
process.once('SIGTERM', () => { stopping = true; });
try {
  log({ type: 'ready', barrier });
  while (!stopping) {
    for (const observation of await consumer.pollOnce()) log({ type: 'event', ...observation });
  }
} finally {
  real.destroy();
  await orm.close(true);
}
