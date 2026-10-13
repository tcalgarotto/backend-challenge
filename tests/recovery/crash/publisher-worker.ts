import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { SqlOutboxStore } from '../../../src/messaging/outbox/sql-outbox-store.js';
import { SqsOutboxPublisher } from '../../../src/messaging/outbox/sqs-outbox-publisher.js';
import { sqsClient } from '../../integration/sqs/support/sqs.js';

const databaseUrl = process.env['JWP_WORKER_DATABASE_URL'];
const queueUrl = process.env['JWP_WORKER_QUEUE_URL'];
if (!databaseUrl || !queueUrl) throw new Error('Publisher requires JWP_WORKER_DATABASE_URL and JWP_WORKER_QUEUE_URL.');
const barrier = process.env['JWP_BARRIER'] ?? '';
if (!['', 'before-send', 'after-send'].includes(barrier)) throw new Error(`Unknown publisher barrier ${barrier}.`);
const leaseMs = Number(process.env['JWP_OUTBOX_LEASE_MS'] ?? '30000');

const log = (line: Record<string, unknown>) => console.log(JSON.stringify({ ...line, pid: process.pid }));
const orm = await openFinancialOrm(databaseUrl);
const real = sqsClient();

/** A barreira segura antes do SendMessage ou após o envio, antes de marcar publicado; o pai aplica SIGKILL. */
const client = {
  async send(command: unknown, options?: unknown) {
    if (barrier && command instanceof SendMessageCommand) {
      const eventId = command.input.MessageDeduplicationId;
      if (barrier === 'after-send') {
        const sent = await real.send(command, options as never);
        log({ type: 'barrier', stage: 'after-send', eventId, sqsMessageId: sent.MessageId });
      } else {
        log({ type: 'barrier', stage: 'before-send', eventId });
      }
      return new Promise(() => {});
    }
    return real.send(command as never, options as never);
  },
} as unknown as SQSClient;

const publisher = new SqsOutboxPublisher(client, queueUrl, new SqlOutboxStore(orm, leaseMs),
  { batchSize: Number(process.env['JWP_OUTBOX_BATCH'] ?? '5'), retryBaseMs: 50, retryMaxMs: 500 });
let stopping = false;
process.once('SIGTERM', () => { stopping = true; });
log({ type: 'ready', barrier, leaseMs });
while (!stopping) {
  try {
    const results = await publisher.publishBatch();
    for (const result of results) log({ type: 'publication', ...result });
    if (results.length === 0) await Bun.sleep(25);
  } catch (error) {
    log({ type: 'publish-error', code: (error as { code?: string }).code ?? String(error) });
    await Bun.sleep(100);
  }
}
log({ type: 'stopped' });
real.destroy();
await orm.close(true);
