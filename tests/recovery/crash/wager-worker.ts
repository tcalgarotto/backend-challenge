import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { ProcessInboxWager, SqlInboxProcessWagerStore } from '../../../src/messaging/sql-inbox-process-wager-store.js';
import { SqsWagerConsumer, type WagerMessageProcessor } from '../../../src/messaging/sqs-wager-consumer.js';
import { sqsClient } from '../../integration/sqs/support/sqs.js';

const databaseUrl = process.env['JWP_WORKER_DATABASE_URL'];
const queueUrl = process.env['JWP_WORKER_QUEUE_URL'];
if (!databaseUrl || !queueUrl) throw new Error('Wager worker requires JWP_WORKER_DATABASE_URL and JWP_WORKER_QUEUE_URL.');
const barrier = process.env['JWP_BARRIER'] === 'after-commit';

const log = (line: Record<string, unknown>) => console.log(JSON.stringify({ ...line, pid: process.pid }));
const orm = await openFinancialOrm(databaseUrl);
const client = sqsClient();
const inbox = new ProcessInboxWager(new SqlInboxProcessWagerStore(orm));

/** A barreira fica após o commit de inbox/ledger/outbox e antes de DeleteMessage, permitindo SIGKILL com efeito durável e sem ack. */
const processor: WagerMessageProcessor = {
  async execute(input, idempotencyKey, metadata) {
    const result = await inbox.execute(input, idempotencyKey, metadata);
    log({ type: 'committed', messageId: metadata.messageId, transactionId: result.body.transactionId, status: result.body.status });
    if (barrier) {
      log({ type: 'barrier', stage: 'after-commit-before-ack', messageId: metadata.messageId });
      await new Promise(() => {});
    }
    return result;
  },
};

const consumer = new SqsWagerConsumer(client, queueUrl, processor,
  { waitTimeSeconds: 1, maxMessages: 1, retryVisibilitySeconds: 0, shutdownGraceMs: 5_000 });
let stopping = false;
consumer.installSigtermHandler(async result => {
  log({ type: 'stopped', result });
  client.destroy();
  await orm.close(true);
  process.exit(0);
});
process.once('SIGTERM', () => { stopping = true; });
log({ type: 'ready', barrier });
while (!stopping) {
  try {
    for (const observation of await consumer.pollOnce()) log({ type: 'message', ...observation });
  } catch (error) {
    if (stopping) break;
    log({ type: 'poll-error', code: (error as { code?: string; name?: string }).code ?? (error as { name?: string }).name });
    await Bun.sleep(100);
  }
}
await new Promise(() => {});
