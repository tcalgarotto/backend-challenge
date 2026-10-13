import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { ProcessInboxWager, SqlInboxProcessWagerStore, type InboxMessageMetadata } from '../../../src/messaging/sql-inbox-process-wager-store.js';
import { SqsWagerConsumer, type WagerMessageProcessor } from '../../../src/messaging/sqs-wager-consumer.js';
import type { ProcessWagerResult } from '../../../src/wagering/application/process-wager.js';
import { sqsClient } from '../sqs/support/sqs.js';

const databaseUrl = process.env['JWP_MESSAGING_DATABASE_URL'];
const queueUrl = process.env['JWP_MESSAGING_QUEUE_URL'];
const delayMs = Number(process.env['JWP_MESSAGING_PROCESS_DELAY_MS'] ?? '10000');
if (!databaseUrl || !queueUrl || !Number.isSafeInteger(delayMs) || delayMs < 1) {
  throw new Error('Worker requires database URL, queue URL and a positive delay.');
}

const orm = await openFinancialOrm(databaseUrl);
const client = sqsClient();
const actual = new ProcessInboxWager(new SqlInboxProcessWagerStore(orm));
const delayed: WagerMessageProcessor = {
  async execute(input: unknown, idempotencyKey: unknown, metadata: InboxMessageMetadata): Promise<ProcessWagerResult> {
    console.log(`PROCESSING ${metadata.messageId}`);
    await Bun.sleep(delayMs);
    return actual.execute(input, idempotencyKey, metadata);
  },
};
const consumer = new SqsWagerConsumer(client, queueUrl, delayed, {
  waitTimeSeconds: 1,
  retryVisibilitySeconds: 0,
  shutdownGraceMs: 25,
});
consumer.installSigtermHandler(async result => {
  console.log(`STOPPED ${result}`);
  client.destroy();
  await orm.close(true);
  process.exit(0);
});
console.log('READY');
await consumer.run();
