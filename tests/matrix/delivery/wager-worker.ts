

import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { ProcessInboxWager, SqlInboxProcessWagerStore } from '../../../src/messaging/sql-inbox-process-wager-store.js';
import { SqsWagerConsumer } from '../../../src/messaging/sqs-wager-consumer.js';
import { sqsClient } from './support.js';

const databaseUrl = process.env['JWP_WORKER_DATABASE_URL'];
const queueUrl = process.env['JWP_WORKER_QUEUE_URL'];
const endpoint = process.env['JWP_WORKER_SQS_ENDPOINT'];
if (!databaseUrl || !queueUrl || !endpoint) throw new Error('Wager worker requires JWP_WORKER_DATABASE_URL, JWP_WORKER_QUEUE_URL and JWP_WORKER_SQS_ENDPOINT.');

const log = (line: Record<string, unknown>) => console.log(JSON.stringify({ ...line, pid: process.pid }));
const orm = await openFinancialOrm(databaseUrl);
const client = sqsClient(endpoint);
const consumer = new SqsWagerConsumer(client, queueUrl, new ProcessInboxWager(new SqlInboxProcessWagerStore(orm)),
  { waitTimeSeconds: 1, maxMessages: 5, retryVisibilitySeconds: 0, shutdownGraceMs: 5_000 });
let stopping = false;
consumer.installSigtermHandler(async result => {
  log({ type: 'stopped', result });
  client.destroy();
  await orm.close(true);
  process.exit(0);
});
process.once('SIGTERM', () => { stopping = true; });
const [backend] = await orm.em.fork().getConnection().execute<{ pid: number }[]>('SELECT pg_backend_pid() pid');
log({ type: 'ready', backend: backend!.pid });
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
