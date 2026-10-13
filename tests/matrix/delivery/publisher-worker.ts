
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { SqlOutboxStore } from '../../../src/messaging/outbox/sql-outbox-store.js';
import { SqsOutboxPublisher } from '../../../src/messaging/outbox/sqs-outbox-publisher.js';
import { sqsClient } from './support.js';

const databaseUrl = process.env['JWP_WORKER_DATABASE_URL'];
const queueUrl = process.env['JWP_WORKER_QUEUE_URL'];
const endpoint = process.env['JWP_WORKER_SQS_ENDPOINT'];
if (!databaseUrl || !queueUrl || !endpoint) throw new Error('Publisher requires JWP_WORKER_DATABASE_URL, JWP_WORKER_QUEUE_URL and JWP_WORKER_SQS_ENDPOINT.');

const log = (line: Record<string, unknown>) => console.log(JSON.stringify({ ...line, pid: process.pid }));
const orm = await openFinancialOrm(databaseUrl);
const client = sqsClient(endpoint);
const publisher = new SqsOutboxPublisher(client, queueUrl, new SqlOutboxStore(orm), { batchSize: 5, retryBaseMs: 50, retryMaxMs: 500 });
let stopping = false;
process.once('SIGTERM', () => { stopping = true; });
const [backend] = await orm.em.fork().getConnection().execute<{ pid: number }[]>('SELECT pg_backend_pid() pid');
log({ type: 'ready', backend: backend!.pid });
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
client.destroy();
await orm.close(true);
