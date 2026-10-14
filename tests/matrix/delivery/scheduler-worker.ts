
import { configureReversalPolicy } from '../../../src/wagering/domain/reversal-policy.js';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { SqlPendingReferenceScheduler } from '../../../src/messaging/pending-reference-scheduler.js';

const databaseUrl = process.env['JWP_WORKER_DATABASE_URL'];
if (!databaseUrl) throw new Error('Scheduler requires JWP_WORKER_DATABASE_URL.');

// Mesma validação de boot do servidor: política inválida encerra o worker sem ecoar o valor.
configureReversalPolicy(process.env);
const log = (line: Record<string, unknown>) => console.log(JSON.stringify({ ...line, pid: process.pid }));
const orm = await openFinancialOrm(databaseUrl);
const scheduler = new SqlPendingReferenceScheduler(orm, { baseBackoffMs: 100, maxBackoffMs: 400 });
let stopping = false;
process.once('SIGTERM', () => { stopping = true; });
const [backend] = await orm.em.fork().getConnection().execute<{ pid: number }[]>('SELECT pg_backend_pid() pid');
log({ type: 'ready', backend: backend!.pid });
while (!stopping) {
  try {
    const outcome = await scheduler.runOnce();
    if (outcome.status === 'IDLE') await Bun.sleep(25);
    else log({ type: 'outcome', ...outcome });
  } catch (error) {
    log({ type: 'scheduler-error', code: (error as { code?: string }).code ?? String(error) });
    await Bun.sleep(100);
  }
}
log({ type: 'stopped' });
await orm.close(true);
