

import { $ } from 'bun';
import { createHash } from 'node:crypto';
import { mkdir, readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const args = Bun.argv.slice(2);
if (args.some(arg => arg !== '--keep')) throw new Error('Usage: bun run test:compose [--keep]');
const keep = args.includes('--keep');
const project = Bun.env['SMOKE_PROJECT'] ?? 'jwp-compose-smoke';
if (!/^jwp-compose-smoke(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?$/.test(project)) {
  throw new Error('SMOKE_PROJECT must be jwp-compose-smoke or jwp-compose-smoke-<suffix>');
}
const replicas = Number(Bun.env['SMOKE_REPLICAS'] ?? '3');
if (!Number.isSafeInteger(replicas) || replicas < 3 || replicas > 10) throw new Error('SMOKE_REPLICAS must be 3..10');
const env = {
  ...process.env,
  APP_ROLES: 'http,workers',
  JWP_APP_REPLICAS: String(replicas),
  JWP_PG_PORT: Bun.env['SMOKE_PG_PORT'] ?? '56401',
  JWP_SQS_PORT: Bun.env['SMOKE_SQS_PORT'] ?? '56402',
  JWP_APP_PORT_RANGE: Bun.env['SMOKE_APP_PORT_RANGE'] ?? '3090-3099',
};
for (const port of [env.JWP_PG_PORT, env.JWP_SQS_PORT]) {
  if (!/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535) throw new Error('Invalid smoke dependency port');
}
const range = /^(\d+)-(\d+)$/.exec(env.JWP_APP_PORT_RANGE);
if (!range || Number(range[1]) < 1 || Number(range[2]) > 65535 || Number(range[2]) - Number(range[1]) + 1 < replicas) {
  throw new Error('SMOKE_APP_PORT_RANGE must provide a valid port range for every replica');
}
const output = resolve(root, Bun.env['SMOKE_OUT'] ?? `test-results/compose-smoke/${new Date().toISOString().replaceAll(':', '-')}`);
await mkdir(output, { recursive: true });
const compose = (...rest: string[]) => $`docker compose --project-directory ${root} -f ${resolve(root, 'compose.yml')} -p ${project} ${rest}`.cwd(root).env(env).quiet().nothrow();
const commands: { args: string[]; exit: number; stdout: string; stderr: string }[] = [];
async function runCompose(...rest: string[]) {
  const result = await compose(...rest);
  commands.push({ args: rest, exit: result.exitCode, stdout: result.stdout.toString(), stderr: result.stderr.toString() });
  if (result.exitCode !== 0) throw new Error(`compose ${rest.join(' ')} exited ${result.exitCode}: ${result.stderr.toString().slice(-2000)}`);
  return result.stdout.toString().trim();
}
async function docker(...rest: string[]) {
  const result = await $`docker ${rest}`.quiet().nothrow();
  if (result.exitCode !== 0) throw new Error(`docker ${rest.join(' ')} exited ${result.exitCode}: ${result.stderr.toString().slice(-2000)}`);
  return result.stdout.toString().trim();
}
const checks: { name: string; passed: boolean; detail: unknown }[] = [];
function check(name: string, passed: boolean, detail: unknown = null) {
  checks.push({ name, passed, detail });
  console.log(`[${passed ? 'PASS' : 'FAIL'}] ${name} :: ${JSON.stringify(detail).slice(0, 400)}`);
}
const endpoint = `http://127.0.0.1:${env.JWP_SQS_PORT}`;
const sqs = new SQSClient({ region: 'us-east-1', endpoint, maxAttempts: 1,
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' } });
let imageIds: string[] = [];
let bases: string[] = [];
try {
  // O namespace validado limita a limpeza aos recursos desta execução.
  await runCompose('down', '-v', '--remove-orphans');
  check('fresh Compose reset', true);
  await runCompose('up', '-d', '--build', '--wait', '--wait-timeout', '180');
  const text = await runCompose('ps', '-a', '--format', 'json');
  const containers = (text.startsWith('[') ? JSON.parse(text) : text.split('\n').filter(Boolean).map(line => JSON.parse(line))) as
    { ID: string; Service: string; State: string; Health: string; ExitCode: number }[];
  const applications = containers.filter(container => container.Service === 'app');
  check('expected services, no proxy', containers.length === replicas + 4 && containers.every(container =>
    ['app', 'postgres', 'sqs', 'migrate', 'db-role'].includes(container.Service)), containers.map(container => container.Service));
  check('all HTTP replicas healthy', applications.length === replicas && applications.every(container =>
    container.State === 'running' && container.Health === 'healthy'), applications.map(container =>
      ({ id: container.ID, state: container.State, health: container.Health })));
  check('PostgreSQL and SQS healthy', ['postgres', 'sqs'].every(service => containers.some(container =>
    container.Service === service && container.State === 'running' && container.Health === 'healthy')));
  check('migration and runtime role setup completed', ['migrate', 'db-role'].every(service => containers.some(container =>
    container.Service === service && container.State === 'exited' && container.ExitCode === 0)));

  const inspected = JSON.parse(await docker('inspect', ...applications.map(container => container.ID))) as
    { Image: string; Config: { Cmd: string[]; Env: string[] } }[];
  imageIds = [...new Set(inspected.map(container => container.Image))];
  check('replicas run the same compiled Docker image', imageIds.length === 1 && inspected.every(container =>
    JSON.stringify(container.Config.Cmd) === JSON.stringify(['bun', 'dist/main.js']) &&
    container.Config.Env.includes('APP_ROLES=http,workers')), imageIds);
  bases = await Promise.all(Array.from({ length: replicas }, async (_, index) => {
    const address = await runCompose('port', '--index', String(index + 1), 'app', '3037');
    if (!/^127\.0\.0\.1:\d+$/.test(address)) throw new Error(`Unexpected replica address: ${address}`);
    return `http://${address}`;
  }));
  for (const [index, base] of bases.entries()) {
    const response = await fetch(`${base}/health/ready`, { signal: AbortSignal.timeout(10_000) });
    const body = await response.json() as { status?: string; checks?: { postgres?: string; sqs?: string } };
    check(`replica ${index + 1} /health/ready`, response.status === 200 && body.status === 'ready' &&
      body.checks?.postgres === 'up' && body.checks.sqs === 'up', { base, status: response.status, body });
  }
  const pg = JSON.parse(await runCompose('exec', '-T', 'postgres', 'psql', '-U', 'jwp', '-d', 'jwp', '-At', '-v', 'ON_ERROR_STOP=1', '-c',
    `SELECT json_build_object('connected',true,'database',current_database(),
      'migrations',(SELECT count(*) FROM jwp_financial_migrations),
      'missingTables',(SELECT count(*) FROM unnest(ARRAY['wallets','wager_transactions','ledger_entries','outbox_events','wager_message_inbox','pending_reference_jobs','wager_event_inbox']) AS name WHERE to_regclass(name) IS NULL),
      'runtimeRoleSafe',(SELECT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls FROM pg_roles WHERE rolname='jwp_api'),
      'wallets',(SELECT count(*) FROM wallets),'wagers',(SELECT count(*) FROM wager_transactions),'outbox',(SELECT count(*) FROM outbox_events))`)) as
    { connected: boolean; database: string; migrations: number; missingTables: number; runtimeRoleSafe: boolean; wallets: number; wagers: number; outbox: number };
  check('PostgreSQL round trip, migrated schema and safe runtime LOGIN', pg.connected && pg.database === 'jwp' &&
    pg.migrations >= 6 && pg.missingTables === 0 && pg.runtimeRoleSafe, pg);
  check('database starts without financial data', pg.wallets === 0 && pg.wagers === 0 && pg.outbox === 0, pg);
  const attributes = new Map<string, Record<string, string>>();
  for (const name of ['wager-transactions.fifo', 'wager-transactions-dlq.fifo', 'wager-events.fifo', 'wager-events-dlq.fifo']) {
    const result = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: `${endpoint}/queue/us-east-1/000000000000/${name}`,
      AttributeNames: ['All'] }), { abortSignal: AbortSignal.timeout(10_000) });
    const attrs = result.Attributes ?? {};
    attributes.set(name, attrs);
    check(`SQS round trip ${name}`, attrs['QueueArn']?.endsWith(`:${name}`) === true && attrs['FifoQueue'] === 'true' &&
      (['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed'] as const).every(key => attrs[key] === '0'), attrs);
  }
  for (const name of ['wager-transactions', 'wager-events']) {
    const policy = JSON.parse(attributes.get(`${name}.fifo`)?.['RedrivePolicy'] ?? '{}') as { deadLetterTargetArn?: string; maxReceiveCount?: string | number };
    check(`SQS ${name} redrive`, policy.deadLetterTargetArn === attributes.get(`${name}-dlq.fifo`)?.['QueueArn'] &&
      Number(policy.maxReceiveCount) === 5, policy);
  }
} catch (error) {
  check('smoke execution', false, String(error));
  try { await Bun.write(resolve(output, 'compose-failure.log'), await runCompose('logs', '--no-color', '--tail', '100')); } catch { /* Limpeza não deve mascarar a falha original. */ }
} finally {
  sqs.destroy();
  if (!keep) {
    try {
      await runCompose('down', '-v', '--remove-orphans');
      const resources = {
        containers: await docker('ps', '-aq', '--filter', `label=com.docker.compose.project=${project}`),
        volumes: await docker('volume', 'ls', '-q', '--filter', `label=com.docker.compose.project=${project}`),
        networks: await docker('network', 'ls', '-q', '--filter', `label=com.docker.compose.project=${project}`),
      };
      check('down -v removes all smoke resources', Object.values(resources).every(value => value === ''), resources);
    } catch (error) { check('down -v cleanup', false, String(error)); }
  }
}
const sourcePaths = ['scripts/compose-smoke/run.ts', 'Dockerfile', 'compose.yml', 'compose.db.yml', 'compose.sqs.yml',
  'package.json', 'bun.lock', 'tsconfig.json', 'tsconfig.build.json',
  ...(await $`git ls-files src infra`.cwd(root).quiet().text()).trim().split('\n')];
const sourceSha256 = Object.fromEntries(await Promise.all(sourcePaths.map(async path =>
  [path, createHash('sha256').update(await readFile(resolve(root, path))).digest('hex')])));
const failed = checks.filter(item => !item.passed).length;
await Bun.write(resolve(output, 'commands.json'), JSON.stringify(commands, null, 2) + '\n');
await Bun.write(resolve(output, 'summary.json'), JSON.stringify({ project, replicas, bases, imageIds, kept: keep,
  sourceCommit: (await $`git rev-parse HEAD`.cwd(root).quiet().text()).trim(), sourceSha256,
  passed: checks.length - failed, failed, checks }, null, 2) + '\n');
console.log(`compose smoke: ${failed === 0 ? 'ALL PASSED' : `${failed} FAILED`} (${checks.length - failed} pass / ${failed} fail)`);
console.log(`evidence: ${output}`);
process.exitCode = failed === 0 ? 0 : 1;
