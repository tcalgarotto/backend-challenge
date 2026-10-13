import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { Child, waitUntil } from '../../matrix/delivery/support.js';
import { isolatedPair, requireSqs, sqsClient, type QueuePair } from '../sqs/support/sqs.js';

const role = `jwp_a02_${randomUUID().replaceAll('-', '')}`;
const children: Child[] = [];
const sqs = sqsClient();
let orm: Awaited<ReturnType<typeof openFinancialOrm>>, commands: QueuePair, events: QueuePair, runtimeUrl: string;
const q = (sql: string, args: unknown[] = []) => orm.em.fork().getConnection().execute<any[]>(sql, args);
const urls: string[] = [];
async function start(name: string) {
  const child = Child.spawn(name, ['bun', 'run', 'start:workers'], {
    JWP_DATABASE_URL: runtimeUrl, JWP_SQS_ENDPOINT: process.env['JWP_SQS_ENDPOINT']!,
    JWP_SQS_COMMAND_QUEUE_URL: commands.url, JWP_SQS_EVENT_QUEUE_URL: events.url,
    JWP_SQS_QUEUE_URLS: '', AWS_ACCESS_KEY_ID: 'test', AWS_SECRET_ACCESS_KEY: 'test', AWS_REGION: 'us-east-1', PORT: '0', HOST: '127.0.0.1',
  });
  children.push(child);
  const listening = await child.waitFor(line => line['type'] === 'listening', 'HTTP listening');
  await child.waitFor(line => line['type'] === 'workers.started', 'workers started');
  urls.push(`http://127.0.0.1:${listening['port']}`);
  return child;
}
async function http(index: number, method: string, path: string, body?: any, key?: string) {
  const response = await fetch(urls[index] + path, { method, headers: { 'Content-Type': 'application/json', ...(key ? {'Idempotency-Key': key} : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}) });
  return { status: response.status, body: await response.json() as any };
}
function wager(wallet: any, kind: string, reference?: string) {
  return { providerId: 'a02-provider', externalTransactionId: randomUUID(), playerId: wallet.playerId, walletId: wallet.id,
    roundId: 'a02-round', gameId: 'a02-game', kind, money: { amount: '25.00', currency: 'BRL' },
    ...(reference ? { referenceExternalTransactionId: reference } : {}) };
}
async function send(body: any) {
  const messageId = randomUUID();
  await sqs.send(new SendMessageCommand({ QueueUrl: commands.url, MessageGroupId: body.walletId, MessageDeduplicationId: messageId,
    MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
      data: { ...body, idempotencyKey: body.externalTransactionId } }) }));
  return messageId;
}
async function allPublished() {
  return waitUntil(async () => {
    const [row] = await q('SELECT count(*)::int total,count(published_at)::int published FROM outbox_events');
    return row.total > 0 && row.total === row.published ? row : undefined;
  }, 'operational publisher marks every outbox event');
}

beforeAll(async () => {
  const ownerUrl = process.env['JWP_RUNTIME_TEST_DATABASE_URL'];
  if (!ownerUrl || !process.env['JWP_SQS_ENDPOINT']) throw new Error('Empty JWP_RUNTIME_TEST_DATABASE_URL and real JWP_SQS_ENDPOINT required; no skip.');
  await requireSqs(sqs);
  orm = await openFinancialOrm(ownerUrl);
  expect((await q("SELECT to_regclass('wallets') relation"))[0].relation).toBeNull();
  const migration = Bun.spawn(['bun','run','db:migrate'], { env:{ ...process.env, JWP_MIGRATION_DATABASE_URL:ownerUrl },stdout:'pipe',stderr:'pipe' });
  const [exit, output, errors] = await Promise.all([migration.exited,new Response(migration.stdout).text(),new Response(migration.stderr).text()]);
  expect(exit).toBe(0);
  console.log(JSON.stringify({proof:'operator-db-migrate',exit,output,errors}));
  await q(`CREATE ROLE ${role} LOGIN PASSWORD 'a02_test_only'`);
  await q(`GRANT jwp_runtime TO ${role}`);
  const url = new URL(ownerUrl); url.username = role; url.password = 'a02_test_only'; runtimeUrl = url.toString();
  const [permissions] = await q(`SELECT rolsuper,rolcreatedb,rolcreaterole,has_schema_privilege(?, 'public', 'CREATE') AS ddl FROM pg_roles WHERE rolname=?`,[role,role]);
  expect(permissions).toEqual({rolsuper:false,rolcreatedb:false,rolcreaterole:false,ddl:false});
  commands = await isolatedPair(sqs, 'wager-transactions.fifo');
  events = await isolatedPair(sqs, 'wager-events.fifo');
  for (let i=0;i<3;i++) await start(`service-${i}`);
  expect(new Set(children.map(c => c.pid)).size).toBe(3);
},60000);

afterAll(async () => {
  const shutdown = await Promise.all(children.map(c=>c.stop()));
  console.log(JSON.stringify({proof:'cleanup',shutdown}));
  if (commands) await commands.dispose();
  if (events) await events.dispose();
  sqs.destroy();
  if (orm) {
    try { await orm.migrator.down({to:0}); await q(`DROP ROLE IF EXISTS ${role}`); }
    finally { await orm.close(true); }
  }
},60000);

test('three operational starts consume SQS, publish SQL outbox and progress pending references', async () => {
  for (let i=0;i<3;i++) {
    expect((await http(i,'GET','/health/live')).status).toBe(200);
    expect((await http(i,'GET','/health/ready')).status).toBe(200);
  }
  const opened = await http(0,'POST','/wallets',{playerId:randomUUID(),initialBalance:{amount:'100.00',currency:'BRL'}});
  expect(opened.status).toBe(201);
  const wallet=opened.body, bet=wager(wallet,'BET'), refund=wager(wallet,'REFUND',bet.externalTransactionId);
  const pending=await http(1,'POST','/wagering/transactions',refund,refund.externalTransactionId);
  expect(pending.status).toBe(202); expect(pending.body.status).toBe('PENDING_REFERENCE');
  const transactionId=pending.body.transactionId;
  await waitUntil(async()=>{
    const [row]=await q('SELECT attempts,next_attempt_at FROM pending_reference_jobs WHERE transaction_id=?',[transactionId]);
    return row?.attempts>0 ? row : undefined;
  },'scheduler reschedules real absent reference');
  const before = await allPublished();
  const messageId=await send(bet);
  await waitUntil(async()=>{
    const [row]=await q('SELECT status FROM wager_transactions WHERE id=?',[transactionId]);
    return row?.status==='PROCESSED' ? row : undefined;
  },'scheduler processes REFUND after SQS BET');
  expect((await q('SELECT count(*)::int count FROM pending_reference_jobs WHERE transaction_id=?',[transactionId]))[0].count).toBe(0);
  expect((await q('SELECT processed_at FROM wager_message_inbox WHERE message_id=?',[messageId]))[0].processed_at).not.toBeNull();
  expect((await http(2,'GET',`/wallets/${wallet.id}`)).body).toMatchObject({balance:{amount:'100.00'},version:3});
  expect((await http(2,'POST',`/wallets/${wallet.id}/reconciliation`)).body).toMatchObject({consistent:true,checkedEntries:3});
  const after=await allPublished(); expect(after.total).toBeGreaterThan(before.total);
  const eventIds = new Set<string>();
  await waitUntil(async()=>{
    const response=await sqs.send(new ReceiveMessageCommand({QueueUrl:events.url,WaitTimeSeconds:1,MaxNumberOfMessages:10}));
    for(const msg of response.Messages??[]) {
      const event=JSON.parse(msg.Body!); expect(event.data.walletId).toBe(wallet.id);
      eventIds.add(event.eventId);
      await sqs.send(new DeleteMessageCommand({QueueUrl:events.url,ReceiptHandle:msg.ReceiptHandle!}));
    }
    return eventIds.size===after.total ? true : undefined;
  },'published eventIds arrive at separate real SQS queue');
  const metrics=await fetch(urls[0]+'/metrics').then(r=>r.text()); expect(metrics).toContain('jwp_outbox_publications_total');
  console.log(JSON.stringify({proof:'three-operational-services',pids:children.map(c=>c.of('listening')[0]!['pid']),before,after,eventIds:[...eventIds],pendingTransaction:transactionId,
    observations:children.flatMap(c=>c.lines).filter(l=>['outbox.publication','pending_reference.outcome','sqs.message'].includes(String(l['type'])))}));
},60000);

test('SIGTERM drains a SQL-blocked in-flight consumer before SQL/SQS close; restart publishes durable remainder',async()=>{
  const opened=await http(0,'POST','/wallets',{playerId:randomUUID(),initialBalance:{amount:'100.00',currency:'BRL'}});
  expect(opened.status).toBe(201);
  const wallet=opened.body, bet=wager(wallet,'BET');
  const lock=orm.em.fork(); await lock.begin();
  try {
    await lock.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE',[wallet.id],'all',lock.getTransactionContext());
    await send(bet);
    await waitUntil(async()=>{
      const rows=await q("SELECT pid FROM pg_stat_activity WHERE usename=? AND wait_event_type='Lock' AND query LIKE '%wallets%'",[role]);
      return rows.length>0 ? rows : undefined;
    },'consumer SQL reaches wallet lock (controlled drain barrier)',4000);
    for(const child of children) child.proc.kill('SIGTERM');
    // O sinal encerrou os loops, mas a TX em voo continua bloqueada pela barreira SQL.
    await Bun.sleep(100);
    expect(children.some(c=>c.proc.exitCode===null)).toBe(true);
    const draining=children.filter(c=>c.of('workers.stopped').length===0);
    expect(draining.length).toBeGreaterThan(0);
    for(const child of draining) process.kill(Number(child.of('listening')[0]!['pid']), 'SIGTERM');
  } finally { await lock.commit(); }
  await waitUntil(async()=>children.every(c=>c.proc.exitCode!==null || c.proc.signalCode!==null) ? true : undefined, 'one SIGTERM drains all command processes',20000);
  const stopped=await Promise.all(children.map(c=>c.stop()));
  for(const result of stopped) { expect(result.exitCode).toBe(0); expect(result.signal).not.toBe('SIGKILL'); }
  expect(children.every(c=>c.of('workers.stopped').length===1)).toBe(true);
  expect((await q('SELECT status FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',[bet.providerId,bet.externalTransactionId]))[0].status).toBe('PROCESSED');
  const beforeRestart=await q('SELECT count(*)::int pending FROM outbox_events WHERE published_at IS NULL');
  const restart=await start('service-restart');
  await allPublished();
  expect((await http(3,'POST',`/wallets/${wallet.id}/reconciliation`)).body).toMatchObject({consistent:true,storedBalance:{amount:'75.00'},checkedEntries:2});
  const ended=await restart.stop(); expect(ended.exitCode).toBe(0);
  console.log(JSON.stringify({proof:'SIGTERM-controlled-wallet-lock-drain-restart',stopped,beforeRestart,restart:ended,
    drainLogs:children.flatMap(c=>c.of('workers.stopped'))}));
},60000);


test('worker-only role starts without HTTP; invalid worker configuration fails before DB setup',async()=>{
  const worker=Child.spawn('workers-only',['bun','run','start'],{
    APP_ROLES:'workers',JWP_DATABASE_URL:runtimeUrl,JWP_SQS_ENDPOINT:process.env['JWP_SQS_ENDPOINT']!,
    JWP_SQS_COMMAND_QUEUE_URL:commands.url,JWP_SQS_EVENT_QUEUE_URL:events.url,JWP_SQS_QUEUE_URLS:'',
    AWS_ACCESS_KEY_ID:'test',AWS_SECRET_ACCESS_KEY:'test',AWS_REGION:'us-east-1',
  });
  children.push(worker);
  await worker.waitFor(l=>l['type']==='workers.started','internal workers started');
  expect(worker.of('listening')).toHaveLength(0);
  expect((await worker.stop()).exitCode).toBe(0);
  const controls:[Record<string,string>,string][]=[
    [{APP_ROLES:'http,invalid'},'INVALID_APP_ROLES'],
    [{APP_ROLES:'workers',JWP_SQS_COMMAND_QUEUE_URL:'',JWP_SQS_EVENT_QUEUE_URL:''},'WORKER_QUEUE_URLS_REQUIRED'],
    [{APP_ROLES:'workers',JWP_SQS_COMMAND_QUEUE_URL:commands.url,JWP_SQS_EVENT_QUEUE_URL:commands.url},'WORKER_QUEUES_MUST_BE_SEPARATE'],
  ];
  for(const [env,code] of controls){
    const child=Bun.spawn(['bun','dist/main.js'],{env:{...process.env,JWP_DATABASE_URL:'',...env},stdout:'pipe',stderr:'pipe'});
    const [exit,errors]=await Promise.all([child.exited,new Response(child.stderr).text()]);
    expect(exit).not.toBe(0);expect(errors).toContain(code);
  }
  console.log(JSON.stringify({proof:'worker-only-and-negative-configuration',codes:controls.map(c=>c[1])}));
},30000);

test('HTTP-only loads .env with Bun but is not ready while SQS is absent', async () => {
  const { mkdtemp, readFile, writeFile, rm } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { resolve } = await import('node:path');
  const directory = await mkdtemp(resolve(tmpdir(), 'jwp-http-env-'));
  const model = await readFile('.env.example', 'utf8');
  await writeFile(resolve(directory, '.env'), model
    .replace(/^JWP_DATABASE_URL=.*$/m, `JWP_DATABASE_URL=${runtimeUrl}`)
    .replace(/^PORT=.*$/m, 'PORT=0')
    .replace(/^JWP_SQS_ENDPOINT=.*$/m, 'JWP_SQS_ENDPOINT=http://127.0.0.1:9'));
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    !key.startsWith('JWP_') && !key.startsWith('AWS_') && !['PORT', 'HOST', 'APP_ROLES'].includes(key)));
  const child = Bun.spawn(['bun', resolve('dist/main.js')], { cwd: directory, env, stdout: 'pipe', stderr: 'pipe' });
  const lines: string[] = [];
  const consume = (async () => { for await (const chunk of child.stdout) lines.push(new TextDecoder().decode(chunk)); })();
  try {
    await waitUntil(async () => /"type":"listening"/.test(lines.join('')), 'HTTP-only .env listening');
    const port = /"type":"listening","port":(\d+)/.exec(lines.join(''))![1];
    const ready = await fetch(`http://127.0.0.1:${port}/health/ready`);
    expect(ready.status).toBe(503);
    expect(await ready.json()).toMatchObject({ status: 'unavailable', checks: { postgres: 'up', sqs: 'not_configured' } });
    expect(lines.join('')).not.toContain('workers.started');
  } finally {
    child.kill('SIGTERM');
    expect(await child.exited).toBe(0);
    await consume;
    await rm(directory, { recursive: true, force: true });
  }
});

test('compiled boot aggregates invalid environment fields before opening SQL or SQS', async () => {
  const child = Bun.spawn(['bun', 'dist/main.js'], { env: { ...process.env, APP_ROLES: 'workers,invalid',
    JWP_DATABASE_URL: 'postgresql://runtime:local@127.0.0.1:9/jwp', PORT: 'invalid',
    JWP_READINESS_TIMEOUT_MS: '0', JWP_WORKER_INTERVAL_MS: '0',
    JWP_SQS_COMMAND_QUEUE_URL: '', JWP_SQS_EVENT_QUEUE_URL: '', JWP_SQS_QUEUE_URLS: '',
  }, stdout: 'pipe', stderr: 'pipe' });
  const [exit, output, errors] = await Promise.all([child.exited,
    new Response(child.stdout).text(), new Response(child.stderr).text()]);
  expect(exit).not.toBe(0);
  expect(output).toBe('');
  for (const code of ['INVALID_ENVIRONMENT', 'INVALID_APP_ROLES', 'INVALID_PORT',
    'INVALID_READINESS_TIMEOUT_MS', 'INVALID_WORKER_INTERVAL_MS', 'WORKER_QUEUE_URLS_REQUIRED'])
    expect(errors).toContain(code);
  expect(errors).not.toContain('ECONNREFUSED');
  expect(errors).not.toContain('runtime:local@');
});
