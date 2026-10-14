import { expect, test } from 'bun:test';

const cwd = new URL('../../', import.meta.url).pathname;
test('compiled server refuses missing database configuration', async () => {
  const env = { ...Bun.env }; delete env['JWP_DATABASE_URL'];
  const child = Bun.spawn(['bun', 'dist/main.js'], { cwd, env, stdout: 'pipe', stderr: 'pipe' });
  const [code, error] = await Promise.all([child.exited, new Response(child.stderr).text()]);
  expect(code).not.toBe(0);
  expect(error).toContain('JWP_DATABASE_URL_REQUIRED');
  expect(await new Response(child.stdout).text()).not.toContain('listening');
});

test('compiled server refuses an invalid JWP_REVERSAL_POLICY before touching infrastructure, without echoing it', async () => {
  const raw = 'Per_Reference-n0t-valid';
  const env = { ...Bun.env, JWP_DATABASE_URL: 'postgres://smoke:smoke@127.0.0.1:1/smoke', APP_ROLES: 'http', JWP_REVERSAL_POLICY: raw };
  const child = Bun.spawn(['bun', 'dist/main.js'], { cwd, env, stdout: 'pipe', stderr: 'pipe' });
  const [code, error, output] = await Promise.all([child.exited, new Response(child.stderr).text(), new Response(child.stdout).text()]);
  expect(code).not.toBe(0);
  expect(error).toContain('INVALID_REVERSAL_POLICY');
  expect(error + output).not.toContain(raw);
  expect(output).not.toContain('listening');
});

test('compiled health module boots Nest DI and answers real HTTP independently of infrastructure', async () => {
  const child = Bun.spawn(['bun', '-e', `import 'reflect-metadata';import {NestFactory} from '@nestjs/core';
    import {AppModule} from './dist/app.module.js';const app=await NestFactory.create(AppModule,{logger:false});
    app.enableShutdownHooks();await app.listen(0,'127.0.0.1');console.log(JSON.stringify({port:app.getHttpServer().address().port}));`],
    { cwd, stdout: 'pipe', stderr: 'pipe' });
  const reader = child.stdout.getReader();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const ready = (async () => {
      let output = '';
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) throw new Error(await new Response(child.stderr).text());
        output += new TextDecoder().decode(chunk.value);
        if (output.includes('\n')) return JSON.parse(output.split('\n')[0]!) as { port: number };
      }
    })();
    const { port } = await Promise.race([ready, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error('Bootstrap timeout')), 7000);
    })]);
    const response = await fetch(`http://127.0.0.1:${port}/health/live`);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: 'ok' });
  } finally {
    if (timer) clearTimeout(timer);
    child.kill(); await child.exited; reader.releaseLock();
  }
}, 10000);
