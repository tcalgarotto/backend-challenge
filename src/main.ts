import 'reflect-metadata';
import { SQSClient } from '@aws-sdk/client-sqs';
import type { INestApplication } from '@nestjs/common';
import { ApplicationWorkers } from './runtime/workers.js';
import { loadEnvironment } from './config/env.js';
import { configureCurrencies } from './shared/domain/currency.js';
import { configureReversalPolicy } from './wagering/domain/reversal-policy.js';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { openFinancialOrm } from './persistence/financial-orm.js';
import { installHttpObservability } from './observability/http.js';
import { Observability, observedQueue } from './observability/observability.js';
import { InvalidJsonFilter } from './shared/http/invalid-json-filter.js';
import { LocalProviderIdentity, assertLocalIdentityAllowed } from './shared/http/provider-identity.js';

const config = loadEnvironment(process.env);
// Registro e allowlist de moedas são validados antes de qualquer dependência externa.
configureCurrencies(process.env);
// Política de reversão validada no boot; valor inválido encerra sem ecoar o conteúdo (JWP-ADR-005).
configureReversalPolicy(process.env);
// Sem adapter de IdP no repositório, o boot usa identidade local; em produção exige opt-in explícito.
const identity = new LocalProviderIdentity();
assertLocalIdentityAllowed(process.env, identity);
const { port, databaseUrl: url, readinessTimeoutMs } = config;
// Readiness consulta todas as filas configuradas, incluindo DLQs monitoradas.
// O contrato de readiness exige PostgreSQL e SQS em qualquer papel HTTP.
const queues = config.queueUrls.map(observedQueue);
const endpoint = config.endpoint;
const sqs = queues.length > 0
  ? new SQSClient({ region: config.region, maxAttempts: 1, ...(endpoint ? { endpoint } : {}) })
  : undefined;
// Migrations usam credenciais owner em comando separado do servidor.
// O runtime usa jwp_runtime e não executa DDL na inicialização.
const orm = await openFinancialOrm(url);
let app: INestApplication | undefined;
let workers: ApplicationWorkers | undefined;
try {
  const observability = new Observability({ orm, readinessTimeoutMs, requiredDependencies: ['postgres', 'sqs'], ...(sqs ? { sqs: { client: sqs, queues } } : {}) });
  workers = config.workers ? new ApplicationWorkers(orm, sqs!, config.workers, observability) : undefined;
  app = await NestFactory.create(AppModule.register(orm, observability, workers, identity), { logger: false, abortOnError: false });
  installHttpObservability(app, observability);
  app.useGlobalFilters(new InvalidJsonFilter());
  let closing = false;
  const shutdown = () => {
    if (closing) return;
    closing = true;
    // Limita o drain mesmo se o transporte ignorar seu timeout; trabalho sem ack
    // recupera por visibilidade ou lease. Saída forçada não é encerramento limpo.
    const deadline = setTimeout(() => {
      observability.logger.error('shutdown.timeout', {});
      process.exit(1);
    }, 30000);
    void app!.close().then(() => {
      clearTimeout(deadline);
      process.exit(0);
    }, () => {
      clearTimeout(deadline);
      observability.logger.error('shutdown.failed', {});
      process.exit(1);
    });
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  if (config.http) {
    await app.listen(port, config.host);
    const address = app.getHttpServer().address() as { port: number };
    process.stdout.write(`${JSON.stringify({ type: 'listening', port: address.port, pid: process.pid })}\n`);
  } else await app.init();
  workers?.start();
} catch (error) {
  await workers?.stop();
  await app?.close();
  sqs?.destroy();
  await orm.close(true);
  throw error;
}
