import { Controller, Get, Inject, Optional, Res, type INestApplication } from '@nestjs/common';
import { Observability } from './observability.js';
import { sqlFailureReason, wagerKind } from './messaging.js';
import { PublicRoute } from '../shared/http/provider-identity.js';
import { assignedCorrelationId, recordedInfrastructureFailure } from '../shared/http/typed-response.js';

export const OBSERVABILITY = Symbol('JwpObservability');

interface HttpRequest {
  readonly method: string;
  readonly baseUrl?: string;
  readonly route?: { readonly path?: unknown };
  readonly params?: Readonly<Record<string, unknown>>;
  readonly body?: unknown;
}
interface HttpResponse {
  statusCode: number;
  locals?: Record<string, unknown>;
  json(body: unknown): unknown;
  on(event: 'finish', listener: () => void): unknown;
}
interface ReplyResponse {
  status(code: number): ReplyResponse;
  json(body: unknown): void;
  setHeader(name: string, value: string): void;
  send(body: string): void;
}

/** Sem probes configurados, readiness retorna 503. */
let unconfigured: Observability | undefined;
const fallback = () => (unconfigured ??= new Observability());

@PublicRoute()
@Controller('health')
export class HealthController {
  private readonly observability: Observability;
  constructor(@Optional() @Inject(OBSERVABILITY) observability?: Observability) { this.observability = observability ?? fallback(); }

  /** Liveness não consulta dependências; falha externa não torna o processo morto. */
  @Get('live')
  live(): { status: string } { return { status: 'ok' }; }

  @Get('ready')
  async ready(@Res() response: ReplyResponse): Promise<void> {
    const report = await this.observability.readiness.check();
    response.status(report.ready ? 200 : 503).json({ status: report.ready ? 'ready' : 'unavailable', checks: report.checks });
  }
}

@PublicRoute()
@Controller('metrics')
export class MetricsController {
  private readonly observability: Observability;
  constructor(@Optional() @Inject(OBSERVABILITY) observability?: Observability) { this.observability = observability ?? fallback(); }

  @Get()
  async metrics(@Res() response: ReplyResponse): Promise<void> {
    const text = await this.observability.registry.render(this.observability.collectTimeoutMs);
    response.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
    response.status(200).send(text);
  }
}

const string = (value: unknown): string | undefined => typeof value === 'string' && value.length > 0 ? value : undefined;
const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : undefined;

/** Usa template da rota, não caminho com IDs, para limitar cardinalidade. */
function routeTemplate(request: HttpRequest): string {
  const path = request.route?.path;
  return typeof path === 'string' ? `${request.baseUrl ?? ''}${path}` || '/' : 'unmatched';
}

/** Lê somente campos permitidos da resposta; valores monetários não entram nos logs. */
export function httpObservability(observability: Observability) {
  const { metrics, logger } = observability;
  return (request: HttpRequest, response: HttpResponse, next: () => void): void => {
    const started = performance.now();
    let body: Record<string, unknown> | undefined;
    const json = response.json.bind(response);
    response.json = (value: unknown) => { body = record(value); return json(value); };
    response.on('finish', () => {
      const seconds = (performance.now() - started) / 1000;
      const route = routeTemplate(request);
      if (route.startsWith('/health') || route === '/metrics') return;
      const method = request.method;
      metrics.httpRequests.inc({ method, route, status_code: String(response.statusCode) });
      metrics.httpDuration.observe({ method, route }, seconds);
      const error = record(body?.['error']);
      const requestBody = record(request.body);
      const status = string(body?.['status']);
      const replay = typeof body?.['idempotentReplay'] === 'boolean' ? body['idempotentReplay'] as boolean : undefined;
      const kind = route === '/wagering/transactions' && method === 'POST' ? wagerKind(requestBody) : undefined;
      if (kind && status) {
        metrics.transactions.inc({ channel: 'http', status, kind });
        metrics.processing.observe({ channel: 'http', outcome: 'completed' }, seconds);
        if (replay) metrics.duplicates.inc({ layer: 'idempotency' });
      }
      // Mesma série do canal SQS: falha SQL transitória que esgotou os retries e virou 503.
      const sqlReason = response.statusCode === 503 ? sqlFailureReason(recordedInfrastructureFailure(response)) : undefined;
      if (sqlReason) metrics.sqlTransient.inc({ channel: 'http', reason: sqlReason });
      const level = response.statusCode >= 500 ? 'error' : response.statusCode >= 400 ? 'warn' : 'info';
      logger.log(level, 'http.request', {
        method, route, httpStatus: response.statusCode, durationMs: Math.round(seconds * 1000),
        correlationId: assignedCorrelationId(response) ?? string(body?.['correlationId']), errorCode: string(error?.['code']),
        transactionId: string(body?.['transactionId']), status, kind, reason: sqlReason, failureCode: string(body?.['failureCode']), idempotentReplay: replay,
        walletId: string(request.params?.['walletId']) ?? string(requestBody?.['walletId'])
          ?? (route === '/wallets' && method === 'POST' ? string(body?.['id']) : undefined),
        providerId: string(request.params?.['providerId']) ?? string(requestBody?.['providerId']),
      });
    });
    next();
  };
}


export function installHttpObservability(app: INestApplication, observability: Observability): void {
  app.use(httpObservability(observability));
}
