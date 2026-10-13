import { randomUUID } from 'node:crypto';
import { InfrastructureUnavailableError, isTransientInfrastructureFailure } from '../infrastructure/sql-transaction.js';

export interface HttpResponse { status(code: number): HttpResponse; json(body: unknown): void; locals?: Record<string, unknown> }
export interface TypedHttpError { readonly code: string; readonly httpStatus: number }
export interface HttpResult { readonly httpStatus: number; readonly body: unknown }

const CORRELATION_ID = 'jwpCorrelationId';
/** Um correlationId compartilhado liga caso de uso, eventos, erro e log da mesma requisição. */
export function requestCorrelationId(response: { locals?: Record<string, unknown> }): string {
  const current = response.locals?.[CORRELATION_ID];
  if (typeof current === 'string') return current;
  const correlationId = randomUUID();
  if (response.locals) response.locals[CORRELATION_ID] = correlationId;
  return correlationId;
}
export function assignedCorrelationId(response: { locals?: Record<string, unknown> }): string | undefined {
  const current = response.locals?.[CORRELATION_ID];
  return typeof current === 'string' ? current : undefined;
}

/** Erros tipados preservam status; transientes viram 503 retryable. Erro inesperado continua 500, sem virar resultado de negócio. */
export async function respond(response: HttpResponse, run: (correlationId: string) => Promise<HttpResult>,
  typed: (error: unknown) => TypedHttpError | undefined): Promise<void> {
  const correlationId = requestCorrelationId(response);
  let result: HttpResult;
  try {
    result = await run(correlationId);
  } catch (error) {
    const known = typed(error) ?? (error instanceof InfrastructureUnavailableError || isTransientInfrastructureFailure(error)
      ? { code: 'INFRASTRUCTURE_UNAVAILABLE', httpStatus: 503 } : undefined);
    if (!known) throw error;
    response.status(known.httpStatus).json({ error: { code: known.code, message: known.code,
      retryable: known.code === 'INFRASTRUCTURE_UNAVAILABLE' }, correlationId });
    return;
  }
  response.status(result.httpStatus).json(result.body);
}
