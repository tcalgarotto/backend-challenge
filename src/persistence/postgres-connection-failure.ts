import type { EventEmitter } from 'node:events';

// O pg emite no cliente o mesmo erro das queries interrompidas pelo socket.
// Essa origem distingue falha de transporte de erro da aplicação sem código,
// sem depender do texto da mensagem.
const connectionFailures = new WeakSet<Error>();
const transportOrigin = Object.freeze({});
const transportOriginField = '__jwpPostgresTransportOrigin';

export function observePostgresConnection(
  connection: EventEmitter,
  ready: (error: Error | null, connection: EventEmitter) => void,
): void {
  connection.on('error', (error: unknown) => {
    if (error instanceof Error) {
      connectionFailures.add(error);
      // DriverException copia propriedades próprias, mas perde a identidade do erro pg.
      if (Object.isExtensible(error)) Object.defineProperty(error, transportOriginField, { value: transportOrigin });
    }
  });
  ready(null, connection);
}

export function isPostgresConnectionFailure(error: unknown): boolean {
  return error instanceof Error && (connectionFailures.has(error)
    || (error as unknown as Record<string, unknown>)[transportOriginField] === transportOrigin)
    && (error as { code?: unknown }).code === undefined;
}

// Se o socket do pool morreu ocioso, BEGIN falha e o knex encerra a TX;
// a causa do driver fica em executionPromise, enquanto queries posteriores
// e rollback lançam erro genérico. A classificação deve usar a causa preservada.

export async function transactionFailureCause(trx: unknown): Promise<unknown> {
  const t = trx as { isCompleted?: () => boolean; executionPromise?: Promise<unknown> } | undefined;
  if (!t?.isCompleted?.() || !t.executionPromise) return undefined;
  // Promise resolvida vence na fila de microtasks; nunca aguardar uma ainda pendente.
  return Promise.race([
    t.executionPromise.then(() => undefined, (cause: unknown) => cause),
    new Promise<undefined>(resolve => setImmediate(resolve)),
  ]);
}
