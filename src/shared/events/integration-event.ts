export interface EventContext {
  readonly eventId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
}
export interface IntegrationEventProps<T> extends EventContext {
  readonly aggregateId: string;
  readonly data: T;
}
export class InvalidEventError extends Error {
  readonly code = 'INVALID_EVENT';
  constructor() {
    super('INVALID_EVENT');
    this.name = 'InvalidEventError';
  }
}
function deepFreeze<T>(value: T): T {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}
/** Identidade e instante vêm da transação, nunca da serialização; retries reutilizam o envelope persistido. */
export abstract class IntegrationEvent<T extends object> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId: string | undefined;
  readonly data: Readonly<T>;
  #occurredAt: string;
  protected constructor(p: IntegrationEventProps<T>) {
    const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
    const text = (s: unknown) => typeof s === 'string' && s.trim().length > 0 && s.length <= 128;
    if (
      !uuid.test(p.eventId) ||
      !uuid.test(p.aggregateId) ||
      !text(p.correlationId) ||
      (p.causationId !== undefined && !text(p.causationId)) ||
      !(p.occurredAt instanceof Date) ||
      !Number.isFinite(p.occurredAt.getTime())
    )
      throw new InvalidEventError();
    this.eventId = p.eventId;
    this.aggregateId = p.aggregateId;
    this.correlationId = p.correlationId;
    this.causationId = p.causationId;
    this.#occurredAt = p.occurredAt.toISOString();
    this.data = deepFreeze(structuredClone(p.data));
  }
  get occurredAt(): Date {
    return new Date(this.#occurredAt);
  }
  toJSON() {
    return Object.freeze({
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId === undefined ? {} : { causationId: this.causationId }),
      occurredAt: this.#occurredAt,
      version: this.version,
      data: this.data,
    });
  }
}
