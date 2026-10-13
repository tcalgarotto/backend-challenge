import type { IntegrationEvent } from '../../shared/events/integration-event.js';
import { isExactUuid, isOpaqueIdentifier } from '../../shared/domain/identifiers.js';
export interface OutboxMessageState {
  readonly id: string;
  readonly aggregateId: string;
  readonly eventType: string;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly occurredAt: Date;
  readonly attempts: number;
  readonly nextAttemptAt?: Date;
  readonly publishedAt?: Date;
}
const time=(at: Date): number => {
  if(!(at instanceof Date)||!Number.isFinite(at.getTime())) throw new Error('INVALID_OUTBOX_TIME');return at.getTime();
};
function freeze<T>(value: T): T {
  if(value && typeof value==='object') {for(const v of Object.values(value)) freeze(v);Object.freeze(value);}return value;
}
export class OutboxMessage {
  readonly id: string;
  readonly aggregateId: string;
  readonly eventType: string;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly #occurredAt: number;
  #attempts: number;
  #nextAttemptAt: number | undefined;
  #publishedAt: number | undefined;
  private constructor(p: OutboxMessageState) {
    this.id=p.id;this.aggregateId=p.aggregateId;this.eventType=p.eventType;
    this.payload=freeze(structuredClone(p.payload));this.#occurredAt=time(p.occurredAt);this.#attempts=p.attempts;
    this.#nextAttemptAt=p.nextAttemptAt===undefined?undefined:time(p.nextAttemptAt);
    this.#publishedAt=p.publishedAt===undefined?undefined:time(p.publishedAt);Object.freeze(this);
  }
  static enqueue(event: IntegrationEvent<object>): OutboxMessage {
    return this.rehydrate({id:event.eventId,aggregateId:event.aggregateId,eventType:event.eventType,
      payload:event.toJSON(),occurredAt:event.occurredAt,attempts:0,nextAttemptAt:event.occurredAt});
  }
  static rehydrate(p: OutboxMessageState): OutboxMessage {
    if(!isExactUuid(p.id)||!isExactUuid(p.aggregateId)||!isOpaqueIdentifier(p.eventType)
      || !p.payload || typeof p.payload!=='object' || Array.isArray(p.payload)
      || !Number.isSafeInteger(p.attempts)||p.attempts<0) throw new Error('INVALID_OUTBOX_MESSAGE');
    const message=new OutboxMessage(p);
    if((message.#nextAttemptAt!==undefined && message.#nextAttemptAt<message.#occurredAt)
      ||(message.#publishedAt!==undefined && message.#publishedAt<message.#occurredAt)) throw new Error('INVALID_OUTBOX_TIME');
    return message;
  }
  get occurredAt(): Date {return new Date(this.#occurredAt);}
  get attempts(): number {return this.#attempts;}
  get nextAttemptAt(): Date | undefined {return this.#nextAttemptAt===undefined?undefined:new Date(this.#nextAttemptAt);}
  get publishedAt(): Date | undefined {return this.#publishedAt===undefined?undefined:new Date(this.#publishedAt);}
  isPending(): boolean {return this.#publishedAt===undefined;}
  isDue(now: Date): boolean {return this.isPending() && (this.#nextAttemptAt??this.#occurredAt)<=time(now);}
  markPublished(at: Date): void {
    const t=time(at);if(t<this.#occurredAt) throw new Error('INVALID_OUTBOX_TIME');
    this.#publishedAt??=t;
  }
  scheduleRetry(now: Date, policy: {baseMs: number;maxMs: number}={baseMs:1000,maxMs:60000}): void {
    const t=time(now);
    if(!this.isPending()||t<this.#occurredAt||!Number.isSafeInteger(policy.baseMs)||!Number.isSafeInteger(policy.maxMs)
      ||policy.baseMs<1||policy.maxMs<policy.baseMs||this.#attempts===Number.MAX_SAFE_INTEGER) throw new Error('INVALID_OUTBOX_RETRY');
    const delay=Math.min(policy.maxMs,policy.baseMs*2**Math.min(this.#attempts,30));
    const next=t+delay;
    if(!Number.isFinite(new Date(next).getTime())) throw new Error('INVALID_OUTBOX_TIME');
    this.#attempts++;this.#nextAttemptAt=next;
  }
}
