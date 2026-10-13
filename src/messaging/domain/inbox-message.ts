import { isOpaqueIdentifier } from '../../shared/domain/identifiers.js';
export interface InboxMessageState {
  readonly messageId: string;
  readonly consumerName: string;
  readonly payloadHash: string;
  readonly receivedAt: Date;
  readonly processedAt?: Date;
}
export type ReceiveInboxProps = Omit<InboxMessageState, 'processedAt'>;
const timestamp = (d: Date): number => {
  if (!(d instanceof Date) || !Number.isFinite(d.getTime())) throw new Error('INVALID_INBOX_TIME');
  return d.getTime();
};
export class InboxMessage {
  readonly messageId: string;
  readonly consumerName: string;
  readonly payloadHash: string;
  readonly #receivedAt: number;
  #processedAt: number | undefined;
  private constructor(p: InboxMessageState) {
    this.messageId=p.messageId; this.consumerName=p.consumerName; this.payloadHash=p.payloadHash;
    this.#receivedAt=timestamp(p.receivedAt); this.#processedAt=p.processedAt===undefined?undefined:timestamp(p.processedAt); Object.freeze(this);
  }
  static receive(p: ReceiveInboxProps): InboxMessage {
    if (!isOpaqueIdentifier(p.messageId) || !isOpaqueIdentifier(p.consumerName) || !/^[a-f0-9]{64}$/.test(p.payloadHash)) throw new Error('INVALID_INBOX_MESSAGE');
    return new InboxMessage(p);
  }
  static rehydrate(p: InboxMessageState): InboxMessage {
    // Checa corrupção persistida sem reaplicar markProcessed na reconstrução.
    if (!isOpaqueIdentifier(p.messageId) || !isOpaqueIdentifier(p.consumerName) || !/^[a-f0-9]{64}$/.test(p.payloadHash)) throw new Error('INVALID_INBOX_MESSAGE');
    if (p.processedAt!==undefined && timestamp(p.processedAt)<timestamp(p.receivedAt)) throw new Error('INVALID_INBOX_TIME');
    return new InboxMessage(p);
  }
  get receivedAt(): Date { return new Date(this.#receivedAt); }
  get processedAt(): Date | undefined { return this.#processedAt===undefined?undefined:new Date(this.#processedAt); }
  isProcessed(): boolean { return this.#processedAt!==undefined; }
  markProcessed(at: Date): void {
    const time=timestamp(at); if(time<this.#receivedAt) throw new Error('INVALID_INBOX_TIME');
    // Redelivery não pode alterar o instante do processamento original.
    this.#processedAt ??= time;
  }
}
