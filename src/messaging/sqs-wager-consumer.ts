import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { InfrastructureUnavailableError, isTransientInfrastructureFailure } from '../shared/infrastructure/sql-transaction.js';
import { ProcessWagerError, type ProcessWagerResult } from '../wagering/application/process-wager.js';
import { WAGER_INBOX_CONSUMER, type InboxMessageMetadata } from './sql-inbox-process-wager-store.js';
import { InvalidWagerMessageError, messageCommand, parseWagerMessageBody } from './wager-message.js';

export type MessageDisposition = 'ACKNOWLEDGED' | 'RETRY' | 'PERMANENT';

export interface ConsumerObservation {
  readonly messageId?: string;
  readonly disposition: MessageDisposition;
  readonly receiveCount: number;
  readonly errorCode?: string;
}

export interface SqsWagerConsumerOptions {
  readonly waitTimeSeconds: number;
  readonly maxMessages: number;
  /** Falha permanente volta logo à fila para alcançar a DLQ por redrive. */
  readonly retryVisibilitySeconds: number;
  /** Falha transitória usa backoff exponencial limitado por retryMaxSeconds. */
  readonly retryBaseSeconds: number;
  readonly retryMaxSeconds: number;
  /** Heartbeat mantém invisível a mensagem ainda não resolvida. */
  readonly visibilityTimeoutSeconds: number;
  readonly heartbeatIntervalMs: number;
  readonly pollErrorDelayMs: number;
  readonly shutdownGraceMs: number;
  readonly consumerName: string;
}

// Com cinco recebimentos, as tentativas transitórias ocorrem aproximadamente
// em 0, 3, 9, 21 e 45 s. A dependência precisa voltar até a quinta tentativa;
// se ela falhar, a visibilidade de 48 s vence e o broker envia à DLQ
// em cerca de 93 s. Lock de wallet quente retorna à tentativa após 3 s.

export const defaultSqsWagerConsumerOptions: SqsWagerConsumerOptions = Object.freeze({
  waitTimeSeconds: 20,
  maxMessages: 10,
  retryVisibilitySeconds: 1,
  retryBaseSeconds: 3,
  retryMaxSeconds: 60,
  visibilityTimeoutSeconds: 30,
  heartbeatIntervalMs: 10_000,
  pollErrorDelayMs: 250,
  shutdownGraceMs: 15_000,
  consumerName: WAGER_INBOX_CONSUMER,
});

/** SQS limita a visibilidade a 12 horas. */
const MAXIMUM_VISIBILITY_SECONDS = 43_200;

export function transientRetryVisibilitySeconds(receiveCount: number, baseSeconds: number, maxSeconds: number): number {
  const attempt = Number.isSafeInteger(receiveCount) && receiveCount > 0 ? receiveCount : 1;
  return Math.min(maxSeconds, baseSeconds * 2 ** Math.min(attempt - 1, 20));
}

export interface WagerMessageProcessor {
  execute(input: unknown, idempotencyKey: unknown, metadata: InboxMessageMetadata): Promise<ProcessWagerResult>;
}

const errorCode = (error: unknown): string | undefined => {
  const code = (error as { code?: unknown } | null | undefined)?.code;
  return typeof code === 'string' ? code : undefined;
};
const transient = (error: unknown): boolean =>
  error instanceof InfrastructureUnavailableError ||
  error instanceof ProcessWagerError && error.code === 'INFRASTRUCTURE_UNAVAILABLE' ||
  isTransientInfrastructureFailure(error);
const wholeSeconds = (value: number, minimum: number, maximum: number): boolean =>
  Number.isInteger(value) && value >= minimum && value <= maximum;

interface Heartbeat {
  readonly timer: ReturnType<typeof setInterval>;
  pending: Promise<void>;
}

export class SqsWagerConsumer {
  private readonly options: SqsWagerConsumerOptions;
  private stopping = false;
  private readonly unfinished = new Set<Message>();
  private readonly heartbeats = new Map<Message, Heartbeat>();
  private receiveAbort: AbortController | undefined;
  private redriveLoaded = false;
  private redriveLimit: number | undefined;
  private inFlight: { readonly message: Message; readonly settled: Promise<void> } | undefined;

  constructor(
    private readonly client: SQSClient,
    private readonly queueUrl: string,
    private readonly processWager: WagerMessageProcessor,
    options: Partial<SqsWagerConsumerOptions> = {},
  ) {
    const configured = { ...defaultSqsWagerConsumerOptions, ...options };
    if (!wholeSeconds(configured.waitTimeSeconds, 0, 20) ||
        !wholeSeconds(configured.maxMessages, 1, 10) ||
        !wholeSeconds(configured.retryVisibilitySeconds, 0, MAXIMUM_VISIBILITY_SECONDS) ||
        !wholeSeconds(configured.retryBaseSeconds, 0, MAXIMUM_VISIBILITY_SECONDS) ||
        !wholeSeconds(configured.retryMaxSeconds, configured.retryBaseSeconds, MAXIMUM_VISIBILITY_SECONDS) ||
        !wholeSeconds(configured.visibilityTimeoutSeconds, 1, MAXIMUM_VISIBILITY_SECONDS) ||
        !Number.isInteger(configured.heartbeatIntervalMs) || configured.heartbeatIntervalMs < 1 ||
        configured.heartbeatIntervalMs >= configured.visibilityTimeoutSeconds * 1000 ||
        !Number.isInteger(configured.pollErrorDelayMs) || configured.pollErrorDelayMs < 0 ||
        !Number.isInteger(configured.shutdownGraceMs) || configured.shutdownGraceMs < 1 ||
        configured.consumerName.trim().length === 0) {
      throw new Error('Invalid SQS wager consumer options.');
    }
    this.options = Object.freeze(configured);
  }

  /** Sem política de DLQ não existe recebimento final conhecido. */
  private async loadRedriveLimit(signal: AbortSignal): Promise<void> {
    if(this.redriveLoaded) return;
    const result=await this.client.send(new GetQueueAttributesCommand({QueueUrl:this.queueUrl,
      AttributeNames:['RedrivePolicy']}),{abortSignal:signal});
    const raw=result.Attributes?.['RedrivePolicy'];
    if(raw) {
      const count=Number((JSON.parse(raw) as {maxReceiveCount?:unknown}).maxReceiveCount);
      if(!Number.isSafeInteger(count)||count<1||count>1000) throw new Error('INVALID_REDRIVE_LIMIT');
      this.redriveLimit=count;
    }
    this.redriveLoaded=true;
  }

  async pollOnce(): Promise<ConsumerObservation[]> {
    if (this.stopping) return [];
    const abort = new AbortController();
    this.receiveAbort = abort;
    let messages: Message[];
    try {
      await this.loadRedriveLimit(abort.signal);
      if (this.stopping) return [];
      const response = await this.client.send(new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: this.options.maxMessages,
        WaitTimeSeconds: this.options.waitTimeSeconds,
        MessageSystemAttributeNames: ['ApproximateReceiveCount'],
      }), { abortSignal: abort.signal });
      messages = response.Messages ?? [];
    } finally {
      if (this.receiveAbort === abort) this.receiveAbort = undefined;
    }

    // Cada receipt permanece invisível durante a espera no lote e o processamento.
    for (const message of messages) {
      this.unfinished.add(message);
      this.startHeartbeat(message);
    }
    const observations: ConsumerObservation[] = [];
    try {
      await this.consumeBatch(messages, observations);
    } catch (error) {
      // Falha ao devolver ou dar ack aborta o lote; devolver os receipts pendentes
      // impede heartbeat eterno que bloquearia o grupo FIFO e o redrive.
      await this.releaseUnfinished(messages);
      throw error;
    }
    return observations;
  }

  private async releaseUnfinished(messages: Message[]): Promise<void> {
    const pending = messages.filter(message => this.unfinished.has(message));
    await Promise.all(pending.map(message => this.stopHeartbeat(message)));
    await Promise.allSettled(pending.map(message => this.returnVisibility(message)));
    for (const message of pending) this.unfinished.delete(message);
  }

  private async consumeBatch(messages: Message[], observations: ConsumerObservation[]): Promise<void> {
    for (const message of messages) {
      if (this.stopping) {
        await this.stopHeartbeat(message);
        await this.returnVisibility(message);
        this.unfinished.delete(message);
        continue;
      }
      let resolveSettled!: () => void;
      const settled = new Promise<void>(resolve => { resolveSettled = resolve; });
      this.inFlight = { message, settled };
      try {
        observations.push(await this.consume(message));
      } finally {
        await this.stopHeartbeat(message);
        this.unfinished.delete(message);
        resolveSettled();
        if (this.inFlight?.message === message) this.inFlight = undefined;
      }
    }
  }

  async run(): Promise<void> {
    this.stopping = false;
    while (!this.stopping) {
      try {
        await this.pollOnce();
      } catch (error) {
        if (this.stopping && (error as { name?: string }).name === 'AbortError') break;
        if (this.stopping) break;
        await Bun.sleep(this.options.pollErrorDelayMs);
      }
    }
  }

  async stop(graceMs = this.options.shutdownGraceMs): Promise<'DRAINED' | 'VISIBILITY_RETURNED'> {
    this.stopping = true;
    this.receiveAbort?.abort();
    const active = this.inFlight;
    if (active) {
      await Promise.race([active.settled, Bun.sleep(graceMs)]);
    }
    const unfinished = [...this.unfinished];
    if (unfinished.length === 0) return 'DRAINED';
    // Após devolver o receipt, o heartbeat não pode ocultá-lo novamente.
    await Promise.all(unfinished.map(message => this.stopHeartbeat(message)));
    // Devolve todos os receipts recebidos antes de permitir a saída do processo.
    const returned = await Promise.allSettled(unfinished.map(message => this.returnVisibility(message)));
    const failed = returned.find(result => result.status === 'rejected');
    if (failed?.status === 'rejected') throw failed.reason;
    return 'VISIBILITY_RETURNED';
  }

  installSigtermHandler(onStopped: (result: 'DRAINED' | 'VISIBILITY_RETURNED') => void | Promise<void> = () => undefined): () => void {
    let handling = false;
    const handler = () => {
      if (handling) return;
      handling = true;
      void this.stop().then(onStopped);
    };
    process.once('SIGTERM', handler);
    return () => process.off('SIGTERM', handler);
  }

  private async consume(message: Message): Promise<ConsumerObservation> {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
    let logicalMessageId: string | undefined;
    try {
      if (!message.ReceiptHandle) throw new InvalidWagerMessageError();
      // MessageGroupId define ordem pelo produtor; a correção não depende do grupo,
      // por isso qualquer grupo é aceito. Nossos produtores usam walletId.
      const envelope = parseWagerMessageBody(message.Body);
      logicalMessageId = envelope.messageId;
      const command = messageCommand(envelope);
      await this.processWager.execute(command.input, command.idempotencyKey, {
        consumerName: this.options.consumerName,
        messageId: envelope.messageId,
        envelopeHash: command.envelopeHash,
        receiveCount, finalAttempt:this.redriveLimit!==undefined && receiveCount>=this.redriveLimit,
      });
    } catch (error) {
      const retry = transient(error);
      await this.stopHeartbeat(message);
      await this.defer(message, retry
        ? transientRetryVisibilitySeconds(receiveCount, this.options.retryBaseSeconds, this.options.retryMaxSeconds)
        : this.options.retryVisibilitySeconds);
      return {
        ...(logicalMessageId ? { messageId: logicalMessageId } : {}),
        disposition: retry ? 'RETRY' : 'PERMANENT',
        receiveCount,
        ...(errorCode(error) ? { errorCode: errorCode(error)! } : {}),
      };
    }
    await this.stopHeartbeat(message);
    try {
      await this.client.send(new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle }));
      return { messageId: logicalMessageId, disposition: 'ACKNOWLEDGED', receiveCount };
    } catch (error) {
      // SQL já confirmou. Deixar sem ack é seguro porque a inbox torna
      // a próxima entrega sem efeito financeiro adicional.
      return {
        ...(logicalMessageId ? { messageId: logicalMessageId } : {}),
        disposition: 'RETRY',
        receiveCount,
        ...(errorCode(error) ? { errorCode: errorCode(error)! } : {}),
      };
    }
  }

  /** Falha do heartbeat permite nova tentativa ou redelivery por visibilidade. */
  private startHeartbeat(message: Message): void {
    if (!message.ReceiptHandle || this.heartbeats.has(message)) return;
    const timer = setInterval(() => {
      beat.pending = beat.pending.then(() => this.client.send(new ChangeMessageVisibilityCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: this.options.visibilityTimeoutSeconds,
      }))).then(() => undefined, () => undefined);
    }, this.options.heartbeatIntervalMs);
    timer.unref?.();
    const beat: Heartbeat = { timer, pending: Promise.resolve() };
    this.heartbeats.set(message, beat);
  }

  /** Espera heartbeat em voo para não sobrescrever a visibilidade final. */
  private async stopHeartbeat(message: Message): Promise<void> {
    const beat = this.heartbeats.get(message);
    if (!beat) return;
    clearInterval(beat.timer);
    this.heartbeats.delete(message);
    await beat.pending;
  }

  private async defer(message: Message, visibilitySeconds: number): Promise<void> {
    if (!message.ReceiptHandle) return;
    await this.client.send(new ChangeMessageVisibilityCommand({
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle,
      VisibilityTimeout: visibilitySeconds,
    }));
  }

  private async returnVisibility(message: Message): Promise<void> {
    if (!message.ReceiptHandle) return;
    await this.client.send(new ChangeMessageVisibilityCommand({
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle,
      VisibilityTimeout: 0,
    }));
  }
}
