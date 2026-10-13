export type LogLevel = 'info' | 'warn' | 'error';

/** IDs de correlação pertencem aos logs, nunca aos labels de métricas. */
const IDENTIFIERS = ['correlationId', 'messageId', 'eventId', 'transactionId', 'walletId', 'providerId', 'idempotencyKey'] as const;
/** Códigos, estados e templates de rota limitam os atributos descritivos. */
const ATTRIBUTES = ['status', 'kind', 'disposition', 'errorCode', 'failureCode', 'outcome', 'dependency', 'currency',
  'method', 'route', 'consumer', 'component', 'reason'] as const;
const NUMBERS = ['httpStatus', 'durationMs', 'receiveCount', 'attempts', 'checkedEntries'] as const;
const BOOLEANS = ['idempotentReplay', 'duplicate', 'repaired'] as const;

type Identifier = (typeof IDENTIFIERS)[number];
type Attribute = (typeof ATTRIBUTES)[number];
type NumberField = (typeof NUMBERS)[number];
type BooleanField = (typeof BOOLEANS)[number];

/** Campos permitidos excluem valores, saldos, payloads e credenciais, impedindo vazamento financeiro. */
export type LogFields = { readonly [K in Identifier | Attribute]?: string | undefined }
  & { readonly [K in NumberField]?: number | undefined }
  & { readonly [K in BooleanField]?: boolean | undefined };

const STRING_FIELDS = new Set<string>([...IDENTIFIERS, ...ATTRIBUTES]);
const NUMBER_FIELDS = new Set<string>(NUMBERS);
const BOOLEAN_FIELDS = new Set<string>(BOOLEANS);
const MAX_STRING = 128;
const EVENT_TYPE = /^[a-z][a-z0-9_.]{0,63}$/;
const SENSITIVE_FIELDS = new Set(['authorization', 'proxyauthorization', 'password', 'passwd', 'token',
  'accesstoken', 'refreshtoken', 'secret', 'clientsecret', 'apikey', 'awssecretaccesskey']);
const REDACTED = '[REDACTED]';
/**
 * Exceção explícita: a chave de idempotência não é segredo (o cliente a escolhe e a reenvia)
 * e precisa ficar visível para auditoria e replay; só os nomes exatos, em qualquer caixa, escapam da máscara.
 */
const IDEMPOTENCY_KEY_NAMES = new Set(['idempotencykey']);
const isIdempotencyKeyName = (key: string): boolean => IDEMPOTENCY_KEY_NAMES.has(key.replace(/[_-]/g, '').toLowerCase());

/**
 * Valor inline de chave de idempotência sai literal, sem nenhum padrão de segredo: o cliente pode escolher
 * valores como `password=x`, `Bearer x` ou URLs, que parseWager aceita. Fim do valor: entre aspas, 1–128
 * caracteres (limite de parseWager, cobre espaços e pontuação); sem aspas, prefixo opcional `Bearer `/`Basic `
 * mais até 128 caracteres sem espaço, aspas, `&`, `,` ou `;`. O nome não pode ter prefixo (x-idempotency-key-secret).
 */
const INLINE_IDEMPOTENCY_KEY = /(?<![a-z0-9_.-])idempotency[_-]?key\s*[:=]\s*(?:"[^"]{1,128}"|'[^']{1,128}'|(?:(?:Bearer|Basic) )?[^\s&,;"']{1,128})/gi;

function redactSecrets(value: string): string {
  return value
    .replace(/([a-z][a-z0-9+.-]*:\/\/)[^\s/?#]*@/gi, `$1${REDACTED}@`)
    .replace(/\b(?:Bearer|Basic)\s+[a-z0-9_+/.=-]+/gi, REDACTED)
    // Prefixos e sufixos (AWS_SECRET_ACCESS_KEY, db_password) também carregam credenciais.
    .replace(/\b([a-z0-9_.-]*(?:secret|password|passwd|token|key|credential|auth)[a-z0-9_.-]*)\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^\s&,;]+)/gi,
      (match: string, key: string) => (isIdempotencyKeyName(key) ? match : `${key}=${REDACTED}`));
}

/** Só os trechos fora das chaves de idempotência passam pela máscara; segredos vizinhos continuam ocultos. */
export function redactLogString(value: string): string {
  let out = '', last = 0;
  for (const m of value.matchAll(INLINE_IDEMPOTENCY_KEY)) {
    out += redactSecrets(value.slice(last, m.index)) + m[0];
    last = m.index + m[0].length;
  }
  return out + redactSecrets(value.slice(last));
}

/** Chamadores sem tipagem não ampliam os campos registrados; chaves desconhecidas e tipos inválidos são descartados. */
export function sanitizeLogFields(fields: Readonly<Record<string, unknown>>): Record<string, string | number | boolean> {
  const safe: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (SENSITIVE_FIELDS.has(key.replace(/[_-]/g, '').toLowerCase())) safe[key] = REDACTED;
    // Mascarar antes de truncar impede expor credenciais de URLs longas.
    // Chave de idempotência é valor do cliente, não segredo: literal, como parseWager a aceitou (<= 128).
    else if (isIdempotencyKeyName(key) && typeof value === 'string' && value.length > 0) safe[key] = value.slice(0, MAX_STRING);
    else if (STRING_FIELDS.has(key) && typeof value === 'string' && value.length > 0) safe[key] = redactLogString(value).slice(0, MAX_STRING);
    else if (NUMBER_FIELDS.has(key) && typeof value === 'number' && Number.isFinite(value)) safe[key] = value;
    else if (BOOLEAN_FIELDS.has(key) && typeof value === 'boolean') safe[key] = value;
  }
  return safe;
}

/** Falha do destino de logs não interrompe o fluxo de negócio. */
export class JsonLogger {
  constructor(
    private readonly sink: (line: string) => void = line => { process.stdout.write(line); },
    private readonly clock: () => Date = () => new Date(),
  ) {}

  log(level: LogLevel, type: string, fields: LogFields = {}): void {
    const entry = { level, type: EVENT_TYPE.test(type) ? type : 'invalid_event_type', at: this.clock().toISOString(),
      ...sanitizeLogFields(fields) };
    try { this.sink(`${JSON.stringify(entry)}\n`); } catch { /* Falha de log não interrompe o negócio. */ }
  }

  info(type: string, fields?: LogFields): void { this.log('info', type, fields); }
  warn(type: string, fields?: LogFields): void { this.log('warn', type, fields); }
  error(type: string, fields?: LogFields): void { this.log('error', type, fields); }
}

/** Código tipado identifica falhas; texto da mensagem não vira log ou label. */
export function failureCode(error: unknown): string {
  const code = (error as { code?: unknown } | null | undefined)?.code;
  if (typeof code === 'string' && code.length > 0) return code;
  const name = (error as { name?: unknown } | null | undefined)?.name;
  return typeof name === 'string' && name.length > 0 ? name : 'UNKNOWN';
}
