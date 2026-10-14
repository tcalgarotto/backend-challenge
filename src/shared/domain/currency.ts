import { DomainError } from './domain-error.js';

export interface CurrencyDefinition {
  readonly code: string;
  readonly scale: number;
}

/** Escala única do contrato (CHALLENGE §6.1) e das colunas numeric(20,2); outra escala exige ADR-016. */
export const SUPPORTED_MONEY_SCALE = 2;
/** Variável opcional: subconjunto do registro habilitado para novas operações. */
export const ENABLED_CURRENCIES_VARIABLE = 'JWP_ENABLED_CURRENCIES';

// Registro estático. Moeda nova entra aqui e na CHECK de wallets.currency, por migration nova.
export const CURRENCY_REGISTRY: readonly CurrencyDefinition[] = Object.freeze([
  Object.freeze({ code: 'BRL', scale: 2 }),
  Object.freeze({ code: 'USD', scale: 2 }),
  Object.freeze({ code: 'EUR', scale: 2 }),
]);

export type CurrencyConfigErrorCode = 'INVALID_CURRENCY_CODE' | 'DUPLICATE_CURRENCY' | 'UNSUPPORTED_CURRENCY_SCALE'
  | 'UNKNOWN_CURRENCY' | 'EMPTY_CURRENCY_REGISTRY';

/** Posição segura do item recusado: índice do token (base 0) e tamanho, nunca o texto. */
export interface CurrencyConfigErrorDetail {
  readonly index: number;
  readonly length: number;
}

export class CurrencyConfigError extends Error {
  readonly index?: number;
  readonly length?: number;
  constructor(readonly code: CurrencyConfigErrorCode, detail?: CurrencyConfigErrorDetail) {
    // A mensagem e as propriedades levam só o código e, no máximo, índice e tamanho do token.
    // O valor da variável ou do registro nunca é guardado no erro, para não vazar no stderr do boot.
    super(detail === undefined ? code : `${code} (index ${detail.index}, length ${detail.length})`);
    this.name = 'CurrencyConfigError';
    if (detail !== undefined) {
      this.index = detail.index;
      this.length = detail.length;
    }
  }
}

const isoCode = /^[A-Z]{3}$/;

/** Falha ao carregar o módulo se o registro tiver código inválido, duplicado ou escala diferente de 2. */
export function validateCurrencyRegistry(registry: readonly CurrencyDefinition[]): ReadonlyMap<string, CurrencyDefinition> {
  const definitions = new Map<string, CurrencyDefinition>();
  for (const [index, definition] of registry.entries()) {
    const length = typeof definition.code === 'string' ? definition.code.length : 0;
    if (typeof definition.code !== 'string' || !isoCode.test(definition.code))
      throw new CurrencyConfigError('INVALID_CURRENCY_CODE', { index, length });
    if (definitions.has(definition.code)) throw new CurrencyConfigError('DUPLICATE_CURRENCY', { index, length });
    if (definition.scale !== SUPPORTED_MONEY_SCALE)
      throw new CurrencyConfigError('UNSUPPORTED_CURRENCY_SCALE', { index, length });
    definitions.set(definition.code, definition);
  }
  if (definitions.size === 0) throw new CurrencyConfigError('EMPTY_CURRENCY_REGISTRY');
  return definitions;
}

const known = validateCurrencyRegistry(CURRENCY_REGISTRY);
let enabled: ReadonlySet<string> = new Set(known.keys());

/** Ausente ou vazia habilita o registro inteiro; caixa e espaços internos não são normalizados. */
export function parseEnabledCurrencies(raw: string | undefined,
  registry: ReadonlyMap<string, CurrencyDefinition> = known): readonly string[] {
  if (raw === undefined || raw.trim() === '') return Object.freeze([...registry.keys()]);
  const codes = raw.split(',').map(code => code.trim());
  const seen = new Set<string>();
  for (const [index, code] of codes.entries()) {
    const length = code.length;
    if (!isoCode.test(code)) throw new CurrencyConfigError('INVALID_CURRENCY_CODE', { index, length });
    if (!registry.has(code)) throw new CurrencyConfigError('UNKNOWN_CURRENCY', { index, length });
    if (seen.has(code)) throw new CurrencyConfigError('DUPLICATE_CURRENCY', { index, length });
    seen.add(code);
  }
  return Object.freeze([...seen]);
}

/** Chamado no boot antes de abrir o ORM; registro ou allowlist inválidos encerram o processo. */
export function configureCurrencies(env: Record<string, string | undefined>): readonly string[] {
  const list = parseEnabledCurrencies(env[ENABLED_CURRENCIES_VARIABLE], validateCurrencyRegistry(CURRENCY_REGISTRY));
  enabled = new Set(list);
  return list;
}

/** Moeda do registro: vale para reidratar dados persistidos mesmo fora da allowlist. */
export function isKnownCurrency(currency: unknown): currency is string {
  return typeof currency === 'string' && known.has(currency);
}

/** Moeda aceita em novas entradas (abertura de wallet e comandos de wager). */
export function isEnabledCurrency(currency: unknown): currency is string {
  return typeof currency === 'string' && enabled.has(currency);
}

export function currencyScale(currency: string): number {
  const definition = known.get(currency);
  if (!definition) throw new DomainError('UNSUPPORTED_CURRENCY');
  return definition.scale;
}

export function enabledCurrencies(): readonly string[] {
  return Object.freeze([...enabled]);
}
