import { afterEach, expect, test } from 'bun:test';
import {
  CURRENCY_REGISTRY, CurrencyConfigError, type CurrencyConfigErrorCode, configureCurrencies, currencyScale, enabledCurrencies, isEnabledCurrency,
  isKnownCurrency, parseEnabledCurrencies, SUPPORTED_MONEY_SCALE, validateCurrencyRegistry,
} from '../../src/shared/domain/currency.js';
import { Money } from '../../src/shared/domain/money.js';

afterEach(() => { configureCurrencies({}); });

const failure = (run: () => unknown): CurrencyConfigError => {
  try { run(); } catch (error) { if (error instanceof CurrencyConfigError) return error; throw error; }
  throw new Error('expected CurrencyConfigError');
};

test('registro traz BRL, USD e EUR com escala 2 e fica imutável', () => {
  expect(CURRENCY_REGISTRY.map(definition => [definition.code, definition.scale])).toEqual([['BRL', 2], ['USD', 2], ['EUR', 2]]);
  expect(SUPPORTED_MONEY_SCALE).toBe(2);
  expect(Object.isFrozen(CURRENCY_REGISTRY)).toBe(true);
  expect(CURRENCY_REGISTRY.every(definition => Object.isFrozen(definition))).toBe(true);
  for (const code of ['BRL', 'USD', 'EUR']) expect(currencyScale(code)).toBe(2);
  expect(() => currencyScale('BTC')).toThrow('UNSUPPORTED_CURRENCY');
});

test('registro com escala diferente de 2, código inválido, duplicado ou vazio falha no boot', () => {
  expect(failure(() => validateCurrencyRegistry([{ code: 'BTC', scale: 8 }])).code).toBe('UNSUPPORTED_CURRENCY_SCALE');
  expect(failure(() => validateCurrencyRegistry([{ code: 'JPY', scale: 0 }])).code).toBe('UNSUPPORTED_CURRENCY_SCALE');
  expect(failure(() => validateCurrencyRegistry([{ code: 'BRL', scale: 2.5 }])).code).toBe('UNSUPPORTED_CURRENCY_SCALE');
  for (const code of ['brl', 'BR', 'BRLX', ' BRL', ''])
    expect(failure(() => validateCurrencyRegistry([{ code, scale: 2 }])).code).toBe('INVALID_CURRENCY_CODE');
  expect(failure(() => validateCurrencyRegistry([{ code: 'BRL', scale: 2 }, { code: 'BRL', scale: 2 }])).code).toBe('DUPLICATE_CURRENCY');
  expect(failure(() => validateCurrencyRegistry([])).code).toBe('EMPTY_CURRENCY_REGISTRY');
  expect(failure(() => validateCurrencyRegistry([{ code: 'BTC', scale: 8 }])).message).toBe('UNSUPPORTED_CURRENCY_SCALE (index 0, length 3)');
});

test('allowlist ausente ou vazia habilita o registro inteiro', () => {
  for (const raw of [undefined, '', '  ']) expect(parseEnabledCurrencies(raw)).toEqual(['BRL', 'USD', 'EUR']);
  expect(configureCurrencies({})).toEqual(['BRL', 'USD', 'EUR']);
  expect(enabledCurrencies()).toEqual(['BRL', 'USD', 'EUR']);
});

test('allowlist válida restringe novas entradas sem impedir reidratação de moeda do registro', () => {
  expect(configureCurrencies({ JWP_ENABLED_CURRENCIES: ' BRL , EUR ' })).toEqual(['BRL', 'EUR']);
  expect(isEnabledCurrency('BRL')).toBe(true);
  expect(isEnabledCurrency('USD')).toBe(false);
  expect(isKnownCurrency('USD')).toBe(true);
  expect(Money.rehydrate({ amount: '1.00', currency: 'USD' }).toJSON()).toEqual({ amount: '1.00', currency: 'USD' });
});

test('allowlist com moeda desconhecida, minúscula, vazia no meio ou duplicada falha sem ecoar valor inválido', () => {
  const cases: [string, CurrencyConfigErrorCode, number, number][] = [
    ['BRL,GBP', 'UNKNOWN_CURRENCY', 1, 3],
    ['BTC', 'UNKNOWN_CURRENCY', 0, 3],
    ['brl', 'INVALID_CURRENCY_CODE', 0, 3],
    ['BRL,,USD', 'INVALID_CURRENCY_CODE', 1, 0],
    ['BRL,BRL', 'DUPLICATE_CURRENCY', 1, 3],
    ['postgres://user:pass@host', 'INVALID_CURRENCY_CODE', 0, 25],
  ];
  for (const [raw, code, index, length] of cases) {
    const error = failure(() => configureCurrencies({ JWP_ENABLED_CURRENCIES: raw }));
    expect(error.code).toBe(code);
    expect(error.message).toBe(`${code} (index ${index}, length ${length})`);
    expect({ index: error.index, length: error.length }).toEqual({ index, length });
    // Nenhuma propriedade, nem a serialização do erro, carrega o valor cru ou um token dele.
    expect('currency' in error).toBe(false);
    const dump = JSON.stringify({ ...error, message: error.message }) + Bun.inspect(error);
    for (const token of raw.split(',').filter(t => t !== '')) expect(dump).not.toContain(token);
  }
  // Configuração recusada não altera a allowlist ativa.
  expect(enabledCurrencies()).toEqual(['BRL', 'USD', 'EUR']);
});

test('registro inválido falha só com código, índice e tamanho', () => {
  const invalid = failure(() => validateCurrencyRegistry([{ code: 'segredo-xyz', scale: 2 }]));
  expect(invalid.message).toBe('INVALID_CURRENCY_CODE (index 0, length 11)');
  expect(Bun.inspect(invalid)).not.toContain('segredo-xyz');
  expect(failure(() => validateCurrencyRegistry([{ code: 'BRL', scale: 2 }, { code: 'BRL', scale: 2 }])).message)
    .toBe('DUPLICATE_CURRENCY (index 1, length 3)');
  expect(failure(() => validateCurrencyRegistry([{ code: 'BRL', scale: 8 }])).code).toBe('UNSUPPORTED_CURRENCY_SCALE');
  expect(failure(() => validateCurrencyRegistry([])).message).toBe('EMPTY_CURRENCY_REGISTRY');
});

test('Money aceita só moedas do registro, sensível à caixa', () => {
  for (const code of ['BRL', 'USD', 'EUR']) expect(Money.from({ amount: '0.01', currency: code }).currency).toBe(code);
  for (const code of ['brl', 'Usd', 'XYZ', 'BTC', '', 'BR L']) expect(() => Money.from({ amount: '0.01', currency: code })).toThrow('UNSUPPORTED_CURRENCY');
});

const bootWith = async (allowlist: string) => {
  const started = Date.now();
  const child = Bun.spawn(['bun', 'src/main.ts'], {
    cwd: new URL('../..', import.meta.url).pathname, stdout: 'pipe', stderr: 'pipe',
    env: { PATH: process.env['PATH'] ?? '', HOME: process.env['HOME'] ?? '', JWP_DATABASE_URL: 'postgresql://boot:check@127.0.0.1:1/jwp',
      PORT: '0', JWP_ENABLED_CURRENCIES: allowlist },
  });
  const [code, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
  return { code, output: stdout + stderr, elapsed: Date.now() - started };
};

test('boot encerra antes de abrir o banco e não ecoa a allowlist inválida', async () => {
  // Sentinel com cara de URL de banco: nem ele nem pedaços dele podem aparecer na saída.
  const sentinel = 'postgres://jwpsentinel063:s3gr3d0@sentinel-host.invalid:5432/sentineldb';
  const cases: [string, string][] = [
    [sentinel, 'INVALID_CURRENCY_CODE'], ['brl', 'INVALID_CURRENCY_CODE'], ['BRL,BTC', 'UNKNOWN_CURRENCY'],
    ['BRL,,USD', 'INVALID_CURRENCY_CODE'], ['BRL,BRL', 'DUPLICATE_CURRENCY'],
  ];
  for (const [raw, code] of cases) {
    const result = await bootWith(raw);
    expect(result.code).not.toBe(0);
    expect(result.elapsed).toBeLessThan(10_000);
    expect(result.output).toContain(code);
    expect(result.output).not.toContain(raw);
    expect(result.output).not.toContain('boot:check');
    expect(result.output).not.toContain('ECONNREFUSED');
    expect(result.output).not.toMatch(/currency:/);
  }
  const sentinelRun = await bootWith(sentinel);
  for (const piece of ['jwpsentinel063', 's3gr3d0', 'sentinel-host', 'sentineldb']) expect(sentinelRun.output).not.toContain(piece);
  expect((await bootWith('brl')).output).not.toContain('brl');
  expect((await bootWith('BTC')).output).not.toContain('BTC');
}, 60_000);
