import { expect, test } from 'bun:test';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';

// Evidências versionadas são públicas: um argv capturado do host já expôs uma chave de CLI.
// Só padrões de alta confiança, para não confundir senhas fixas das stacks descartáveis.
const patterns: [string, RegExp][] = [
  ['provider-token', /\b(?:crsr_|github_pat_|gh[pousr]_|sk-(?:proj-|ant-)?|xox[baprs]-)[A-Za-z0-9_-]{16,}/],
  ['aws-access-key', /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/],
  ['jwt', /\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/],
  ['private-key', /-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----/],
  ['bearer', /\bBearer\s+[A-Za-z0-9_+/.=-]{20,}/],
  ['api-key-option', /--api[-_]?key[= ]+(?!\[REDACTED\])[A-Za-z0-9_+/.=-]{16,}/i],
];

export function credentialFindings(text: string): string[] {
  return patterns.filter(([, pattern]) => pattern.test(text)).map(([category]) => category);
}

function files(dir: string): string[] {
  return readdirSync(dir).flatMap(name => {
    const path = join(dir, name);
    const info = statSync(path);
    return info.isDirectory() ? files(path) : info.isFile() ? [path] : [];
  });
}

test('detector reconhece formatos de credencial sem depender de valores reais', () => {
  const synthetic = 'crsr' + '_' + 'A1b2C3d4E5f6G7h8J9k0';
  expect(credentialFindings(`agent --api-key ${synthetic}`)).toEqual(['provider-token', 'api-key-option']);
  expect(credentialFindings('agent --api-key [REDACTED]')).toEqual([]);
  expect(credentialFindings('postgresql://jwp:jwp_load_local@127.0.0.1/jwp')).toEqual([]);
});

test('evidências versionadas não contêm credenciais', () => {
  const root = resolve(import.meta.dir, '../..', 'reports');
  if (!existsSync(root)) return;
  const leaks = files(root).flatMap(path => credentialFindings(readFileSync(path, 'latin1'))
    .map(category => `${relative(root, path)}: ${category}`));
  expect(leaks).toEqual([]); // só caminho e categoria; nunca o valor
});
