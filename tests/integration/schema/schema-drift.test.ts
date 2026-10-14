import { afterAll, beforeAll, expect, test } from 'bun:test';
import type { EntityMetadata } from '@mikro-orm/core';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';

// Guarda de drift: as migrations são SQL manual (JWP-ADR-015); o ORM não gera DDL.
// Compara o schema produzido pelas migrations com o que as entidades declaram.
type Q = (sql: string, args?: unknown[]) => Promise<Record<string, any>[]>;
interface DbColumn { type: string; notNull: boolean; default: string | null }
interface DbTable { columns: Map<string, DbColumn>; primaryKey: string[]; indexes: { columns: string[]; unique: boolean }[];
  foreignKeys: { columns: string[]; table: string }[] }

// Projeções parciais deliberadas: a entidade mapeia só o necessário para o lock.
const PROJECTIONS: Record<string, string> = { wager_transactions: 'LockMode needs only the row identity; financial reads stay in SQL' };

// Exceções de default: cada entrada `tabela.coluna` exige justificativa e menção no JWP-ADR-015.
// Vazia por decisão; nenhum default (nem de PK) é dispensado implicitamente.
export const DEFAULT_EXEMPTIONS: Readonly<Record<string, string>> = {};

// Convenção do projeto: string sem tamanho explícito vira text + CHECK no SQL.
function expectedType(meta: EntityMetadata, name: string): string {
  const prop = meta.properties[name]!;
  if (prop.type === 'string' && prop.length === undefined) return 'text';
  return prop.columnTypes[0]!.toLowerCase();
}
const normType = (t: string) => t.replace('timestamp with time zone', 'timestamptz').replace('character varying', 'varchar');
// Equivalências de palavra-chave aceitas explicitamente: no PostgreSQL CURRENT_TIMESTAMP e now() retornam o
// mesmo instante (início da transação). Qualquer outra forma diferente é divergência.
export const DEFAULT_KEYWORD_EQUIVALENCES: Readonly<Record<string, string>> = { current_timestamp: 'now()' };

// Separa literais '...' (com '' escapado) do restante; índices ímpares são literais.
const splitLiterals = (s: string) => s.split(/('(?:[^']|'')*')/);
// Parênteses externos só saem quando envolvem a expressão inteira, ignorando parênteses dentro de literais.
function wrapsWhole(s: string): boolean {
  if (!s.startsWith('(') || !s.endsWith(')')) return false;
  let depth = 0;
  const code = splitLiterals(s).map((p, i) => i % 2 ? "''" : p).join('');
  for (let i = 0; i < code.length; i++) {
    if (code[i] === '(') depth++;
    else if (code[i] === ')' && --depth === 0 && i < code.length - 1) return false;
  }
  return depth === 0;
}

// Normaliza só a sintaxe do catálogo: cast final (::text, ::character varying(3), ::bigint[]) e parênteses
// externos. Fora de literais, minúsculas e espaços colapsados (palavras-chave/funções não diferenciam caixa).
// O conteúdo de literais nunca muda: 'BRL' ≠ 'brl', 'BR L' ≠ 'BRL'. Não há equivalência numérica nem booleana
// (1 ≠ 1.0 ≠ '1', true ≠ 't'): na dúvida, reporta. DEFAULT NULL equivale a ausência de default.
export function normDefault(d: string | null | undefined): string {
  if (d == null) return 'none';
  let s = String(d).trim();
  for (let prev = ''; prev !== s;) {
    prev = s;
    if (wrapsWhole(s)) s = s.slice(1, -1).trim();
    s = s.replace(/::\s*"?[a-z_][a-z0-9_ ]*"?(\(\s*\d+(\s*,\s*\d+)?\s*\))?(\[\])*\s*$/i, '').trim();
  }
  s = splitLiterals(s).map((p, i) => i % 2 ? p : p.toLowerCase().replace(/\s+/g, ' ').replace(/\s*([(),])\s*/g, '$1')).join('').trim();
  if (s === 'null') return 'none';
  return DEFAULT_KEYWORD_EQUIVALENCES[s] ?? s;
}

export async function introspect(q: Q, table: string): Promise<DbTable | undefined> {
  const [rel] = await q('SELECT to_regclass(?)::oid oid', [`public.${table}`]);
  if (!rel?.['oid']) return undefined;
  const oid = rel['oid'];
  const cols = await q(`SELECT a.attname n, format_type(a.atttypid,a.atttypmod) t, a.attnotnull nn, pg_get_expr(d.adbin,d.adrelid) d
    FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE a.attrelid=? AND a.attnum>0 AND NOT a.attisdropped`, [oid]);
  const keyCols = async (sql: string) => (await q(sql, [oid])).map(r => ({ ...r, columns: r['cols'] as string[] }) as Record<string, any> & { columns: string[] });
  const idx = await keyCols(`SELECT i.indisunique u, i.indisprimary p, i.indpred IS NOT NULL partial,
    array(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(n,o) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.n ORDER BY k.o)::text[] cols
    FROM pg_index i WHERE i.indrelid=?`);
  const fks = await keyCols(`SELECT c.confrelid::regclass::text tbl,
    array(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY k(n,o) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.n ORDER BY k.o)::text[] cols
    FROM pg_constraint c WHERE c.conrelid=? AND c.contype='f'`);
  return {
    columns: new Map(cols.map(c => [c['n'] as string, { type: normType(c['t']), notNull: c['nn'] as boolean, default: c['d'] as string | null }])),
    primaryKey: idx.find(i => i['p'])?.columns ?? [],
    indexes: idx.filter(i => !i['p'] && !i['partial']).map(i => ({ columns: i.columns, unique: i['u'] as boolean })),
    foreignKeys: fks.map(f => ({ columns: f.columns, table: f['tbl'] as string })),
  };
}

export function drift(metas: readonly EntityMetadata[], tables: ReadonlyMap<string, DbTable | undefined>): string[] {
  const out: string[] = [];
  for (const meta of metas) {
    const t = meta.tableName, db = tables.get(t);
    if (!db) { out.push(`${t}: table missing`); continue; }
    const pk = meta.primaryKeys.flatMap(k => meta.properties[k]!.fieldNames);
    if (pk.join(',') !== db.primaryKey.join(',')) out.push(`${t}: primary key entity=${pk} database=${db.primaryKey}`);
    const mapped = new Set<string>();
    for (const prop of meta.props) {
      if (prop.persist === false) continue;
      for (const field of prop.fieldNames) {
        mapped.add(field);
        const col = db.columns.get(field);
        if (!col) { out.push(`${t}.${field}: column missing`); continue; }
        const want = expectedType(meta, prop.name);
        if (prop.kind === 'scalar' && normType(want) !== col.type) out.push(`${t}.${field}: type entity=${want} database=${col.type}`);
        if (!prop.nullable !== col.notNull) out.push(`${t}.${field}: nullable entity=${!!prop.nullable} database=${!col.notNull}`);
        // Defaults comparados em toda coluna, inclusive PK/id; só a lista explícita dispensa.
        const [e, d] = [normDefault(prop.defaultRaw), normDefault(col.default)];
        if (e !== d && !(`${t}.${field}` in DEFAULT_EXEMPTIONS)) out.push(`${t}.${field}: default entity=${e} database=${d}`);
        if (prop.unique && !db.indexes.some(i => i.unique && i.columns.join(',') === field)) out.push(`${t}.${field}: unique missing`);
        if ((prop.kind === 'm:1' || prop.kind === '1:1') && !db.foreignKeys.some(f => f.columns.join(',') === prop.fieldNames.join(',')))
          out.push(`${t}.${field}: foreign key missing`);
      }
    }
    for (const decl of [...meta.indexes.map(i => ({ ...i, unique: false })), ...meta.uniques.map(u => ({ ...u, unique: true }))]) {
      const cols = [decl.properties ?? []].flat().flatMap(p => meta.properties[p as string]?.fieldNames ?? [String(p)]);
      if (!db.indexes.some(i => i.columns.join(',') === cols.join(',') && (!decl.unique || i.unique)))
        out.push(`${t}: ${decl.unique ? 'unique' : 'index'} (${cols}) missing`);
    }
    if (!(t in PROJECTIONS)) for (const c of db.columns.keys()) if (!mapped.has(c)) out.push(`${t}.${c}: column not mapped by entity`);
  }
  return out.sort();
}

let orm: Awaited<ReturnType<typeof openFinancialOrm>>;
let q: Q;
const metas = () => Object.values(orm.getMetadata().getAll()).filter(m => !m.embeddable && !m.virtual);
const snapshot = async (query: Q) => new Map(await Promise.all(metas().map(async m => [m.tableName, await introspect(query, m.tableName)] as const)));

beforeAll(async () => {
  const url = process.env['JWP_SCHEMA_DATABASE_URL'];
  if (!url) throw new Error('JWP_SCHEMA_DATABASE_URL is required; use an empty disposable database and migration owner credentials.');
  orm = await openFinancialOrm(url);
  q = (sql, args = []) => orm.em.fork().getConnection().execute(sql, args, 'all');
  expect((await q("SELECT to_regclass('public.wallets') relation"))[0]?.['relation']).toBeNull();
  await orm.migrator.up();
});
afterAll(async () => {
  if (!orm) return;
  try { await orm.migrator.down({ to: 0 }); } finally { await orm.close(true); }
});

test('schema produced by the SQL migrations matches every MikroORM entity (tables, columns, types, nullability, defaults, keys)', async () => {
  expect(metas().map(m => m.tableName).sort()).toEqual(['wager_transactions', 'wallets']);
  const found = drift(metas(), await snapshot(q));
  console.log(JSON.stringify({ proof: 'schema-drift', entities: metas().length, projections: Object.keys(PROJECTIONS), found }));
  expect(found).toEqual([]);
});

test('negative control: a migration-side divergence inside a rolled-back transaction is reported', async () => {
  const seen = await orm.em.fork().transactional(async em => {
    const tq: Q = (sql, args = []) => em.getConnection().execute(sql, args, 'all', em.getTransactionContext());
    await tq('ALTER TABLE wallets ALTER COLUMN player_id DROP NOT NULL');
    await tq('ALTER TABLE wallets ALTER COLUMN balance TYPE numeric(21,2)');
    await tq('ALTER TABLE wallets ADD COLUMN drift_probe text');
    const found = drift(metas(), await snapshot(tq));
    throw Object.assign(new Error('rollback'), { found });
  }).catch((e: { found?: string[] }) => e.found);
  expect(seen).toEqual(['wallets.balance: type entity=numeric(20,2) database=numeric(21,2)',
    'wallets.drift_probe: column not mapped by entity', 'wallets.player_id: nullable entity=false database=true'].sort());
  expect(drift(metas(), await snapshot(q))).toEqual([]);
});

test('negative control: a database-only default on the wallets.id primary key is reported (QA v10 repro)', async () => {
  expect(DEFAULT_EXEMPTIONS).toEqual({});
  const seen = await orm.em.fork().transactional(async em => {
    const tq: Q = (sql, args = []) => em.getConnection().execute(sql, args, 'all', em.getTransactionContext());
    await tq('ALTER TABLE wallets ALTER COLUMN id SET DEFAULT gen_random_uuid()');
    const found = drift(metas(), await snapshot(tq));
    throw Object.assign(new Error('rollback'), { found });
  }).catch((e: { found?: string[] }) => e.found);
  expect(seen).toEqual(['wallets.id: default entity=none database=gen_random_uuid()']);
  expect(drift(metas(), await snapshot(q))).toEqual([]);
});

// Aplica defaults na entidade wallets (cópia) e ALTERs no banco numa transação desfeita; devolve as divergências.
async function defaultProbe(entity: Record<string, string | undefined>, alters: string[]): Promise<string[]> {
  const wallets = metas().find(m => m.tableName === 'wallets')!;
  const properties = { ...wallets.properties } as Record<string, any>;
  for (const [name, raw] of Object.entries(entity)) properties[name] = { ...properties[name], defaultRaw: raw };
  const patched = Object.assign(Object.create(Object.getPrototypeOf(wallets)), wallets,
    { properties, props: wallets.props.map(p => properties[p.name]) }) as EntityMetadata;
  const others = metas().filter(m => m !== wallets);
  const seen = await orm.em.fork().transactional(async em => {
    const tq: Q = (sql, args = []) => em.getConnection().execute(sql, args, 'all', em.getTransactionContext());
    for (const sql of alters) await tq(sql);
    throw Object.assign(new Error('rollback'), { found: drift([patched, ...others], await snapshot(tq)) });
  }).catch((e: { found?: string[] }) => e.found);
  if (!Array.isArray(seen)) throw new Error('probe did not roll back as expected');
  return seen;
}
const reported = (col: string) => [expect.stringMatching(new RegExp(`^wallets\\.${col}: default entity=.+ database=.+$`))];
const dbDefault = (col: string, expr: string) => `ALTER TABLE wallets ALTER COLUMN ${col} SET DEFAULT ${expr}`;

// Casos adversariais de normDefault (QA v14 e auto-revisão): cada um deve gerar exatamente uma divergência.
const DEFAULT_DIVERGENCES: [string, string, string, string][] = [
  ['literal case: entity brl, database BRL (QA v14 repro)', 'currency', "'brl'", "'BRL'"],
  ['literal case: entity BRL, database brl', 'currency', "'BRL'", "'brl'"],
  ['whitespace inside literal', 'currency', "'BRL'", "'BR L'"],
  ['trailing whitespace inside literal', 'currency', "'BRL '", "'BRL'"],
  ['cast must not hide a different value', 'currency', "'BRX'", "'BRL'::varchar"],
  ['cast with length must not hide a different value', 'currency', "'BRX'", "'BRL'::character varying(3)"],
  ['parenthesised cast must not hide a different value', 'currency', "'BRX'", "('BRL'::text)"],
  ['NULL default vs entity literal', 'currency', "'BRL'", 'NULL'],
  ['numeric 1 vs 1.0', 'version', '1.0', '1'],
  ['numeric 1 vs quoted 1', 'version', "'1'", '1'],
  ['numeric 1 vs 2 behind a cast', 'version', '1', "'2'::bigint"],
  ['timestamp now() vs fixed literal', 'created_at', 'now()', "'2026-01-01 00:00:00+00'::timestamptz"],
];
for (const [name, col, entityRaw, dbExpr] of DEFAULT_DIVERGENCES) {
  test(`negative control: default divergence is reported (${name})`, async () => {
    const prop = col === 'created_at' ? 'createdAt' : col;
    expect(await defaultProbe({ [prop]: entityRaw }, [dbDefault(col, dbExpr)])).toEqual(reported(col));
  });
}

// Equivalências legítimas (controle positivo): só sintaxe de catálogo, caixa de palavra-chave e a lista explícita.
const DEFAULT_EQUIVALENCES: [string, string, string | undefined, string][] = [
  ['same literal through catalog cast', 'currency', "'BRL'", "'BRL'::character varying"],
  ['same integer', 'version', '1', '1'],
  ['keyword case only', 'created_at', 'NOW()', 'now()'],
  ['explicit CURRENT_TIMESTAMP ≡ now()', 'created_at', 'now()', 'CURRENT_TIMESTAMP'],
  ['DEFAULT NULL ≡ no default', 'currency', undefined, 'NULL'],
];
for (const [name, col, entityRaw, dbExpr] of DEFAULT_EQUIVALENCES) {
  test(`positive control: equivalent defaults are accepted (${name})`, async () => {
    const prop = col === 'created_at' ? 'createdAt' : col;
    expect(await defaultProbe({ [prop]: entityRaw }, [dbDefault(col, dbExpr)])).toEqual([]);
  });
}

test('normDefault keeps literal bytes and does not fold booleans, numbers or whitespace inside literals', () => {
  expect(normDefault("'BRL'::text")).toBe("'BRL'");
  expect(normDefault("'brl'")).not.toBe(normDefault("'BRL'::text"));
  expect(normDefault("'a::text'")).toBe("'a::text'");
  expect(normDefault("'it''s'::text")).toBe("'it''s'");
  expect(normDefault("'t'::boolean")).not.toBe(normDefault('true'));
  expect(normDefault("'TRUE'")).not.toBe(normDefault('true'));
  expect(normDefault('1')).not.toBe(normDefault('1.0'));
  expect(normDefault('1')).not.toBe(normDefault("'1'::bigint"));
  expect(normDefault("'a  b'")).not.toBe(normDefault("'a b'"));
  expect(normDefault('NULL::text')).toBe('none');
  expect(normDefault(undefined)).toBe('none');
  expect(normDefault("('x'::text || 'y'::text)")).toBe("'x'::text || 'y'");
  expect(DEFAULT_KEYWORD_EQUIVALENCES).toEqual({ current_timestamp: 'now()' });
});

test('negative control: an entity-side divergence (missing table, new column, declared unique) is reported', async () => {
  const tables = await snapshot(q);
  const wallets = metas().find(m => m.tableName === 'wallets')!;
  const ghost = { ...wallets, tableName: 'ghost_table' } as EntityMetadata;
  const extended = Object.assign(Object.create(Object.getPrototypeOf(wallets)), wallets, {
    props: [...wallets.props, { ...wallets.properties['playerId']!, name: 'nickname', fieldNames: ['nickname'] }],
    uniques: [{ properties: ['currency'] }],
  }) as EntityMetadata;
  extended.properties = { ...wallets.properties, nickname: extended.props.at(-1)! } as any;
  expect(drift([ghost, extended], tables)).toEqual(['ghost_table: table missing', 'wallets.nickname: column missing',
    'wallets: unique (currency) missing'].sort());
});
