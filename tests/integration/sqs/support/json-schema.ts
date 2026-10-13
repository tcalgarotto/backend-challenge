


type Schema = { readonly [keyword: string]: unknown };

const SUPPORTED = new Set([
  '$schema', '$id', '$ref', '$defs', 'title', 'description',
  'type', 'const', 'enum', 'pattern', 'minLength', 'maxLength', 'minimum', 'maximum',
  'properties', 'required', 'additionalProperties', 'allOf', 'if', 'then', 'not',
]);

export function validate(root: Schema, value: unknown): string[] {
  const errors: string[] = [];
  check(root, root, value, '$', errors);
  return errors;
}

function check(root: Schema, schema: Schema, value: unknown, path: string, errors: string[]): void {
  for (const keyword of Object.keys(schema)) {
    if (!SUPPORTED.has(keyword)) throw new Error(`unsupported schema keyword ${keyword} at ${path}`);
  }
  if (typeof schema['$ref'] === 'string') {
    check(root, resolve(root, schema['$ref']), value, path, errors);
  }
  if ('const' in schema && !sameJson(schema['const'], value)) errors.push(`${path}: expected const ${JSON.stringify(schema['const'])}`);
  if (Array.isArray(schema['enum']) && !schema['enum'].some(option => sameJson(option, value))) errors.push(`${path}: not in enum`);
  if (typeof schema['type'] === 'string' && !hasType(schema['type'], value)) {
    errors.push(`${path}: expected ${schema['type']}`);
    return;
  }
  if (typeof value === 'string') {
    if (typeof schema['pattern'] === 'string' && !new RegExp(schema['pattern'], 'u').test(value)) errors.push(`${path}: pattern mismatch`);
    if (typeof schema['minLength'] === 'number' && [...value].length < schema['minLength']) errors.push(`${path}: shorter than minLength`);
    if (typeof schema['maxLength'] === 'number' && [...value].length > schema['maxLength']) errors.push(`${path}: longer than maxLength`);
  }
  if (typeof value === 'number' && typeof schema['minimum'] === 'number' && value < schema['minimum']) errors.push(`${path}: below minimum`);
  if (typeof value === 'number' && typeof schema['maximum'] === 'number' && value > schema['maximum']) errors.push(`${path}: above maximum`);
  if (isObject(value)) {
    const properties = (schema['properties'] ?? {}) as Record<string, Schema>;
    for (const name of (schema['required'] ?? []) as string[]) {
      if (!(name in value)) errors.push(`${path}.${name}: required`);
    }
    for (const [name, child] of Object.entries(value)) {
      const propertySchema = properties[name];
      if (propertySchema) check(root, propertySchema, child, `${path}.${name}`, errors);
      else if (schema['additionalProperties'] === false) errors.push(`${path}.${name}: additional property`);
    }
  }
  for (const part of (schema['allOf'] ?? []) as Schema[]) check(root, part, value, path, errors);
  if (isObject(schema['if'])) {
    const conditionHolds = validate(rootWith(root, schema['if']), value).length === 0;
    if (conditionHolds && isObject(schema['then'])) check(root, schema['then'], value, path, errors);
  }
  if (isObject(schema['not']) && validate(rootWith(root, schema['not']), value).length === 0) errors.push(`${path}: matches forbidden schema`);
}

function rootWith(root: Schema, schema: Schema): Schema {
  return { ...schema, $defs: root['$defs'] };
}

function resolve(root: Schema, ref: string): Schema {
  const match = /^#\/\$defs\/([A-Za-z0-9]+)$/.exec(ref);
  const target = match ? (root['$defs'] as Record<string, Schema> | undefined)?.[match[1]!] : undefined;
  if (!target) throw new Error(`unresolvable $ref ${ref}`);
  return target;
}

function hasType(type: string, value: unknown): boolean {
  if (type === 'object') return isObject(value);
  if (type === 'integer') return Number.isInteger(value);
  return typeof value === type;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function sameJson(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}
