const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isExactUuid = (value: unknown): value is string => typeof value === 'string' && uuid.exec(value)?.[0] === value;
// PostgreSQL text/JSONB não persistem NUL nem substitutos UTF-16 isolados.
// A flag Unicode permite pares válidos de substitutos.
export const isOpaqueIdentifier = (value: unknown): value is string => typeof value === 'string'
  && value.trim().length > 0 && value.length <= 128 && !/[\u0000\uD800-\uDFFF]/u.test(value);
