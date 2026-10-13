import { z } from 'zod';

export interface WorkerConfig {
  readonly commandsUrl: string;
  readonly eventsUrl: string;
  readonly intervalMs: number;
}

export interface RuntimeConfig { readonly http: boolean; readonly workers?: WorkerConfig }
export interface EnvironmentConfig extends RuntimeConfig {
  readonly port: number;
  readonly host: string;
  readonly databaseUrl: string;
  readonly readinessTimeoutMs: number;
  readonly queueUrls: readonly string[];
  readonly region: string;
  readonly endpoint?: string;
}

export interface EnvironmentIssue { readonly field: string; readonly code: string }
export class EnvironmentConfigError extends Error {
  constructor(readonly issues: readonly EnvironmentIssue[]) {
    super(`INVALID_ENVIRONMENT: ${issues.map(issue => `${issue.field}=${issue.code}`).join('; ')}`);
    this.name = 'EnvironmentConfigError';
  }
}

function validUrl(value: string, protocols: readonly string[], fifo = false): boolean {
  try {
    const url = new URL(value);
    return protocols.includes(url.protocol) && (!fifo || url.pathname.endsWith('.fifo'));
  } catch { return false; }
}

const runtimeFields = z.object({
  APP_ROLES: z.string().default('http'),
  JWP_SQS_COMMAND_QUEUE_URL: z.string().optional(),
  JWP_SQS_EVENT_QUEUE_URL: z.string().optional(),
  JWP_WORKER_INTERVAL_MS: z.string().optional(),
});

function validateRuntime(env: z.infer<typeof runtimeFields>, context: z.RefinementCtx): void {
  const issue = (field: string, message: string) => context.addIssue({ code: z.ZodIssueCode.custom, path: [field], message });
  const roles = env.APP_ROLES.split(',').map(role => role.trim());
  if (roles.some(role => !['http', 'workers'].includes(role)) || new Set(roles).size !== roles.length)
    issue('APP_ROLES', 'INVALID_APP_ROLES');
  if (!roles.includes('workers')) return;
  for (const field of ['JWP_SQS_COMMAND_QUEUE_URL', 'JWP_SQS_EVENT_QUEUE_URL'] as const) {
    const url = env[field];
    if (!url) issue(field, 'WORKER_QUEUE_URLS_REQUIRED');
    else if (!validUrl(url, ['http:', 'https:'], true)) issue(field, 'WORKER_FIFO_QUEUE_URL_REQUIRED');
  }
  if (env.JWP_SQS_COMMAND_QUEUE_URL && env.JWP_SQS_COMMAND_QUEUE_URL === env.JWP_SQS_EVENT_QUEUE_URL)
    issue('JWP_SQS_EVENT_QUEUE_URL', 'WORKER_QUEUES_MUST_BE_SEPARATE');
  const interval = Number(env.JWP_WORKER_INTERVAL_MS ?? '250');
  if (!Number.isSafeInteger(interval) || interval < 1 || interval > 60_000)
    issue('JWP_WORKER_INTERVAL_MS', 'INVALID_WORKER_INTERVAL_MS');
}

const numeric = (fallback: number, minimum: number, maximum: number, code: string) =>
  z.preprocess(value => value === undefined ? fallback : Number(value),
    z.number({ invalid_type_error: code }).int(code).min(minimum, code).max(maximum, code));
const httpUrl = z.string().refine(value => validUrl(value, ['http:', 'https:']), 'INVALID_HTTP_URL');
const environmentSchema = runtimeFields.superRefine(validateRuntime).and(z.object({
  PORT: numeric(3037, 0, 65535, 'INVALID_PORT'),
  HOST: z.string().min(1, 'INVALID_HOST').default('127.0.0.1'),
  JWP_DATABASE_URL: z.string({ required_error: 'JWP_DATABASE_URL_REQUIRED' })
    .min(1, 'JWP_DATABASE_URL_REQUIRED')
    .refine(value => !value || validUrl(value, ['postgres:', 'postgresql:']), 'INVALID_DATABASE_URL'),
  JWP_READINESS_TIMEOUT_MS: numeric(1000, 1, 30_000, 'INVALID_READINESS_TIMEOUT_MS'),
  JWP_SQS_QUEUE_URLS: z.string().default('').transform(value => value.split(',').map(item => item.trim()).filter(Boolean))
    .refine(values => values.every(value => validUrl(value, ['http:', 'https:'])), 'INVALID_SQS_QUEUE_URLS'),
  JWP_SQS_ENDPOINT: z.preprocess(value => value === '' ? undefined : value, httpUrl.optional()),
  AWS_REGION: z.string().min(1, 'INVALID_AWS_REGION').default('us-east-1'),
}));

function parsed<T>(result: z.SafeParseReturnType<unknown, T>): T {
  if (result.success) return result.data;
  // Só nomes e códigos chegam ao erro de boot; valores podem conter credenciais.
  throw new EnvironmentConfigError(result.error.issues.map(issue => ({ field: issue.path.join('.'), code: issue.message })));
}

function runtime(env: z.infer<typeof runtimeFields>): RuntimeConfig {
  const roles = env.APP_ROLES.split(',').map(role => role.trim());
  return { http: roles.includes('http'), ...(roles.includes('workers') ? { workers: {
    commandsUrl: env.JWP_SQS_COMMAND_QUEUE_URL!, eventsUrl: env.JWP_SQS_EVENT_QUEUE_URL!,
    intervalMs: Number(env.JWP_WORKER_INTERVAL_MS ?? '250'),
  } } : {}) };
}

export function loadRuntimeConfig(env: Record<string, string | undefined>): RuntimeConfig {
  return runtime(parsed(runtimeFields.superRefine(validateRuntime).safeParse(env)));
}

export function loadEnvironment(env: Record<string, string | undefined>): EnvironmentConfig {
  const values = parsed(environmentSchema.safeParse(env));
  const roles = runtime(values);
  const queueUrls = [...values.JWP_SQS_QUEUE_URLS];
  if (roles.workers) queueUrls.push(roles.workers.commandsUrl, roles.workers.eventsUrl);
  return Object.freeze({ ...roles, port: values.PORT, host: values.HOST, databaseUrl: values.JWP_DATABASE_URL,
    readinessTimeoutMs: values.JWP_READINESS_TIMEOUT_MS, queueUrls: Object.freeze([...new Set(queueUrls)]),
    region: values.AWS_REGION, ...(values.JWP_SQS_ENDPOINT ? { endpoint: values.JWP_SQS_ENDPOINT } : {}) });
}
