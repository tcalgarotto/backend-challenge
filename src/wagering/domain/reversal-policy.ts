/** Variável opcional que escolhe quantas reversões (REFUND/ROLLBACK) cada referência aceita. */
export const REVERSAL_POLICY_VARIABLE = 'JWP_REVERSAL_POLICY';

/**
 * per_type (padrão, conforme o enunciado): um REFUND e um ROLLBACK por referência; repetir o mesmo tipo é rejeitado.
 * per_reference (opcional, mais restritiva): uma única reversão por referência, de qualquer tipo.
 */
export const REVERSAL_POLICIES = Object.freeze(['per_type', 'per_reference'] as const);
export type ReversalPolicy = (typeof REVERSAL_POLICIES)[number];
export const DEFAULT_REVERSAL_POLICY: ReversalPolicy = 'per_type';

/** Nome do parâmetro de sessão lido pelo gatilho jwp_reversal_policy_guard (Migration202610140010). */
export const REVERSAL_POLICY_SETTING = 'jwp.reversal_policy';

export class ReversalPolicyConfigError extends Error {
  readonly code = 'INVALID_REVERSAL_POLICY';
  readonly length: number;
  constructor(length: number) {
    // Só o código e o tamanho do valor; o texto recebido nunca entra na mensagem nem no erro.
    super(`INVALID_REVERSAL_POLICY (length ${length})`);
    this.name = 'ReversalPolicyConfigError';
    this.length = length;
  }
}

/** Ausente ou vazia usa o padrão; aceita só os valores exatos da lista (espaços nas pontas são removidos). */
export function parseReversalPolicy(raw: string | undefined): ReversalPolicy {
  if (raw === undefined || raw.trim() === '') return DEFAULT_REVERSAL_POLICY;
  const value = raw.trim();
  if (!(REVERSAL_POLICIES as readonly string[]).includes(value)) throw new ReversalPolicyConfigError(value.length);
  return value as ReversalPolicy;
}

let current: ReversalPolicy = DEFAULT_REVERSAL_POLICY;

/** Chamado no boot antes de abrir o ORM; valor inválido encerra o processo sem ecoar o conteúdo. */
export function configureReversalPolicy(env: Record<string, string | undefined>): ReversalPolicy {
  current = parseReversalPolicy(env[REVERSAL_POLICY_VARIABLE]);
  return current;
}

export function reversalPolicy(): ReversalPolicy {
  return current;
}

/** Decide se uma nova reversão do tipo `kind` colide com as reversões já aplicadas à mesma referência. */
export function reversalBlocked(policy: ReversalPolicy, kind: string, appliedKinds: readonly string[]): boolean {
  return policy === 'per_reference' ? appliedKinds.length > 0 : appliedKinds.includes(kind);
}
