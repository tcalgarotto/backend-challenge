import 'reflect-metadata';
import { Inject, Injectable, SetMetadata, type CanActivate, type ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';

/** providerId nulo no modo local não significa provedor autenticado. */
export interface ProviderIdentity { readonly providerId: string | null }

export interface ProviderIdentityRequest { readonly headers: Readonly<Record<string, string | string[] | undefined>> }

/** undefined nega acesso; adapter do IdP valida o token, e o domínio continua validando providerId. */
export interface ProviderIdentityPort { identify(request: ProviderIdentityRequest): Promise<ProviderIdentity | undefined> }
export const PROVIDER_IDENTITY = Symbol('JwpProviderIdentity');

/** Modo sem IdP aceita chamadas, mas não autentica o provedor. */
export class LocalProviderIdentity implements ProviderIdentityPort {
  async identify(): Promise<ProviderIdentity> { return { providerId: null }; }
}

export const INSECURE_LOCAL_AUTH_OPT_IN = 'JWP_ALLOW_INSECURE_LOCAL_AUTH';

export class InsecureLocalAuthError extends Error {
  readonly code = 'INSECURE_LOCAL_AUTH_IN_PRODUCTION';
  constructor() {
    super(`INSECURE_LOCAL_AUTH_IN_PRODUCTION: NODE_ENV=production with LocalProviderIdentity (no-op ProviderAuthGuard); inject an IdP adapter or set ${INSECURE_LOCAL_AUTH_OPT_IN}=true explicitly`);
    this.name = 'InsecureLocalAuthError';
  }
}

/** Recusa o boot em produção com identidade local, salvo opt-in explícito; não altera o fluxo financeiro. */
export function assertLocalIdentityAllowed(env: Readonly<Record<string, string | undefined>>, identity: ProviderIdentityPort): void {
  if (!(identity instanceof LocalProviderIdentity)) return;
  if (env['NODE_ENV'] !== 'production') return;
  if (env[INSECURE_LOCAL_AUTH_OPT_IN] === 'true') return;
  throw new InsecureLocalAuthError();
}

const PUBLIC_ROUTE = 'jwp:publicRoute';
/** Health e coleta de métricas ficam fora da autenticação de provedor. */
export const PublicRoute = () => SetMetadata(PUBLIC_ROUTE, true);

@Injectable()
export class ProviderAuthGuard implements CanActivate {
  constructor(@Inject(Reflector) private readonly reflector: Reflector,
    @Inject(PROVIDER_IDENTITY) private readonly identity: ProviderIdentityPort) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    if (this.reflector.getAllAndOverride<boolean>(PUBLIC_ROUTE, [context.getHandler(), context.getClass()])) return true;
    return (await this.identity.identify(context.switchToHttp().getRequest<ProviderIdentityRequest>())) !== undefined;
  }
}
