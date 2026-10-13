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
