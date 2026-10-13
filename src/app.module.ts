import { Module, type DynamicModule, type BeforeApplicationShutdown, type OnApplicationShutdown } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';

import type { MikroORM } from '@mikro-orm/postgresql';
import { WalletModule } from './wallet/http/wallet.module.js';
import { WageringModule } from './wagering/http/wagering.module.js';
import { HealthController, MetricsController, OBSERVABILITY } from './observability/http.js';
import type { ApplicationWorkers } from './runtime/workers.js';
import { Observability } from './observability/observability.js';
import { LocalProviderIdentity, PROVIDER_IDENTITY, ProviderAuthGuard, type ProviderIdentityPort } from './shared/http/provider-identity.js';

@Module({ controllers: [HealthController, MetricsController] })
export class AppModule {
  static register(orm: MikroORM, observability: Observability = new Observability({ orm }), workers?: ApplicationWorkers,
    identity: ProviderIdentityPort = new LocalProviderIdentity()): DynamicModule {
    const lifecycle: OnApplicationShutdown & BeforeApplicationShutdown = {
      async beforeApplicationShutdown() { await workers?.stop(); },
      async onApplicationShutdown() { observability.close(); await orm.close(true); },
    };
    return { module: AppModule,
      imports: [WalletModule.register(orm, { reconciliationMonitor: observability.reconciliationMonitor }), WageringModule.register(orm)],
      providers: [{ provide: 'FinancialOrmLifecycle', useValue: lifecycle }, { provide: OBSERVABILITY, useValue: observability },
        { provide: PROVIDER_IDENTITY, useValue: identity }, { provide: APP_GUARD, useClass: ProviderAuthGuard }] };
  }
}
