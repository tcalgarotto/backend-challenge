import 'reflect-metadata';
import { Body, Controller, Get, Inject, Module, Param, Post, Query, Res, type DynamicModule } from '@nestjs/common';
import type { MikroORM } from '@mikro-orm/postgresql';
import { respond, type HttpResponse, type TypedHttpError } from '../../shared/http/typed-response.js';
import type { SqlTransactionPolicy } from '../../shared/infrastructure/sql-transaction.js';
import { OpenWallet, WalletOpeningError } from '../application/open-wallet.js';
import { ReconcileWallet, type ReconciliationMonitor } from '../application/reconcile-wallet.js';
import { WalletQueries, WalletReadError } from '../application/wallet-queries.js';
import { ReconciliationSignals } from '../infrastructure/reconciliation-signals.js';
import { SqlWalletOpeningStore } from '../infrastructure/sql-wallet-opening-store.js';
import { SqlWalletQueryStore } from '../infrastructure/sql-wallet-query-store.js';

function typed(error: unknown): TypedHttpError | undefined {
  if (error instanceof WalletReadError) return error;
  if (error instanceof WalletOpeningError) return { code: error.code, httpStatus: error.code === 'WALLET_ALREADY_EXISTS' ? 409 : 400 };
  return undefined;
}

@Controller('wallets')
export class WalletController {
  constructor(@Inject(OpenWallet) private readonly openWallet: OpenWallet,
    @Inject(WalletQueries) private readonly queries: WalletQueries,
    @Inject(ReconcileWallet) private readonly reconcileWallet: ReconcileWallet) {}

  @Post()
  open(@Body() body: unknown, @Res() response: HttpResponse) {
    return respond(response, async correlationId => ({ httpStatus: 201, body: await this.openWallet.execute(body, correlationId) }), typed);
  }

  @Get(':walletId')
  get(@Param('walletId') walletId: string, @Res() response: HttpResponse) {
    return respond(response, async () => ({ httpStatus: 200, body: await this.queries.getWallet(walletId) }), typed);
  }

  @Get(':walletId/ledger')
  ledger(@Param('walletId') walletId: string, @Query('cursor') cursor: unknown, @Query('limit') limit: unknown, @Res() response: HttpResponse) {
    return respond(response, async () => ({ httpStatus: 200, body: await this.queries.listLedger(walletId, { cursor, limit }) }), typed);
  }

  /** 200 indica verificação concluída, mesmo quando houver divergência. */
  @Post(':walletId/reconciliation')
  reconcile(@Param('walletId') walletId: string, @Body() body: unknown, @Res() response: HttpResponse) {
    return respond(response, async correlationId =>
      ({ httpStatus: 200, body: await this.reconcileWallet.execute(walletId, body, correlationId) }), typed);
  }
}

export interface WalletModuleOptions {
  readonly sqlPolicy?: Partial<SqlTransactionPolicy>;
  readonly reconciliationMonitor?: ReconciliationMonitor;
}
/** ORM explícito preserva a composição no build Bun. */
@Module({})
export class WalletModule {
  static register(orm: MikroORM, options: WalletModuleOptions = {}): DynamicModule {
    const store = new SqlWalletQueryStore(orm, options.sqlPolicy);
    const monitor = options.reconciliationMonitor ?? new ReconciliationSignals();
    return { module: WalletModule, controllers: [WalletController], providers: [
      { provide: OpenWallet, useFactory: () => new OpenWallet(new SqlWalletOpeningStore(orm, options.sqlPolicy)) },
      { provide: WalletQueries, useFactory: () => new WalletQueries(store) },
      { provide: ReconcileWallet, useFactory: () => new ReconcileWallet(store, monitor) },
    ], exports: [WalletQueries, ReconcileWallet] };
  }
}
