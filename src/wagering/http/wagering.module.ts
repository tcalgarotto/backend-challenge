import 'reflect-metadata';
import { Body, Controller, Get, Headers, Inject, Module, Param, Post, Res, type DynamicModule } from '@nestjs/common';
import type { MikroORM } from '@mikro-orm/postgresql';
import { ProcessWager, ProcessWagerError } from '../application/process-wager.js';
import { SqlProcessWagerStore } from '../infrastructure/sql-process-wager-store.js';
import { TransactionQueries, TransactionQueryError } from '../application/transaction-queries.js';
import { SqlTransactionQueryStore } from '../infrastructure/sql-transaction-query-store.js';
import { recordInfrastructureFailure, requestCorrelationId, respond, type HttpResponse } from '../../shared/http/typed-response.js';

const typedQuery = (error: unknown) => error instanceof TransactionQueryError ? error : undefined;
@Controller('wagering/transactions')
export class WageringController {
  constructor(@Inject(ProcessWager) private readonly processWager: ProcessWager,
    @Inject(TransactionQueries) private readonly queries: TransactionQueries) {}
  @Get(':transactionId')
  get(@Param('transactionId') transactionId: string, @Res() response: HttpResponse) {
    return respond(response, async () => ({httpStatus:200,body:await this.queries.byId(transactionId)}), typedQuery);
  }
  @Post()
  async submit(@Body() body: unknown, @Headers('idempotency-key') key: unknown, @Res() response: HttpResponse) {
    const correlationId = requestCorrelationId(response);
    try {
      const result = await this.processWager.execute(body,key,correlationId);
      response.status(result.httpStatus).json(result.body);
    } catch(error) {
      if (!(error instanceof ProcessWagerError)) throw error;
      if (error.code === 'INFRASTRUCTURE_UNAVAILABLE') recordInfrastructureFailure(response, error);
      response.status(error.httpStatus).json({error:{code:error.code,message:error.code,
        retryable:error.code === 'INFRASTRUCTURE_UNAVAILABLE'},correlationId});
    }
  }
}

@Controller('providers/:providerId/wagering/transactions')
export class ProviderTransactionController {
  constructor(@Inject(TransactionQueries) private readonly queries: TransactionQueries) {}
  @Get(':externalTransactionId')
  get(@Param('providerId') providerId: string, @Param('externalTransactionId') externalTransactionId: string, @Res() response: HttpResponse) {
    return respond(response, async () => ({httpStatus:200,body:await this.queries.byProviderIdentity(providerId,externalTransactionId)}), typedQuery);
  }
}
/** ORM fornecido explicitamente evita depender de inferência de DI no build Bun. */
@Module({})
export class WageringModule {
  static register(orm: MikroORM): DynamicModule {
    return {module:WageringModule,controllers:[WageringController,ProviderTransactionController],providers:[
      {provide:ProcessWager,useFactory:()=>new ProcessWager(new SqlProcessWagerStore(orm))},
      {provide:TransactionQueries,useFactory:()=>new TransactionQueries(new SqlTransactionQueryStore(orm))},
    ],exports:[ProcessWager,TransactionQueries]};
  }
}
