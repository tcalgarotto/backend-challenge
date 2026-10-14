import { DecimalType, EntitySchema } from '@mikro-orm/core';
import { MikroORM } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { Migration202610110002 } from './migrations/Migration202610110002.js';
import { Migration202610110003 } from './migrations/Migration202610110003.js';
import { Migration202610110004 } from './migrations/Migration202610110004.js';
import { Migration202610120005 } from './migrations/Migration202610120005.js';
import { Migration202610120006 } from './migrations/Migration202610120006.js';
import { Migration202610120007 } from './migrations/Migration202610120007.js';
import { Migration202610130009 } from './migrations/Migration202610130009.js';
import { Migration202610140010 } from './migrations/Migration202610140010.js';
import { Migration202610120008 } from './migrations/Migration202610120008.js';
import { observePostgresConnection } from './postgres-connection-failure.js';

// DecimalType compara por Number; mapeamento e comparação devem preservar
// todos os centavos, inclusive valores acima de 2^53.
export class FinancialDecimalType extends DecimalType<'string'> {
  constructor() { super('string'); }
  override compareValues(a: string, b: string): boolean { return a === b; }
  override convertToDatabaseValue(value: string): string {
    if (typeof value !== 'string' || !/^(0|[1-9][0-9]{0,17})\.[0-9]{2}$/.test(value)) {
      throw new Error('Financial decimal must be a nonnegative exact two-decimal string within numeric(20,2).');
    }
    return value;
  }
}
export class WalletRecord {
  id!: string;
  playerId!: string;
  currency!: string;
  balance!: string;
  version!: bigint;
  createdAt!: Date;
  updatedAt!: Date;
}
export const WalletRecordSchema = new EntitySchema<WalletRecord>({
  class: WalletRecord, tableName: 'wallets', properties: {
    id: { type: 'uuid', primary: true }, playerId: { type: 'string' }, currency: { type: 'string' },
    balance: { type: new FinancialDecimalType(), precision: 20, scale: 2 },
    version: { type: 'bigint' }, createdAt: { type: 'Date', defaultRaw: 'now()' },
    updatedAt: { type: 'Date', defaultRaw: 'now()' },
  },
});

// O lock só precisa da identidade; o snapshot financeiro continua vindo do SQL.
export class WagerTransactionLockRecord { id!: string; }
export const WagerTransactionLockSchema = new EntitySchema<WagerTransactionLockRecord>({
  class: WagerTransactionLockRecord, tableName: 'wager_transactions', properties: { id: { type: 'uuid', primary: true } },
});
export const walletRecordRow = (wallet: WalletRecord) => ({
  id: wallet.id, player_id: wallet.playerId, currency: wallet.currency, balance: wallet.balance,
  version: wallet.version, created_at: wallet.createdAt, updated_at: wallet.updatedAt,
});

// Owner executa DDL; runtime usa LOGIN separado com jwp_runtime,
// que é NOLOGIN e não possui tabelas.
export async function openFinancialOrm(clientUrl: string) {
  return MikroORM.init({ clientUrl, entities: [WalletRecordSchema, WagerTransactionLockSchema], extensions: [Migrator],
    migrations: { migrationsList: [Migration202610110002, Migration202610110003, Migration202610110004, Migration202610120005, Migration202610120006, Migration202610120007, Migration202610120008, Migration202610130009, Migration202610140010], tableName: 'jwp_financial_migrations',
      transactional: true, allOrNothing: true, snapshot: false },
    pool: { min: 0, max: 4, acquireTimeoutMillis: 5000, afterCreate: observePostgresConnection },
  });
}
