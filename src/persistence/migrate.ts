import { openFinancialOrm } from './financial-orm.js';

const url = process.env['JWP_MIGRATION_DATABASE_URL'];
if (!url) throw new Error('JWP_MIGRATION_DATABASE_URL_REQUIRED');
const orm = await openFinancialOrm(url);
try { await orm.migrator.up(); }
finally { await orm.close(true); }
