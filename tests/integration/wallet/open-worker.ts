import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { OpenWallet } from '../../../src/wallet/application/open-wallet.js';
import { SqlWalletOpeningStore } from '../../../src/wallet/infrastructure/sql-wallet-opening-store.js';

const url = process.env['JWP_WALLET_DATABASE_URL'];
const playerId = process.env['JWP_OPEN_PLAYER'];
if (!url || !playerId) throw new Error('Wallet worker requires database URL and player ID.');
const orm = await openFinancialOrm(url);
try {
  // Todos os workers conectam antes de o pai liberar a barreira.
  await orm.em.getConnection().execute('SELECT 1');
  process.stdout.write('ready\n');
  await Bun.stdin.text();
  try {
    const wallet = await new OpenWallet(new SqlWalletOpeningStore(orm)).execute({
      playerId, initialBalance: { amount: '100.00', currency: 'BRL' },
    }, `race-${process.pid}`);
    console.log(JSON.stringify({ ok: true, pid: process.pid, wallet }));
  } catch (error) {
    console.log(JSON.stringify({ ok: false, pid: process.pid, code: (error as { code?: string }).code }));
  }
} finally { await orm.close(true); }
