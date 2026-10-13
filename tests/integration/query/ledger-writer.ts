import { randomUUID } from 'node:crypto';
import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { ProcessWager } from '../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../src/wagering/infrastructure/sql-process-wager-store.js';

// Processo separado faz append na wallet enquanto o pai reconcilia, exercitando a barreira real de lock.
const orm = await openFinancialOrm(process.env['JWP_QUERY_DATABASE_URL']!);
try {
  const useCase = new ProcessWager(new SqlProcessWagerStore(orm));
  const { walletId, playerId, count } = JSON.parse(process.env['JWP_LEDGER_WRITER']!) as { walletId: string; playerId: string; count: number };
  console.log(JSON.stringify({ ready: true, pid: process.pid }));
  await Bun.stdin.stream().getReader().read();
  const statuses: number[] = [];
  for (let i = 0; i < count; i++) {
    const id = randomUUID(), kind = i % 2 === 0 ? 'BET' : 'WIN';
    const result = await useCase.execute({ providerId: 'reconcile-writer', externalTransactionId: id, walletId, playerId,
      roundId: 'r', gameId: 'g', kind, money: { amount: kind === 'BET' ? '1.01' : '2.02', currency: 'BRL' } }, 'key:' + id);
    statuses.push(result.httpStatus);
  }
  console.log(JSON.stringify({ pid: process.pid, statuses }));
} finally { await orm.close(true); }
