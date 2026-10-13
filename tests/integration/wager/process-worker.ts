import { openFinancialOrm } from '../../../src/persistence/financial-orm.js';
import { ProcessWager } from '../../../src/wagering/application/process-wager.js';
import { SqlProcessWagerStore } from '../../../src/wagering/infrastructure/sql-process-wager-store.js';
const orm=await openFinancialOrm(process.env['JWP_WAGER_DATABASE_URL']!);
try {
  const [connection]=await orm.em.fork().getConnection().execute<{pid:number}[]>('SELECT pg_backend_pid() pid');
  const useCase=new ProcessWager(new SqlProcessWagerStore(orm));
  console.log(JSON.stringify({ready:true,pid:process.pid,backend:connection!.pid}));
  await Bun.stdin.stream().getReader().read();
  const request=JSON.parse(process.env['JWP_WAGER_COMMAND']!);
  const results=await Promise.all(Array.from({length:Number(process.env['JWP_WAGER_COUNT'] ?? '1')},async()=>{
    try { return await useCase.execute(request.body,request.key); }
    catch(error) { return {error:(error as {code?:string}).code}; }
  }));
  console.log(JSON.stringify({pid:process.pid,results}));
} finally { await orm.close(true); }
