import { openFinancialOrm } from '../../../../src/persistence/financial-orm.js';
import { SqlPendingReferenceScheduler } from '../../../../src/messaging/pending-reference-scheduler.js';
const url=Bun.env['JWP_SCHEDULER_DATABASE_URL'];if(!url)throw new Error('Dedicated scheduler DB required');
const orm=await openFinancialOrm(url);
const options=JSON.parse(Bun.env['JWP_SCHEDULER_AUDIT_OPTIONS']??'{}') as {mode?:string;claim?:any;leaseMs?:number};
const scheduler=new SqlPendingReferenceScheduler(orm,{leaseMs:options.leaseMs??1000,jitter:()=>0});
const input=Bun.stdin.stream().getReader();
const barrier=async()=>{await input.read();};
try{
 const [{pid:backend}]=await orm.em.fork().getConnection().execute<any[]>('SELECT pg_backend_pid() pid');
 console.log(JSON.stringify({ready:true,pid:process.pid,backend}));await barrier();
 const claim=options.claim?{...options.claim,expiresAt:new Date(options.claim.expiresAt),leaseUntil:new Date(options.claim.leaseUntil)}:await scheduler.claimOne();
 console.log(JSON.stringify({claimed:claim,pid:process.pid}));
 if(options.mode==='claim-barrier'){await barrier();}
 if(claim&&options.mode!=='claim-only'){
  const outcome=await scheduler.processClaim(claim);console.log(JSON.stringify({outcome,pid:process.pid}));
 }
 if(options.mode==='commit-barrier')await barrier();
}finally{await orm.close(true);}
