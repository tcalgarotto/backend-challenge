import { SendMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { openFinancialOrm } from '../../../../src/persistence/financial-orm.js';
import { SqlOutboxStore } from '../../../../src/messaging/outbox/sql-outbox-store.js';
import { SqsOutboxPublisher } from '../../../../src/messaging/outbox/sqs-outbox-publisher.js';
import { SqlEventDeduplicator, SqsEventConsumer } from '../../../../src/messaging/outbox/sqs-event-consumer.js';
import { sqsClient } from '../../sqs/support/sqs.js';
const input=JSON.parse(Bun.env['JWP_OUTBOX_AUDIT_OPTIONS']??'{}') as {mode:string;queueUrl:string;batchSize?:number;leaseMs?:number;drain?:boolean};
const orm=await openFinancialOrm(Bun.env['JWP_OUTBOX_DATABASE_URL']!);const sqs=sqsClient();
const stdin=Bun.stdin.stream().getReader();const barrier=async()=>{await stdin.read();};
try{
 const [row]=await orm.em.fork().getConnection().execute<any[]>('SELECT pg_backend_pid() backend');
 console.log(JSON.stringify({ready:true,pid:process.pid,backend:row.backend}));await barrier();
 const store=new SqlOutboxStore(orm,input.leaseMs??3000);
 const client={send:async(command:any,options:any)=>{
  if(command instanceof DeleteMessageCommand&&input.mode==='ack-barrier'){
   console.log(JSON.stringify({ackBoundary:true,receipt:command.input.ReceiptHandle,pid:process.pid}));await barrier();
  }
  const result=await sqs.send(command,options);
  if(command instanceof SendMessageCommand){
   const eventId=JSON.parse(command.input.MessageBody!).eventId;
   const [row]=await orm.em.fork().getConnection().execute<any[]>('SELECT claim_token,published_at FROM outbox_events WHERE event_id=?',[eventId]);
   if(!row?.claim_token||row.published_at!==null)throw new Error('Uncommitted or invalid claim before send');
   console.log(JSON.stringify({sent:eventId,pid:process.pid}));
   if(input.mode==='send-barrier')await barrier();
  }
  return result;
 }} as any;
 if(input.mode==='claim-barrier'){
  const claimed=await store.claimBatch(input.batchSize??1);console.log(JSON.stringify({claimed,pid:process.pid}));await barrier();
 }else if(input.mode==='consume'||input.mode==='ack-barrier'){
  const outcome=await new SqsEventConsumer(client,input.queueUrl,new SqlEventDeduplicator(orm),{waitTimeSeconds:1}).pollOnce();
  console.log(JSON.stringify({outcome,pid:process.pid}));
 }else{
  const publisher=new SqsOutboxPublisher(client,input.queueUrl,store,{batchSize:input.batchSize??3});
  // Claims por wallet são exclusivos; drenar exige reivindicar até não haver trabalho disponível.
  const outcome=[];for(;;){const round=await publisher.publishBatch();outcome.push(...round);if(!input.drain||round.length===0)break;}
  console.log(JSON.stringify({outcome,pid:process.pid}));
  if(input.mode==='commit-barrier')await barrier();
 }
}finally{sqs.destroy();await orm.close(true);}
