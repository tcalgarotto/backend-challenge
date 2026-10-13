import { test, expect, spyOn } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Money } from '../../src/shared/domain/money.js';
import { WalletLedgerEntry, LedgerDirection } from '../../src/wallet/domain/wallet-ledger-entry.js';
import { InboxMessage } from '../../src/messaging/domain/inbox-message.js';
import { OutboxMessage } from '../../src/messaging/domain/outbox-message.js';
const at=new Date('2026-10-12T10:00:00Z');
const money=(amount:string)=>Money.from({amount,currency:'BRL'});
test('ledger rejects inconsistent arithmetic/currency/zero and preserves cents beyond 2^53 and date immutability',()=>{
 const props={id:randomUUID(),walletId:randomUUID(),transactionId:randomUUID(),direction:LedgerDirection.Debit,
  money:money('0.01'),balanceBefore:money('9007199254740993.01'),balanceAfter:money('9007199254740993.00'),createdAt:new Date(at)};
 const entry=WalletLedgerEntry.create(props);expect(entry.isBalanced()).toBe(true);expect(Object.isFrozen(entry)).toBe(true);
 props.createdAt.setFullYear(1999);entry.createdAt.setFullYear(1999);expect(entry.createdAt).toEqual(at);
 expect(()=>WalletLedgerEntry.create({...props,balanceAfter:props.balanceBefore})).toThrow('UNBALANCED_LEDGER_ENTRY');
 expect(()=>WalletLedgerEntry.create({...props,money:money('0.00')})).toThrow();
 expect(()=>WalletLedgerEntry.create({...props,balanceAfter:Money.from({amount:'1.00',currency:'USD'})})).toThrow();
});
test('inbox persists the first processing time, rejects backwards time and defends dates',()=>{
 const message=InboxMessage.receive({messageId:randomUUID(),consumerName:'consumer',payloadHash:'a'.repeat(64),receivedAt:new Date(at)});
 expect(message.isProcessed()).toBe(false);expect(()=>message.markProcessed(new Date(at.getTime()-1))).toThrow();
 const done=new Date(at.getTime()+1000);message.markProcessed(done);done.setFullYear(2000);
 message.markProcessed(new Date(at.getTime()+2000));expect(message.processedAt).toEqual(new Date(at.getTime()+1000));
 const restored=InboxMessage.rehydrate({messageId:message.messageId,consumerName:message.consumerName,payloadHash:message.payloadHash,
  receivedAt:message.receivedAt,processedAt:message.processedAt!});expect(restored.isProcessed()).toBe(true);
});
test('outbox retry backoff is capped, published messages cannot retry, payload and dates cannot mutate',()=>{
 const source={id:randomUUID(),aggregateId:randomUUID(),eventType:'Event',payload:{data:{value:1}},occurredAt:new Date(at),attempts:0};
 const message=OutboxMessage.rehydrate(source);source.payload.data.value=2;
 expect(message.payload).toEqual({data:{value:1}});expect(message.isDue(at)).toBe(true);
 message.scheduleRetry(at);expect(message.attempts).toBe(1);expect(message.nextAttemptAt!.getTime()-at.getTime()).toBe(1000);
 message.scheduleRetry(at);expect(message.nextAttemptAt!.getTime()-at.getTime()).toBe(2000);
 for(let i=0;i<20;i++)message.scheduleRetry(at);expect(message.nextAttemptAt!.getTime()-at.getTime()).toBe(60000);
 message.markPublished(at);expect(message.isPending()).toBe(false);expect(message.isDue(at)).toBe(false);
 expect(()=>message.scheduleRetry(at)).toThrow();message.publishedAt!.setFullYear(2000);expect(message.publishedAt).toEqual(at);
});

test('rehydration restores persisted records without calling creation or processing transitions',()=>{
  const entry={id:randomUUID(),walletId:randomUUID(),transactionId:randomUUID(),direction:LedgerDirection.Credit,
    money:Money.from({amount:'1.00',currency:'BRL'}),balanceBefore:Money.zero('BRL'),
    balanceAfter:Money.from({amount:'1.00',currency:'BRL'}),createdAt:new Date(1000)};
  const create=spyOn(WalletLedgerEntry,'create').mockImplementation(()=>{throw new Error('creation forbidden');});
  const receive=spyOn(InboxMessage,'receive').mockImplementation(()=>{throw new Error('receive forbidden');});
  const transition=spyOn(InboxMessage.prototype,'markProcessed').mockImplementation(()=>{throw new Error('transition forbidden');});
  try {
    expect(WalletLedgerEntry.rehydrate(entry).isBalanced()).toBe(true);
    expect(()=>WalletLedgerEntry.rehydrate({...entry,balanceAfter:Money.zero('BRL')})).toThrow('UNBALANCED_LEDGER_ENTRY');
    const inbox=InboxMessage.rehydrate({consumerName:'consumer',messageId:'message',payloadHash:'a'.repeat(64),
      receivedAt:new Date(1000),processedAt:new Date(2000)});
    expect(inbox.processedAt?.getTime()).toBe(2000);expect(inbox.isProcessed()).toBe(true);
    expect(create).not.toHaveBeenCalled();expect(receive).not.toHaveBeenCalled();expect(transition).not.toHaveBeenCalled();
  } finally {create.mockRestore();receive.mockRestore();transition.mockRestore();}
});
