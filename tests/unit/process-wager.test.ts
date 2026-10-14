import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { canonicalJson, parseWager, ProcessWager } from '../../src/wagering/application/process-wager.js';
import { configureCurrencies } from '../../src/shared/domain/currency.js';
const body = () => ({providerId:'p',externalTransactionId:'x',playerId:randomUUID(),walletId:randomUUID(),
  roundId:'r',gameId:'g',kind:'BET',money:{amount:'1.00',currency:'BRL'}});
test('canonical hash sorts nested keys and excludes key/transport metadata',()=>{
  const input=body();
  const reversed=Object.fromEntries(Object.entries(input).reverse());
  reversed['money']={currency:'BRL',amount:'1.00'};
  const a=parseWager(input,'  opaque-key  '),b=parseWager(reversed,'different-key');
  expect(a.payloadHash).toBe(b.payloadHash);
  expect(a.idempotencyKey).toBe('  opaque-key  ');
  expect(canonicalJson({z:{b:2,a:1},a:0})).toBe('{"a":0,"z":{"a":1,"b":2}}');
  for(const field of ['providerId','externalTransactionId','roundId','gameId'] as const)
    expect(parseWager({...input,[field]:'other'},'k').payloadHash).not.toBe(a.payloadHash);
  expect(parseWager({...input,money:{amount:'2.00',currency:'BRL'}},'k').payloadHash).not.toBe(a.payloadHash);
  const uppercase=parseWager({...input,playerId:input.playerId.toUpperCase(),walletId:input.walletId.toUpperCase()},'k');
  expect(uppercase.playerId).toBe(input.playerId);
  expect(uppercase.walletId).toBe(input.walletId);
  expect(uppercase.payloadHash).toBe(a.payloadHash);
  for(const field of ['providerId','externalTransactionId','roundId','gameId'] as const)
    expect(parseWager({...input,[field]:input[field].toUpperCase()},'k').payloadHash).not.toBe(a.payloadHash);
});
test('strict input rejects missing header, body key fallback and malformed DTOs',()=>{
  const input=body();
  for(const key of [undefined,null,'',' ',123]) expect(()=>parseWager(input,key)).toThrow('IDEMPOTENCY_KEY_REQUIRED');
  const invalid:unknown[]=[null,[],{}, {...input,idempotencyKey:'body-key'}, {...input,kind:'OPENING'},
    {...input,referenceExternalTransactionId:null},{...input,referenceExternalTransactionId:'x'},
    {...input,playerId:'player'}, {...input,playerId:input.playerId+'\n'}, {...input,walletId:input.walletId+'\n'},
    {...input,money:{amount:'1.00',currency:1}},{...input,money:{amount:'1.00',currency:null}},
    {...input,money:{amount:'1.00',currency:'BRL',extra:1}},
    ...[1,'1.0','01.00','1.001','1e2','-1.00','NaN','Infinity','0.00','1000000000000000000.00'].map(amount=>({...input,money:{amount,currency:'BRL'}})),
    {...input,kind:'LOSS',money:{amount:'1.00',currency:'BRL'}}];
  for(const value of invalid) expect(()=>parseWager(value,'k')).toThrow('INVALID_PAYLOAD');
  for(const kind of ['REFUND','ROLLBACK']) expect(()=>parseWager({...input,kind},'k')).toThrow('REFERENCE_REQUIRED');
  expect(parseWager({...input,kind:'LOSS',money:{amount:'0.00',currency:'BRL'}},'k').money.isZero()).toBe(true);
});

test('external submissions cannot use the reserved internal provider namespace',()=>{
  for(const providerId of ['internal:wallet-opening','internal:other'])
    expect(()=>parseWager({...body(),providerId},'external-key')).toThrow('INVALID_PAYLOAD');
});

test('unpersistable text is rejected before any store call; valid Unicode is preserved', async()=>{
  let calls = 0;
  const useCase = new ProcessWager({ process: async()=>{ calls++; throw new Error('Store must not be called'); } });
  for (const value of ['bad\u0000value', 'bad\uD800value', 'bad\uDC00value']) {
    for (const field of ['providerId', 'externalTransactionId', 'roundId', 'gameId', 'referenceExternalTransactionId']) {
      await expect(useCase.execute({...body(), kind:'WIN', [field]:value},'key'))
        .rejects.toMatchObject({code:'INVALID_PAYLOAD',httpStatus:400});
    }
    await expect(useCase.execute(body(),value)).rejects.toMatchObject({httpStatus:400});
    await expect(useCase.execute(body(),'key',value)).rejects.toMatchObject({code:'INVALID_PAYLOAD',httpStatus:400});
  }
  expect(calls).toBe(0);
  expect(parseWager({...body(),providerId:'provedor-á-😀'},'chave-á-😀').providerId).toBe('provedor-á-😀');
});

test('idempotency keys preserve opaque bytes and reject invalid shape before persistence',()=>{
  expect(parseWager(body(),'p:transaction-123').idempotencyKey).toBe('p:transaction-123');
  expect(parseWager(body(),'a'.repeat(128)).idempotencyKey).toHaveLength(128);
  for(const key of ['a'.repeat(129),'\u0000','\uD800','   '])
    expect(()=>parseWager(body(),key)).toThrow('IDEMPOTENCY_KEY_REQUIRED');
});

test('moedas habilitadas passam; desconhecidas, minúsculas e fora da allowlist recebem UNSUPPORTED_CURRENCY',()=>{
  const input=body();
  for(const currency of ['BRL','USD','EUR']) expect(parseWager({...input,money:{amount:'1.00',currency}},'k').money.currency).toBe(currency);
  expect(parseWager({...input,money:{amount:'1.00',currency:'USD'}},'k').payloadHash)
    .not.toBe(parseWager(input,'k').payloadHash);
  for(const currency of ['brl','usd','XYZ','BTC','BR L',' BRL','BRL ','']) expect(()=>parseWager({...input,money:{amount:'1.00',currency}},'k')).toThrow('UNSUPPORTED_CURRENCY');
  // Formato do valor continua INVALID_PAYLOAD em moeda válida.
  expect(()=>parseWager({...input,money:{amount:'1.001',currency:'USD'}},'k')).toThrow('INVALID_PAYLOAD');
  try {
    configureCurrencies({JWP_ENABLED_CURRENCIES:'BRL'});
    expect(()=>parseWager({...input,money:{amount:'1.00',currency:'USD'}},'k')).toThrow('UNSUPPORTED_CURRENCY');
    expect(parseWager(input,'k').money.currency).toBe('BRL');
  } finally { configureCurrencies({}); }
});
