# Contrato HTTP

Rotas, formatos, status e códigos de erro da API. Decisões por trás do mapeamento: [ARCHITECTURE.md](../ARCHITECTURE.md) (JWP-ADR-004, 005 e 009). Catálogo de rotas em [contracts/http-api.json](../contracts/http-api.json).

## Regras de entrada

- JSON com campos conhecidos e tipos estritos; campos desconhecidos são recusados. Strings não podem ser vazias.
- Dinheiro é sempre `{ "amount": "25.00", "currency": "BRL" }`: string com exatamente duas casas, sem sinal, expoente, espaços ou zeros à esquerda. Número JSON, `"1.5"`, `"1e2"`, `"-1.00"`, `"01.00"` e `"1.000"` são recusados com 400. A API habilita BRL, USD e EUR, todas com escala 2 (registro em `src/shared/domain/currency.ts`); `JWP_ENABLED_CURRENCIES` (ex.: `BRL,USD`) restringe esse conjunto e é validada no boot. Moeda fora do registro ou da allowlist, inclusive em minúsculas (`"brl"`), recebe 400 `UNSUPPORTED_CURRENCY`; moeda habilitada diferente da moeda da wallet recebe 400 `CURRENCY_MISMATCH`, sem gravar nada, por HTTP ou SQS. Cada player tem no máximo uma wallet por moeda; não há conversão cambial.
- `playerId`, `walletId` e `transactionId` são UUIDs (comparados sem diferenciar maiúsculas). `providerId`, `externalTransactionId`, `roundId` e `gameId` são identificadores opacos, sensíveis a maiúsculas.
- `POST /wagering/transactions` exige o header `Idempotency-Key`. O formato `provider:external` é só recomendação; a chave não é gerada nem lida do body e não entra no hash do payload.
- Body da transação: `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind` e `money` obrigatórios. `referenceExternalTransactionId` é obrigatório para REFUND e ROLLBACK, opcional para WIN e proibido para BET e LOSS; `null` ou string vazia são inválidos. OPENING não pode ser enviado. BET, WIN, REFUND e ROLLBACK têm valor positivo; LOSS usa exatamente `"0.00"`.
- O formato é validado antes de qualquer efeito. Wallet inexistente é 404; player ou moeda incompatíveis com a wallet são 400. Nenhuma transação é criada para esses casos. Rejeições de saldo e de referência acontecem depois da aceitação e ficam persistidas com snapshot e evento.

## Rotas

| Rota | Entrada | Sucesso |
|---|---|---|
| `POST /wallets` | `playerId`, `initialBalance` | 201: `id`, `playerId`, `balance`, `version` = 1 |
| `GET /wallets/:walletId` | UUID | 200: `id`, `playerId`, `balance`, `version`, `createdAt`, `updatedAt` |
| `GET /wallets/:walletId/ledger?cursor=&limit=` | cursor opaco; `limit` 1–100 (padrão 50) | 200: `items`, `nextCursor` (string ou `null`) |
| `POST /wallets/:walletId/reconciliation` | UUID; body vazio | 200: `storedBalance`, `calculatedBalance`, `difference`, `consistent`, `checkedEntries` |
| `POST /wagering/transactions` | header e body acima | 200 PROCESSED, 202 PENDING_REFERENCE ou 422 REJECTED |
| `GET /wagering/transactions/:transactionId` | UUID | 200: estado persistido |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | identidade externa | 200: mesma representação da consulta por id |
| `GET /health/live` | — | 200 enquanto o processo está vivo |
| `GET /health/ready` | — | 200 com dependências alcançáveis; 503 caso contrário |
| `GET /metrics` | — | 200, formato de exposição Prometheus |

Datas são ISO-8601 em UTC. Health e `/metrics` não exigem autenticação (ver JWP-ADR-010).

**Ledger.** Ordenado de forma ascendente por `walletVersion`. Cada item tem `id`, `walletId`, `transactionId`, `walletVersion`, `direction`, `money`, `balanceBefore`, `balanceAfter` e `createdAt`. A paginação usa o cursor devolvido, nunca offset; cursor inválido ou de outra wallet é 400 `INVALID_CURSOR`. Lançamentos novos recebem versões maiores e os anteriores não mudam.

**Consulta de transação.** `transactionId`, `providerId`, `externalTransactionId`, `walletId`, `playerId`, `roundId`, `gameId`, `kind`, `money`, `status`, `createdAt` e, quando existirem, `referenceExternalTransactionId`, `referenceTransactionId`, `failureCode`, `processedAt` e `resultBalance`. A consulta não devolve `idempotentReplay` (indicador da submissão) nem a chave ou o hash.

## Exemplos

Abrir uma wallet:

```http
POST /wallets
Content-Type: application/json

{ "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", "initialBalance": { "amount": "100.00", "currency": "BRL" } }
```

```json
{ "id": "0192f291-27dd-7d3f-8071-5f8685deef37", "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", "balance": { "amount": "100.00", "currency": "BRL" }, "version": 1 }
```

Uma aposta:

```http
POST /wagering/transactions
Idempotency-Key: provider-a:transaction-123
Content-Type: application/json

{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}
```

200:

```json
{ "transactionId": "0192f298-345e-7e38-af88-e43f851a819d", "status": "PROCESSED", "balance": { "amount": "75.00", "currency": "BRL" }, "idempotentReplay": false }
```

Repetir a mesma requisição devolve o mesmo 200 e o mesmo corpo, só com `"idempotentReplay": true`, mesmo que o saldo atual já seja outro: o replay devolve o resultado original. O replay de uma rejeição mantém o 422, o `failureCode` e o saldo daquele momento.

202, referência ainda não recebida (REFUND antes da BET):

```json
{ "transactionId": "0192f298-345e-7e38-af88-e43f851a819d", "status": "PENDING_REFERENCE", "idempotentReplay": false }
```

Uma resposta pendente não traz saldo. O scheduler reprocessa a transação; um replay com a mesma chave devolve o estado atual, inclusive o terminal quando houver.

422, aposta acima do saldo:

```json
{ "transactionId": "0192f298-345e-7e38-af88-e43f851a819d", "status": "REJECTED", "failureCode": "INSUFFICIENT_FUNDS", "balance": { "amount": "20.00", "currency": "BRL" }, "idempotentReplay": false }
```

Reconciliação:

```json
{ "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37", "storedBalance": { "amount": "75.00", "currency": "BRL" }, "calculatedBalance": { "amount": "75.00", "currency": "BRL" }, "difference": { "amount": "0.00", "currency": "BRL" }, "consistent": true, "checkedEntries": 2 }
```

`difference = storedBalance − calculatedBalance` e pode ser negativa, porque é um valor derivado. Uma divergência devolve `consistent: false` com 200 (a verificação terminou), gera log e métrica e nunca é corrigida automaticamente.

## Erros

Erros anteriores à aceitação usam o envelope abaixo, sem `transactionId` nem status financeiro:

```json
{ "error": { "code": "INVALID_PAYLOAD", "message": "INVALID_PAYLOAD", "retryable": false }, "correlationId": "45c54c0b-b284-4abf-b242-9c8cba7e5a41" }
```

O cliente decide pelo `code`; `message` é informativo e não deve ser interpretado. O envelope não repete o payload recebido.

No contrato atual, o 400 não identifica o campo inválido e `message` repete `code`. Detalhes estruturados de campo/caminho exigem uma evolução do contrato e validação consistente entre endpoints; não são fornecidos por esta versão.

| HTTP | Código | Estado durável | O que o cliente faz |
|---|---|---|---|
| 400 | `INVALID_PAYLOAD`, `UNSUPPORTED_CURRENCY`, `IDEMPOTENCY_KEY_REQUIRED`, `REFERENCE_REQUIRED`, `PLAYER_MISMATCH`, `CURRENCY_MISMATCH`, `INVALID_CURSOR` | Nada foi aceito | Corrigir o contrato ou o contexto |
| 404 | `WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND` | Recurso inexistente | Verificar a identidade |
| 409 | `IDEMPOTENCY_CONFLICT` | A operação original continua intacta | Não reenviar payload diferente com a mesma chave |
| 409 | `EXTERNAL_IDENTITY_CONFLICT` | A operação original continua intacta | Consultar por provider/external e recuperar a chave original |
| 409 | `WALLET_ALREADY_EXISTS` | A wallet existente continua intacta | Consultar a wallet existente |
| 422 | `INSUFFICIENT_FUNDS` | REJECTED com evento | Replay não reavalia o saldo |
| 422 | `REVERSAL_INSUFFICIENT_FUNDS` | REJECTED com evento | Reversão que deixaria saldo negativo; tratar à parte da aposta sem saldo |
| 422 | `REFERENCE_CONTEXT_MISMATCH`, `REFERENCE_KIND_INVALID`, `REFERENCE_NOT_PROCESSED`, `REVERSAL_AMOUNT_MISMATCH`, `ALREADY_REVERSED`, `SELF_REFERENCE` | REJECTED com evento | Corrigir a causa; a mesma identidade continua terminal |
| 422 | `REFERENCE_EXPIRED` | REJECTED com evento | A referência não chegou no prazo; não há reabertura automática |
| 422 | `MONETARY_RANGE_EXCEEDED` | REJECTED com evento | O efeito excederia a faixa monetária; nada é truncado |
| 503 | `INFRASTRUCTURE_UNAVAILABLE` | Nenhum resultado confirmado na resposta | Repetir com a **mesma** chave; um commit incerto se resolve pelo replay |
| 500 | `PERMANENT_INFRASTRUCTURE_FAILURE` | FAILED auditável (JWP-ADR-011) | Terminal, `retryable: false`; requer diagnóstico |

Uma resposta 503 perdida ou um timeout não provam que nada foi gravado: o cliente sempre preserva a chave e repete. Para FAILED, a submissão traz `transactionId`, `status`, `failureCode` e `error.retryable = false`; a consulta por id continua devolvendo 200 com o estado persistido.

As mesmas regras valem para comandos recebidos pela fila (`wager-transactions.fifo`): o consumer usa o mesmo caso de uso; rejeições de negócio são confirmadas e erros de contrato vão para a DLQ após o limite de tentativas ([contracts/messaging](../contracts/messaging/README.md)).

## Formato de Idempotency-Key

A aplicação aceita uma string opaca de até 128 unidades UTF-16, não vazia após trim, sem NUL nem substitutos UTF-16 isolados. Não recomputa, normaliza nem remove espaços da chave recebida. O transporte HTTP pode normalizar whitespace do header antes de entregá-lo à aplicação. `{providerId}:{externalTransactionId}` é o formato recomendado, não uma expressão obrigatória: o banco aplica a unicidade no escopo do provider.

Chave ausente, vazia, de tipo errado ou inválida retorna 400 `IDEMPOTENCY_KEY_REQUIRED`. Uma chave válida com payload divergente retorna 409 `IDEMPOTENCY_CONFLICT`. O header é a fonte da verdade no HTTP; uma chave no body não é fallback. No SQS, data.idempotencyKey passa pela mesma validação do caso de uso.

## Readiness e topologias

`GET /health/ready` exige PostgreSQL e SQS. Resposta pronta: HTTP200, status ready e ambas as checks up. SQS ausente, inacessível ou não configurado retorna HTTP503, status unavailable; not_configured é diagnóstico, nunca autorização para ready200. `/health/live` permanece público e independente dessas dependências.
