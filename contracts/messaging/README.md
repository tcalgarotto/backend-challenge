# Contratos de mensageria

Origem: docs/CHALLENGE.md seções 10 e 11; decisões em ARCHITECTURE.md (JWP-ADR-006 e 007). Provados por tests/integration/sqs (`bun run test:sqs`).

| Arquivo | Conteúdo |
|---|---|
| topology.json | Filas, atributos, redrive e regras de produtor/consumidor |
| wager-transaction-requested.schema.json | Envelope de comando em wager-transactions.fifo |
| integration-event.schema.json | Envelope IntegrationEvent.toJSON() e data dos quatro eventos mínimos |
| examples/*.json | Exemplos válidos e variações inválidas (patch por caminho; null remove o campo) |

## Comando

Envelope `messageId/type/occurredAt/data`, type `WagerTransactionRequested`. Campos desconhecidos são rejeitados. Money é string com escala 2; número, expoente, sinal, zero à esquerda e faixa acima de 999999999999999999.99 são inválidos. OPENING não é submetível. REFUND/ROLLBACK exigem referência; BET/LOSS não a aceitam; LOSS é 0.00 e as demais operações são positivas. Esses casos são erro de contrato, sem wager fictício: no SQS vão à DLQ após maxReceiveCount.

Produtor controlado: MessageGroupId = data.walletId; MessageDeduplicationId = messageId. O MessageId da AWS e o receiptHandle nunca substituem o messageId lógico da inbox.

## Eventos

Destino `wager-events.fifo`, separado dos comandos. MessageGroupId = data.walletId; MessageDeduplicationId = eventId. version = 1 em todos. data leva MoneyProps, nunca a instância Money. Retry reenvia o envelope idêntico (mesmo eventId e occurredAt); o consumidor de referência (ARCHITECTURE.md, JWP-ADR-007) deduplica eventId de forma durável.

| eventType | aggregateId | Quando | Saldo em data |
|---|---|---|---|
| WagerTransactionProcessed | transactionId | Toda aplicada, inclusive LOSS e OPENING positivo | balance e walletVersion observados sob lock |
| WagerTransactionRejected | transactionId | REJECTED por regra de negócio, inclusive REFERENCE_EXPIRED | balance observado; failureCode dos 422 de docs/HTTP.md |
| WalletBalanceChanged | walletId | Somente com mudança de saldo | balanceBefore/balanceAfter/walletVersion |
| WagerTransactionPendingReference | transactionId | Entrada em PENDING_REFERENCE, uma vez | expiresAt do TTL |

OPENING não tem providerId/externalTransactionId/roundId/gameId obrigatórios. As subclasses de evento estão em `src/wagering/events` e `src/wallet/events`; schema, exemplos e testes mudam juntos.

## Implementação

- Consumer de comandos: `src/messaging/sqs-wager-consumer.ts`, com inbox durável por messageId (`src/messaging/sql-inbox-process-wager-store.ts`) na mesma transação do efeito financeiro.
- Outbox: eventos gravados na transação do comando e publicados após o commit por `src/messaging/outbox/sqs-outbox-publisher.ts` (claim/lease em `sql-outbox-store.ts`).
- Scheduler de referências pendentes: `src/messaging/pending-reference-scheduler.ts`.
- Composição: `src/runtime/workers.ts` (`APP_ROLES=workers` ou `http,workers`). Operação, retry e DLQ em [docs/workers.md](../../docs/workers.md).

FIFO e a janela de dedup de 5 minutos do SQS são otimização; constraints do PostgreSQL e a inbox continuam sendo a garantia.

`WagerTransactionFailed` é o evento adicional v1 de falha permanente conhecida e confirmada. O schema exige `PERMANENT_INFRASTRUCTURE_FAILURE`, diagnóstico `SQL_FEATURE_NOT_SUPPORTED`, SQLSTATE `0A000`, número de tentativas (1–1000), saldo/versão sem mudança e `failedAt`. Não é evento de retry transitório. A identidade do evento e o envelope continuam estáveis nas republicações.
