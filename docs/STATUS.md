# Estado do produto

Atualização: 12/10/2026. Este documento descreve o que funciona hoje, como executar e quais são os limites conhecidos.

## O que funciona

| Capacidade | Situação |
|---|---|
| Operações | BET, WIN, LOSS, REFUND e ROLLBACK via HTTP e via SQS FIFO |
| Dinheiro exato | Money em bigint (centavos); decimal string em HTTP/eventos, `numeric` no PostgreSQL; nunca `number` |
| Wallet, ledger e reconciliação | Ledger auditável; saldo = soma do ledger; consultas de wallet e transação |
| Concorrência | Lock por wallet; wallets distintas processam em paralelo |
| Idempotência | Persistente por `provider_id`/`Idempotency-Key`; replay devolve o snapshot terminal |
| Reversão | No máximo uma reversão PROCESSED por referência (REFUND+ROLLBACK somados); segunda vira `ALREADY_REVERSED`/422 |
| Mensageria | Inbox de comandos, outbox de eventos (at-least-once), scheduler de referências pendentes, retry/backoff e DLQ |
| Múltiplas instâncias | N réplicas `http,workers` no mesmo banco e filas; ack só após commit; lease/fencing nos workers |
| Recuperação | Kill após commit e antes do ack resulta em redelivery sem duplicar efeito |
| Schema | Migrations versionadas 002–009 (`bun run db:migrate`); runtime sem DDL e sem superuser |
| Operação | `/health/live`, `/health/ready` (PG e SQS), `/metrics`, logs JSON com `correlationId` |
| Auth | Ponto de extensão `ProviderIdentityPort`/`ProviderAuthGuard`, em modo local no-op |

## Como executar

Stack completa em um comando (PostgreSQL + LocalStack SQS + migrations + 3 réplicas):

```bash
docker compose up -d --build --wait
PORT=$(docker compose port --index 1 app 3037 | cut -d: -f2)
curl -s http://127.0.0.1:$PORT/health/ready
```

Réplicas, portas e variáveis: [referência operacional](workers.md#docker-compose-ordem-réplicas-e-variáveis). Execução sem contêiner da aplicação (Bun local): [workers.md](workers.md#execução-local-sem-contêiner-da-aplicação).

Verificação:

| Comando | Escopo |
|---|---|
| `bun run check` | Typecheck, testes unitários, build e smoke HTTP/DI |
| `bun run test:integration` (ou `bun scripts/reproduce/reproduce.sh`) | Matriz, integração, concorrência e recuperação em PostgreSQL/SQS descartáveis. Sufixo `JWP_REPRO_SUFFIX` / `COMPOSE_PROJECT_NAME` isola containers; portas em `JWP_REPRO_*_PORT` |
| `bun run test:load` | Teste de carga com N réplicas; ver abaixo |

## Teste de carga

Metodologia, resultados e ressalvas em [LOAD_TEST.md](LOAD_TEST.md). Medição local, uma rodada por configuração, sem meta de RPS nem SLA.

## Regras de negócio relevantes

- LOSS exige `"0.00"` BRL e nenhuma referência ([HTTP.md](HTTP.md)).
- Reversão única por referência; ROLLBACK de REFUND é permitido, ROLLBACK de ROLLBACK não ([ARCHITECTURE.md](../ARCHITECTURE.md), JWP-ADR-005).

## Pacote de entrega

A cópia de avaliação é gerada fora deste repositório e não inclui notas de processo.

## Limitações

- Sem IdP real: o modo local aceita qualquer chamada e não autentica o `providerId`.
- `APP_ROLES=http` persiste operações e outbox, mas só `workers` drena filas, publica a outbox e retoma referências pendentes.
- Publicação de eventos é at-least-once, em ordem de commit por wallet (não entre wallets); consumidores devem deduplicar por `eventId`.
- Sob carga a outbox acumula lag e uma wallet muito disputada serializa as operações.
- A carga foi medida em host compartilhado com LocalStack; não representa AWS nem produção.
- Sem implantação pública; a stack Compose é apenas para avaliação local.
