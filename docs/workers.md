# Operação HTTP e workers

O serviço completo usa um processo NestJS com HTTP, consumer de comandos SQS,
publisher da outbox e scheduler de referências. Cada instância usa os mesmos
PostgreSQL e filas. Claims SQL com leases/SKIP LOCKED e locks por wallet permitem
replicar o comando em três ou mais processos, sem coordenador de workers adicional.

Prepare PostgreSQL e SQS reais:

```bash
docker compose -p jwp-db -f compose.db.yml up -d --wait
docker compose -p jwp-sqs -f compose.sqs.yml up -d --wait
bun install --frozen-lockfile
bun run build
JWP_MIGRATION_DATABASE_URL=postgresql://jwp:jwp_local@127.0.0.1:55432/jwp bun run db:migrate
```

O operador cria um LOGIN exclusivo, concede `jwp_runtime` e fornece sua URL em
`JWP_DATABASE_URL`. O LOGIN runtime não deve ser owner ou superuser. Migrations
usam credenciais próprias; o processo de aplicação não executa DDL. Exemplo local
(após as migrations), escolhendo uma senha própria:

```sql
CREATE ROLE jwp_api LOGIN PASSWORD 'replace_me';
GRANT jwp_runtime TO jwp_api;
```

Execute esse SQL pelo owner, por exemplo com
`docker compose -p jwp-db -f compose.db.yml exec postgres psql -U jwp -d jwp`.

Configure as variáveis de `.env.example` com as URLs reais obtidas por
`aws sqs get-queue-url` (ou `awslocal` dentro do container LocalStack):

```bash
export JWP_DATABASE_URL='postgresql://jwp_api:replace_me@127.0.0.1:55432/jwp'
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export JWP_SQS_ENDPOINT=http://127.0.0.1:54566
export JWP_SQS_COMMAND_QUEUE_URL=http://127.0.0.1:54566/queue/us-east-1/000000000000/wager-transactions.fifo
export JWP_SQS_EVENT_QUEUE_URL=http://127.0.0.1:54566/queue/us-east-1/000000000000/wager-events.fifo
bun run start:workers
```

As credenciais `test` e o endpoint acima são exclusivos do LocalStack. Em AWS,
omita o endpoint e use a cadeia de credenciais do SDK. Filas de comandos e eventos
são FIFO, distintas e provisionadas pela infraestrutura. O serviço não consome a
fila de eventos de integração: ela pertence aos assinantes.

MessageGroupId: nossos produtores (`sendWagerTransactionRequested` e o publisher
da outbox) usam `walletId`, para que o FIFO entregue em ordem as operações de uma
mesma wallet. O consumer de comandos aceita qualquer MessageGroupId do produtor
(por provider, rodada etc.): a ordenação do broker é otimização, e a correção vem
do lock por wallet, da idempotência e do inbox no PostgreSQL. Um group diferente
de `walletId` nunca é motivo de DLQ. `MessageDeduplicationId` recomendado: o
`messageId` do envelope.

Retry e DLQ do consumer de comandos:

| Situação | Visibilidade devolvida | Destino |
|---|---|---|
| Sucesso, rejeição de negócio ou PENDING_REFERENCE | — | ack após o commit |
| Falha transitória (PostgreSQL indisponível, lock/statement timeout) | `min(60, 3·2^(ApproximateReceiveCount−1))` s: 3, 6, 12, 24, 48 | nova tentativa |
| Falha permanente (envelope inválido, conflito de idempotência, wallet inexistente) | 1 s | DLQ pela redrive policy após `maxReceiveCount=5` |

Com `maxReceiveCount=5`, os recebimentos transitórios ocorrem em ~0, 3, 9, 21 e
45 s (nominal; ~49 s medido, com polling e processamento). São dois prazos distintos:

- a dependência precisa voltar até a 5ª tentativa (~45–49 s) para o comando ser
  processado sem passar pela DLQ;
- se a 5ª tentativa também falha, ela devolve mais 48 s de visibilidade e a redrive
  move a mensagem à DLQ no recebimento seguinte: ~93 s nominal, ~98 s medido
  localmente após o primeiro recebimento. A partir daí,
  o comando válido exige redrive manual da DLQ.

Enquanto a mensagem espera no batch ou é processada, um heartbeat renova a
visibilidade (30 s a cada 10 s), para que outra instância não a receba de novo. No
LocalStack, o receipt FIFO expira pela visibilidade original mesmo após a
renovação; se o ack posterior ao commit for recusado, a reentrega é absorvida pelo
inbox, sem segundo efeito.

`start:workers` equivale a `APP_ROLES=http,workers bun dist/main.js`. `bun run start`
usa `APP_ROLES=http` por padrão; `APP_ROLES=workers bun run start` opera somente os
workers internos. Para três instâncias completas, execute o comando em três
terminais com `PORT=3037`, `PORT=3038` e `PORT=3039`, compartilhando DB/filas.
`HOST` padrão é `127.0.0.1`; use `HOST=0.0.0.0` em containers.

Configuração inválida falha no bootstrap: roles desconhecidas/duplicadas, URLs
FIFO ausentes/iguais e intervalo fora de 1–60000 ms. `JWP_WORKER_INTERVAL_MS`
(padrão 250 ms) controla a pausa de batches vazios/parciais ou com falha; batches completos com todos os ACKs repetem imediatamente. Não altera o backoff durável
nem o TTL de referência de 15 minutos. O consumer mantém long polling de 20s.

Health é público: `/health/live` mede processo; `/health/ready` consulta PostgreSQL
e ambas as filas usadas pelos workers. `JWP_SQS_QUEUE_URLS` acrescenta filas/DLQs
para readiness e profundidade em `/metrics`. Falhas de operação geram logs JSON,
e os adapters observados registram consumo, publicação, retries e pendências.
Workers não possuem rotas públicas próprias.

SIGTERM/SIGINT param novas buscas/claims. O consumer aborta o long poll, espera
até 15s pelo processamento e devolve visibilidade de receipts ainda incompletos;
o processo aguarda operações SQL e batches de publicação ativos antes de fechar
SQS/ORM. Um deadline global de 30s termina com exit 1 e `shutdown.timeout` se não
houver drain. Exit 0 e `workers.stopped` indicam fechamento limpo. SIGKILL depende
de redelivery/expiração das leases; eventos duráveis restantes são publicados no
restart. Não há promessa de entrega exatamente uma vez no SQS.

Prova de operação, com banco descartável vazio e SQS acessível (sem skip):

```bash
bun run build
JWP_RUNTIME_TEST_DATABASE_URL=postgresql://OWNER:PASSWORD@127.0.0.1:PORT/EMPTY_DB \
JWP_SQS_ENDPOINT=http://127.0.0.1:54566 \
bun test tests/integration/runtime --timeout 60000
bun run check
```

A suíte aplica `db:migrate`, cria LOGIN sem DDL e filas isoladas, inicia três
`start:workers`, envia HTTP/SQS reais, verifica `published_at` e eventos no SQS,
observa retry e conclusão de PENDING_REFERENCE, e usa lock SQL como barreira para
SIGTERM durante processamento. Ela remove seus dados/filas/LOGIN ao terminar.

## Falha permanente conhecida na última tentativa

`FAILED` é um resultado terminal confirmado (contrato em [HTTP.md](HTTP.md), decisão em JWP-ADR-011): identidade válida, banco disponível e diagnóstico positivo de falha permanente. A allowlist atual reconhece apenas SQLSTATE `0A000` (`feature_not_supported`), por código do driver. Não classifica por texto. Deadlock, lock timeout, conexão indisponível, violações de integridade e erros desconhecidos não viram `FAILED` por esgotarem tentativas.

- HTTP: até as 3 tentativas SQL configuradas na política financeira. Depois do rollback da última falha `0A000`, uma TX nova, dentro do orçamento restante, bloqueia a wallet e relê a identidade. Um resultado confirmado anteriormente vence; caso contrário grava `FAILED`, `PERMANENT_INFRASTRUCTURE_FAILURE`, snapshot HTTP500 com `error.retryable=false` e `WagerTransactionFailed` na outbox. Não altera saldo/versão/ledger.
- SQS: o consumer consulta `RedrivePolicy.maxReceiveCount` da fila ao iniciar os polls. Falha conhecida em entrega anterior ao limite continua no caminho permanente/visibilidade curta. Na entrega final, o mesmo serviço de aplicação grava resultado e inbox na mesma TX; ACK somente depois do commit. A topologia padrão tem limite 5; filas de teste com 2/3 provam que o consumer usa o valor real. Sem política de redrive, o consumer não inventa uma entrega final.
- Scheduler: falha `0A000` é auditada no terceiro claim (`permanentFailureAttempts`, padrão3). Claims anteriores mantêm a recuperação por lease; a última tentativa usa nova TX e revalida token/lease/identidade antes de gravar `FAILED` e excluir o job. O limite não converte indisponibilidade transitória em falha terminal nem altera o TTL de referência.

O evento adicional `WagerTransactionFailed` (v1) leva código estável, SQLSTATE, tentativas e saldo/versão auditados; o consumidor de referência aceita e deduplica esse contrato. Replay retorna o snapshot original, inclusive depois de outra movimentação; GET da transação continua HTTP200. Referenciar uma transação FAILED resulta em rejeição de referência não processada.

Se a TX de auditoria falhar, não há resultado financeiro confirmado e o consumer não dá ACK nem fabrica `FAILED`; a mensagem permanece sob retry/redrive e pode chegar à DLQ. Os testes reais preservam essa diferença, inclusive falha controlada no INSERT de auditoria. Provas: `tests/integration/wager/permanent-failure.test.ts`.

## Consumer sem pausa sob backlog

O processamento de cada batch continua **sequencial**. Quando o poll entrega o
batch completo (10 mensagens, opção padrão) e todas terminam em ACKNOWLEDGED,
o próximo receive começa imediatamente. Batch vazio, parcial, RETRY, PERMANENT
ou erro mantém a pausa configurada por JWP_WORKER_INTERVAL_MS (250 ms por padrão).
O long poll continua em 20 s; heartbeat, ACK pós-commit, inbox e DLQ não mudaram.
Não há paralelismo entre receipts ou grupos.

Medição diagnóstica com PostgreSQL 17.6 e LocalStack 4.9.2 reais, Bun 1.3.14,
Intel i7-11800H e 16 CPUs lógicas. Um ApplicationWorkers real, pool de quatro
conexões, publisher e scheduler existentes ativos e inalterados. Pré-enfileirados
200 comandos por rodada: 10 wallets, 20 BET/WIN alternados por wallet,
MessageGroupId=walletId. O tempo começa ao iniciar o worker e termina no 200º ACK.
Criação/envio, duplicate probe e drain final da outbox estão fora dessa janela.
É um diagnóstico de backlog, não o mix de carga nem um SLA de produção.

A comparação usa o loop anterior (pausa de 250 ms depois de todo batch) contra o
atual, com os mesmos módulos SQL/SQS. Três rodadas por variante, no mesmo host,
que também executava outras cargas:

- Antes: 29.27, 28.36, 31.88 msg/s; mediana **29.27 msg/s**.
- Depois: 100.79, 118.62, 115.79 msg/s; mediana **115.79 msg/s**.
- Ganho observado de mediana: **3.96×**. Há variação entre rodadas no host
  compartilhado; não extrapolar esses números para AWS ou mais réplicas.

Cada rodada verifica ordem exata de externalTransactionId no ledger por versão
(20 comandos por wallet), saldo 100.00/versão 21, 21 lançamentos, inbox processado,
ACK de todas as mensagens e DLQ vazia. Uma entrega com dedup ID do broker novo,
mas messageId lógico repetido, prova que o inbox impede um segundo efeito.
Regressões PG+SQS: inbox 8 testes/55 asserts (poison/DLQ, falha de ACK, corrida
HTTP/SQS e SIGTERM); resiliência 4/47 (PG fora por mais de 20 s, backoff,
heartbeat e grupo arbitrário); leak de heartbeat 2/8. Check global passou com
97 unitários/568 asserts e 2 smoke/5 asserts. Ausência de infra não gera skip.

Reprodução em PG vazio e SQS próprio:

```bash
JWP_CONSUMER_DATABASE_URL=postgresql://OWNER:PASSWORD@127.0.0.1:PORT/EMPTY_DB \
JWP_SQS_ENDPOINT=http://127.0.0.1:SQS_PORT \
JWP_CONSUMER_BENCH_ROUNDS=3 JWP_CONSUMER_BENCH_MESSAGES=200 \
JWP_CONSUMER_BENCH_LABEL=after \
bun test tests/integration/runtime/consumer-backlog.test.ts --timeout 360000
```

Logs e fingerprints: gravados pelo teste no diretório de saída local. O diagnóstico prepara/reverte migrations
no banco vazio e remove suas próprias filas. Não rodar em paralelo com outra
migration que cria/remove a mesma role no mesmo cluster.
