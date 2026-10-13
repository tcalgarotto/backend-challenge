# Distributed Wagering Processor

Implementação do desafio de backend da Jungle Gaming. Processa BET, WIN, LOSS, REFUND e ROLLBACK com dinheiro exato, lock por wallet, idempotência persistente, ledger auditável e eventos duráveis. O enunciado está em [docs/CHALLENGE.md](docs/CHALLENGE.md), snapshot do upstream `c7143e6d041585d6913f56eb7415105e074f83dc`.

A entrega cobre HTTP, persistência financeira, mensageria SQS (consumer, outbox e scheduler), readiness, métricas e logs correlacionados. Não há IdP real (ponto de extensão documentado). Contrato da API: [docs/HTTP.md](docs/HTTP.md). Decisões: [ARCHITECTURE.md](ARCHITECTURE.md). Teste de carga, sem meta de RPS nem SLA: [docs/LOAD_TEST.md](docs/LOAD_TEST.md).

## Stack

- Bun 1.3.14, TypeScript 5.9.3 (estrito), NestJS 11.2.7
- PostgreSQL 17.6 e MikroORM 6.6.17 (entidades, transações, migrations e LockMode; claims/SQL explícitos, JWP-ADR-014)
- LocalStack 4.9.2 para SQS FIFO local (`compose.yml`, que inclui `compose.db.yml` e `compose.sqs.yml`)

Versões em `package.json`/`bun.lock`. Domínio em `src/wallet` e `src/wagering` não importa NestJS nem ORM. Money é bigint em centavos; HTTP, eventos e PostgreSQL usam decimal string/`numeric`, nunca `number` monetário.

O mapa [eliminatória → mecanismo → teste](docs/CHALLENGE.md#mecanismos-e-provas-das-falhas-eliminatórias) permite conferir as garantias diretamente no código e nos cenários executáveis.

## Aplicação e domínio

HTTP, consumer SQS e scheduler usam o mesmo caso de uso (`WagerApplicationService`), que decide, grava snapshot e gera eventos. `WalletLedgerEntry`, `InboxMessage` e `OutboxMessage` são classes de domínio usadas nas escritas; os adapters cuidam de identidade, locks e transação. Detalhes em [ARCHITECTURE.md](ARCHITECTURE.md).

## Schema (migrations 002–009)

`bun run db:migrate` aplica as oito migrations reversíveis, com credencial owner (`JWP_MIGRATION_DATABASE_URL`). O processo da aplicação não executa DDL. `jwp_runtime` é role do cluster: a migration a cria se ausente (owner precisa de `CREATEROLE`) ou reutiliza uma role já provisionada pelo DBA após validar seus atributos; o down nunca a remove. Detalhes em [infra/postgres/README.md](infra/postgres/README.md).

| Migration | Conteúdo |
|---|---|
| `Migration202610110002` | Wallets, transações, ledger e barreiras de integridade |
| `Migration202610110003` | Outbox de eventos de integração |
| `Migration202610110004` | Reversão única por referência (índice sem `kind`) |
| `Migration202610120005` | Inbox de comandos, jobs de referência pendente e inbox de eventos |
| `Migration202610120006` | Timestamp durável de recebimento da inbox de comandos |
| `Migration202610120007` | Sequência da outbox (`seq`) para publicação em ordem por wallet |
| `Migration202610120008` | Índice `outbox_transaction_origin_idx` para buscar o evento de origem por transação |
| `Migration202610130009` | `wallets.updated_at`, backfill histórico e proteção de timestamps |

O runtime usa um LOGIN membro de `jwp_runtime`, sem superuser. Detalhe das barreiras: [ARCHITECTURE.md](ARCHITECTURE.md) ADR-003.

## Docker Compose (stack completa)

Um comando sobe PostgreSQL, LocalStack SQS (filas FIFO + DLQs), migrations, o LOGIN de runtime e N réplicas da aplicação (padrão 3):

```bash
docker compose up -d --build --wait
JWP_APP_REPLICAS=5 docker compose up -d --build --wait   # outra quantidade
docker compose ps                                       # portas de cada réplica
docker compose down -v                                  # derruba e apaga dados
```

Ordem: `postgres` saudável → `migrate` (`bun src/persistence/migrate.ts`, credencial owner) → `db-role` (cria `jwp_api` membro de `jwp_runtime`, idempotente) → `app` × N (após `sqs` saudável). Cada réplica escuta 3037 no contêiner; o host publica em `127.0.0.1:3037-3046` (uma porta por réplica, atribuída pelo Compose; a réplica 1 não fica necessariamente em 3037). Descubra a porta real com `docker compose port --index 1 app 3037` ou `docker compose ps`. Com `APP_ROLES=workers` o healthcheck não exige HTTP e `--wait` conclui com os workers em execução.

| Variável | Padrão | Uso |
|---|---|---|
| `JWP_APP_REPLICAS` | `3` | Número de réplicas `app` |
| `APP_ROLES` | `http,workers` | `http`, `workers` ou `http,workers` |
| `JWP_APP_PORT_RANGE` | `3037-3046` | Faixa de portas no host |
| `JWP_PG_PORT` | `55432` | PostgreSQL no host |
| `JWP_SQS_PORT` | `54566` | LocalStack no host |
| `JWP_WORKER_INTERVAL_MS` | `250` | Pausa do scheduler e dos ciclos de consumer/outbox vazios, parciais ou com falha; lotes cheios inteiramente confirmados continuam imediatamente |

Sem balanceador: chame qualquer réplica diretamente. `compose.db.yml`/`compose.sqs.yml` continuam utilizáveis isoladamente (usados por `scripts/load` e `scripts/reproduce`). O build usa `network: host` só para o `bun install`.

## Primeiros passos com a API

Com a stack do Compose no ar (requer `curl`, `jq` e `uuidgen`):

```bash
API=http://$(docker compose port --index 1 app 3037)
PLAYER=$(uuidgen)

# abrir uma wallet com 100.00
WALLET=$(curl -s -X POST $API/wallets -H 'content-type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" | jq -r .id)

# apostar 25.00 (repita o mesmo comando: a resposta volta igual, com idempotentReplay=true)
curl -s -X POST $API/wagering/transactions -H 'content-type: application/json' -H 'Idempotency-Key: provider-a:tx-1' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"tx-1\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-1\",\"gameId\":\"game-1\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

# saldo, ledger e reconciliação
curl -s $API/wallets/$WALLET
curl -s $API/wallets/$WALLET/ledger
curl -s -X POST $API/wallets/$WALLET/reconciliation -H 'content-type: application/json' -d '{}'
```

Payloads, status e códigos de erro: [docs/HTTP.md](docs/HTTP.md).

## Execução local

Requisitos: Bun 1.3.14, Docker Compose v2. Senhas abaixo são só de laboratório local.

```bash
bun install --frozen-lockfile
bun run check
bun run build
docker compose -p jwp-local -f compose.db.yml -f compose.sqs.yml up -d --wait
JWP_MIGRATION_DATABASE_URL=postgresql://jwp:jwp_local@127.0.0.1:55432/jwp bun run db:migrate
docker compose -p jwp-local -f compose.db.yml exec -T postgres psql -U jwp -d jwp \
  -c "CREATE ROLE jwp_api LOGIN PASSWORD 'jwp_api_local'; GRANT jwp_runtime TO jwp_api;"
```

A instalação com `--frozen-lockfile` usa as versões registradas em `bun.lock`. Os comandos acima verificam e compilam a aplicação antes de preparar o banco local.

`CREATE ROLE` é passo único em banco novo. Variáveis: [`.env.example`](.env.example). Bun carrega `.env` automaticamente no diretório de execução. `.env.example` é apenas um modelo: copie com `cp .env.example .env`; variáveis exportadas têm precedência. O LOGIN/senha do modelo corresponde ao comando acima.

O boot usa `loadEnvironment` em `src/config/env.ts`: valida papéis, banco, portas, timeouts e filas antes de abrir recursos. Uma configuração inválida gera `INVALID_ENVIRONMENT` com todos os campos/códigos encontrados, sem seus valores. Defaults e coerção numérica dos valores válidos permanecem iguais. `APP_ROLES=http` ignora o intervalo e as filas exclusivas dos workers; URLs adicionais de readiness continuam sendo validadas. As credenciais AWS seguem a cadeia do SDK.

Serviço completo (HTTP + consumer SQS + publisher da outbox + scheduler):

```bash
export JWP_DATABASE_URL='postgresql://jwp_api:jwp_api_local@127.0.0.1:55432/jwp'
export AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export JWP_SQS_ENDPOINT=http://127.0.0.1:54566
export JWP_SQS_COMMAND_QUEUE_URL=http://127.0.0.1:54566/queue/us-east-1/000000000000/wager-transactions.fifo
export JWP_SQS_EVENT_QUEUE_URL=http://127.0.0.1:54566/queue/us-east-1/000000000000/wager-events.fifo
bun run start:workers
```

`start:workers` equivale a `APP_ROLES=http,workers bun dist/main.js`. `bun run start` sobe só HTTP (`APP_ROLES=http`): persiste apostas e consultas, mas não drena filas, não publica a outbox e não retoma referências. `APP_ROLES=workers bun run start` sobe só os workers internos. Três instâncias: três terminais com `PORT=3037`, `3038` e `3039`, mesmo banco e mesmas filas. Sob carga, separar papéis (processos `APP_ROLES=http` e `APP_ROLES=workers`) dá a cada um seu pool e seu event loop; é a topologia usada no teste de carga. Operação, drain, SIGTERM, retry/backoff e DLQ: [docs/workers.md](docs/workers.md).

```bash
curl -s http://127.0.0.1:3037/health/live
curl -i http://127.0.0.1:3037/health/ready
curl -s http://127.0.0.1:3037/metrics
```

`/health/live` responde `{"status":"ok"}` sem consultar dependências. `/health/ready` exige PostgreSQL e SQS alcançáveis em qualquer topologia HTTP. Sem filas em `JWP_SQS_QUEUE_URLS` no modo só-HTTP, SQS aparece como `not_configured` e readiness responde 503 `unavailable`; o HTTP continua disponível, mas não está pronto pelo contrato. Com filas configuradas, falha em qualquer probe também dá 503. Health é público. Ctrl+C drena workers (quando ligados) e fecha o pool.

## Autenticação (modo local)

Não há Keycloak, login nem tabela de usuários. `ProviderIdentityPort` e `ProviderAuthGuard` (`APP_GUARD`) estão na composição HTTP: modo local aceita qualquer chamada e não autentica o `providerId`. Esse valor continua sujeito às validações de domínio. Health e `/metrics` são `@PublicRoute()`. Desenho com IdP: [ARCHITECTURE.md](ARCHITECTURE.md), JWP-ADR-010. Consumidores SQS não passam pelo guard (canal interno confiável).

## Logs

Logs HTTP são JSON. Em sucesso, `correlationId` vem do contexto da requisição (`response.locals`), o mesmo passado ao use case e gravado nos eventos; respostas de sucesso do contrato **não** devolvem esse campo. `walletId` entra no log quando existe (path, body ou `id` da abertura). Health não exige correlationId. O logger não registra saldo nem payload. Chaves sensíveis são mascaradas com `[REDACTED]`, assim como credenciais em URLs e valores de autorização nas strings permitidas; o mascaramento acontece antes do limite de tamanho. IDs não são labels de métrica.

## LOSS

`LOSS` exige `money.amount` igual a `"0.00"` e não aceita `referenceExternalTransactionId`. O resultado é PROCESSED com `WagerTransactionProcessed`; saldo e versão não mudam, sem ledger nem `WalletBalanceChanged`. Valor diferente de zero ou referência são recusados com 400 `INVALID_PAYLOAD`, por HTTP ou SQS.

## Idempotency-Key

Header obrigatório; recomendado `{providerId}:{externalTransactionId}`. O formato aceito é uma string opaca não vazia após `trim`, com até 128 unidades UTF-16, sem NUL nem substitutos isolados. O prefixo do provider não é obrigatório: o escopo é a coluna `provider_id`, e a aplicação preserva a chave recebida, sem trim, lowercase ou recomposição. Ausência/tipo/formato inválido → HTTP 400 `IDEMPOTENCY_KEY_REQUIRED`. Mesma chave válida com payload divergente → 409 `IDEMPOTENCY_CONFLICT`. Detalhes em [docs/HTTP.md](docs/HTTP.md).

## Reversão única

Uma transação referenciada admite no máximo uma reversão PROCESSED, somando REFUND e ROLLBACK. A segunda operação vira REJECTED/`ALREADY_REVERSED` (HTTP 422), sem novo saldo/ledger. ROLLBACK de um REFUND é permitido (referencia o REFUND). ROLLBACK de ROLLBACK é proibido. A regra contém a do enunciado (sem duas reversões do mesmo tipo) e também impede REFUND + ROLLBACK da mesma BET, que creditariam o stake duas vezes; decisão e prova em [ARCHITECTURE.md](ARCHITECTURE.md), JWP-ADR-005.

## Verificação

| Comando | Escopo |
|---|---|
| `bun run check` | Typecheck, testes unitários, build e smoke HTTP/DI |
| `bun run test:integration` | Integração, concorrência, recuperação e matriz de aceitação contra PostgreSQL/SQS reais e descartáveis (atalho para `bash scripts/reproduce/reproduce.sh`) |
| `bun scripts/reproduce/reproduce.sh` | O mesmo runner, chamado diretamente |
| `bun scripts/reproduce/reproduce.sh --matrix-only` | Matriz e controles, sem repetir todas as regressões |
| `bun run test:compose` | Recria `compose.yml` do zero com imagem real e 3 réplicas HTTP/workers, sem nginx; aguarda healthy, verifica `/health/ready` de cada réplica, PostgreSQL/schema/role runtime e as 4 filas FIFO/DLQs com redrive, e encerra com `down -v`. Sem carga ou mix financeiro; `--keep` conserva a stack para inspeção |
| `bun run test:load` | Carga contra PostgreSQL/SQS reais em Compose próprio (`jwp-load`); por padrão, 3 processos HTTP e 3 de workers, com pools separados. `LOAD_WORKERS=0` usa processos combinados `http,workers`. Verifica saldo = ledger e entrega dos eventos. Ver [docs/LOAD_TEST.md](docs/LOAD_TEST.md) |

O smoke usa o projeto `jwp-compose-smoke`, portas PG/SQS `56401`/`56402` e HTTP `3090–3099`. Para execuções simultâneas, escolha `SMOKE_PROJECT=jwp-compose-smoke-<sufixo>` e portas distintas com `SMOKE_PG_PORT`, `SMOKE_SQS_PORT` e `SMOKE_APP_PORT_RANGE`; `SMOKE_REPLICAS` aceita 3–10. Evidências e hashes do código ficam em `test-results/compose-smoke/` (saída local gerada, fora do pacote), ou em `SMOKE_OUT`. Falha em qualquer sonda ou limpeza retorna exit 1. O runner resolve a raiz pelo próprio arquivo e pode ser chamado de outro diretório com `bun /caminho/do/projeto/scripts/compose-smoke/run.ts`.

Integração, concorrência e recuperação exigem PostgreSQL/SQS reais (nada é mockado) e falham se a infra estiver ausente. A reprodução usa o projeto Compose `jwp-repro`, stacks dedicadas por suíte e observação separada; o runner encerra suas stacks com remoção dos volumes, portanto use ambiente descartável. `JWP_REPRO_SUFFIX` aceita 1–25 caracteres, começando por letra minúscula ASCII ou dígito e continuando com letras minúsculas, dígitos ou hífen; ele isola os nomes de todas essas stacks. Para execuções paralelas, também defina portas e diretórios de saída distintos (`JWP_REPRO_PG_PORT`, `JWP_REPRO_SQS_PORT`, `JWP_REPRO_OBS_SQS_PORT`, `JWP_REPRO_PORT_BASE` e `JWP_REPRO_OUT`). `COMPOSE_PROJECT_NAME` altera o projeto principal; sem sufixo explícito, um nome diferente do padrão também serve de sufixo para as stacks dedicadas e de observação. Vazão e percentis medidos estão em [docs/LOAD_TEST.md](docs/LOAD_TEST.md).

## Rotas

| Rota | Função |
|---|---|
| `POST /wallets` | Abertura |
| `GET /wallets/:walletId` | Saldo atual |
| `GET /wallets/:walletId/ledger` | Histórico com cursor opaco |
| `POST /wallets/:walletId/reconciliation` | Saldo versus ledger, sem reparo |
| `POST /wagering/transactions` | Comando financeiro; exige `Idempotency-Key` |
| `GET /wagering/transactions/:transactionId` | Resultado persistido |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta por identidade externa |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | Diagnóstico |

Payloads, status e códigos de erro: [docs/HTTP.md](docs/HTTP.md).

## Limites

- Sem IdP real, autorização entre provedores ou implantação pública; o modo local não é autenticação.
- Medições em host de desenvolvimento com LocalStack, sem SLA. Na varredura de 8/32/64 VUs com 1 e 3 réplicas, o cenário distribuído ficou entre 528 e 852 req/s e a hot wallet entre 208 e 264 req/s (~4 ms de seção crítica por transação). Com ordem estrita por wallet, a outbox de uma wallet quente drena mais devagar (128–172 s depois de 30 s a 64 VUs). Configurações, percentis e variação: [docs/LOAD_TEST.md](docs/LOAD_TEST.md).
- `bun run start` não sobe workers; mensageria operacional usa `bun run start:workers`.
- Publicação SQS é at-least-once (sem exatamente uma vez); a ordem é garantida por wallet (ordem de commit), não entre wallets diferentes.
- Hot wallet serializa no lock por linha. API financeira habilita BRL.

## Integridade do pacote

O `.delivery-manifest.json` do pacote exportado lista SHA-256 em `fileSha256`, permitindo conferir os bytes dos arquivos presentes. `sourceCommit` e `generatorSha256` identificam a origem da exportação; não são revisões consultáveis no Git do pacote, que contém um único commit. O manifesto não assina a procedência nem substitui os testes.

## Documentação

- [Enunciado](docs/CHALLENGE.md)
- [Contrato HTTP](docs/HTTP.md)
- [Arquitetura e ADRs](ARCHITECTURE.md)
- [Estado do produto](docs/STATUS.md)
- [Operação dos workers](docs/workers.md)
- [Teste de carga](docs/LOAD_TEST.md)
- [Contratos de mensageria](contracts/messaging/README.md)

## Saídas locais de validação

`test-results/` é gerado localmente pela reprodução e pelo smoke do Compose. A carga também permite definir seu diretório de saída. O manifesto `.delivery-manifest.json` lista os hashes dos arquivos exportados.
