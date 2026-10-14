# Teste de carga (`bun run test:load`)

Experimento de carga contra PostgreSQL e SQS (LocalStack) reais, com N réplicas do serviço completo. Todas as medições publicadas aqui são históricas: a execução de referência de 13/10/2026 antecede a multimoeda, a política de reversão configurável e as métricas de contenção. A versão final é medida pelo roteiro em [Execução da versão final](#execução-da-versão-final). Não há meta de RPS nem SLA de produção. Novas execuções podem ser feitas com `bun run test:load`.

## Execução de referência (histórica, 13/10/2026)

Execução concluída em 2026-10-13T19:41:57-03:00 (BRT), sobre o código da época, já com LockMode.PESSIMISTIC_WRITE e anterior à multimoeda. O commit medido não faz parte do histórico publicado deste repositório; por isso os números ficam como histórico e não são atribuídos a esta versão. Resumos versionados: [ambiente e comandos](evidence/README.md), [resumo da carga](evidence/load/summary.json) e [results.json sanitizado](evidence/load/results.json).

Configuração padrão: 3 HTTP + 3 workers, 32 VUs, 20 s por cenário, aquecimento de 5 s, 200 wallets, 500 apostas hot-drain e 1000 comandos SQS. Host compartilhado, PostgreSQL 17.6 e LocalStack 4.9.2 reais; uma rodada, sem SLA.

| Cenário | Requisições | req/s | p50 / p95 / p99 (ms) | Erros HTTP | Retries / falhas SQL transitórias | Máximos amostrados no banco | Lag outbox p50 / p95 / p99 (ms) | Drenagem após carga (ms) |
|---|---:|---:|---|---:|---|---|---|---:|
| distributed | 14384 | 718.2 | 34.3 / 93.4 / 235.4 | 0% | 0 / 0 | {"outbox_pending": 19181, "lock_waiters": 1, "active_sessions": 25, "waiting_on_lock": 1} | 20606 / 32378 / 33291 | 33581 |
| hot-wallet | 4611 | 229.1 | 136.9 / 231.6 / 282.9 | 0% | 0 / 0 | {"outbox_pending": 5695, "lock_waiters": 11, "active_sessions": 15, "waiting_on_lock": 11} | 24907.5 / 43656.2 / 44439 | 43819 |
| hot-drain | 500 | 259.9 | 112.4 / 168.1 / 221.3 | 0% | 0 / 0 | {"outbox_pending": 155, "lock_waiters": 11, "active_sessions": 12, "waiting_on_lock": 11} | 787 / 1126.1 / 1166 | 1255 |

Exit code 0; 17/17 invariantes verdadeiras, incluindo saldo = ledger. Downstream: 42703/42703 eventIds distintos, 0 duplicados e 0 faltantes. Todos os 6 processos encerrados com exit 0. Os máximos amostrados incluem espera de lock e são diferentes de falhas SQL que esgotaram retries. Configuração, outcomes, percentis, série amostrada e métricas completas estão no JSON.

As medições abaixo são históricas, inclusive a canary curta. Não representam uma nova execução e não devem ser usadas como benchmark desta versão. Seus artefatos antigos não acompanham este pacote; a evidência pública conferível desta entrega é a execução acima.

## Execução da versão final

A versão final é medida depois do último commit de produto, com a árvore limpa, nas duas políticas de reversão:

```bash
bash scripts/load/final-candidate.sh "$HOME/.cache/jwp-load-final"
```

O roteiro ([scripts/load/final-candidate.sh](../scripts/load/final-candidate.sh)) recusa mudanças de produto fora do HEAD, roda `bun run test:load` com `JWP_REVERSAL_POLICY=per_type` e depois com `per_reference`, e grava `results.json`/`summary.md` de cada política e o commit medido em `run.json`. O resultado inclui o delta de `jwp_lock_conflicts_total`, que conta cada aborto por disputa de lock (também os absorvidos por retry), ao lado de `jwp_sql_transient_failures_total` e do máximo amostrado de `pg_locks` não concedidos.

## Como executar

```bash
bun install --frozen-lockfile
bun run test:load
```

O comando faz o build e executa [scripts/load/run.ts](../scripts/load/run.ts), que:

1. recria o projeto Compose `jwp-load` (`down -v` e `up --wait`) com `compose.db.yml` + `compose.sqs.yml`, nas portas 56432 (PostgreSQL) e 56566 (SQS), separadas da stack de desenvolvimento;
2. aplica as migrations com a credencial owner (`src/persistence/migrate.ts`) e cria o LOGIN runtime `jwp_load_api`, membro de `jwp_runtime`;
3. sobe `LOAD_REPLICAS` processos `APP_ROLES=http bun dist/main.js` (espera `/health/ready` = 200) e `LOAD_WORKERS` processos separados `APP_ROLES=workers` (consumer SQS, publisher da outbox e scheduler), cada um com seu próprio pool de 4 conexões; assim o publisher não disputa pool nem CPU do processo HTTP. `LOAD_WORKERS=0` volta ao modo combinado `http,workers` (o mesmo que `start:workers`) usado nas execuções históricas;
4. abre as wallets via `POST /wallets`, faz um aquecimento que não entra nos resultados e executa os cenários;
5. verifica as invariantes financeiras, encerra as réplicas com SIGTERM (drain) e derruba o Compose (`LOAD_KEEP_INFRA=1` preserva).

Nada é mockado: as requisições passam pelo HTTP real, pelo PostgreSQL real e pela fila FIFO do LocalStack. Os resultados vão para `results.json` e `summary.md` em `LOAD_OUT_DIR` (padrão: diretório temporário).

| Variável | Padrão | Significado |
|---|---|---|
| `LOAD_REPLICAS` | 3 | Processos HTTP (`http`, ou `http,workers` se `LOAD_WORKERS=0`) |
| `LOAD_WORKERS` | = `LOAD_REPLICAS` (mín. 1) | Processos `APP_ROLES=workers` separados; 0 = combinado |
| `LOAD_CONCURRENCY` | 32 | Usuários virtuais por cenário HTTP |
| `LOAD_DURATION_S` | 20 | Duração dos cenários `distributed` e `hot-wallet` |
| `LOAD_WARMUP_S` | 5 | Aquecimento descartado |
| `LOAD_WALLETS` | 200 | Wallets do cenário `distributed` |
| `LOAD_HOT_DRAIN_BETS` | 500 | BETs de 1.00 contra wallet com 100.00 |
| `LOAD_SQS_MESSAGES` / `LOAD_SQS_WALLETS` | 1000 / 100 | Comandos enviados à fila e wallets usadas |
| `LOAD_DRAIN_TIMEOUT_S` | 900 | Espera máxima pela outbox/fila |

Exit 0 significa que as invariantes passaram e que as réplicas encerraram limpas. O desempenho medido nunca reprova a execução.

## Cenários

| Cenário | Carga | O que exercita |
|---|---|---|
| `distributed` | Laço fechado com 32 VUs por 20 s; ciclos BET, WIN, LOSS, REFUND, BET, ROLLBACK em 200 wallets aleatórias com saldo alto | Paralelismo entre wallets distintas, em round-robin pelas réplicas |
| `hot-wallet` | A mesma mistura, mas todos os VUs na mesma wallet | Serialização no lock por wallet com várias réplicas |
| `hot-drain` | 500 BETs de 1.00, 32 VUs, contra uma wallet com 100.00 | Corrida pelo saldo: exatamente 100 PROCESSED, o resto `INSUFFICIENT_FUNDS`, saldo final 0.00 |
| `sqs` | 1000 `WagerTransactionRequested` enviados em lote à fila de comandos (`MessageGroupId=walletId`, 100 wallets) | Consumer SQS, inbox e processamento pelos workers das réplicas |

## Mix HTTP completo

Cada usuario virtual percorre BET, WIN, LOSS, REFUND, BET, ROLLBACK. A primeira
BET confirmada fornece a referencia exclusiva do REFUND; a segunda fornece a do
ROLLBACK. Referencias conservam provider, wallet, round, game e valor da BET.
Nao reutilizamos uma BET para duas reversoes. LOSS envia exatamente 0.00 BRL
sem referencia. BET/WIN usam 1.00-9.99. No ciclo completo ha duas BET e uma
operacao de cada outro tipo; cortes por tempo e falhas mudam essa proporcao.

Uma BET rejeitada ou com resposta incerta nao fornece referencia. Quando falta
referencia, aquela etapa envia nova BET, sem inventar id nem criar pendencia
artificial. Cada VU avanca somente depois da resposta, mas diferentes VUs e
wallets seguem concorrentes. O mix altera a carga e as taxas; nao comparar
diretamente com os numeros historicos BET/WIN abaixo.

Hot-drain continua exclusivamente BET e SQS continua com o mix BET/WIN anterior.
Downstream continua ativo durante toda a carga: todos eventIds esperados precisam
chegar, sem eventos desconhecidos, malformados ou ausentes. O JSON registra
kinds e kindOutcomes por cenario, incluindo erros; cobertura dos cinco tipos
confirmados em ambos os cenarios HTTP passa a ser gate, nao meta de performance.

## Histórico: validação do mix completo (12/10/2026)

Foram executadas duas rodadas novas: `LOAD_REPLICAS=1` e `3`, com
`LOAD_WORKERS` respectivamente 1 e 3, 8 VUs, 10 s por cenário HTTP,
aquecimento de 2 s, 30 wallets, 150 BETs no hot-drain, 100 comandos SQS em
10 wallets e prazo de drain de 240 s. Ambas terminaram com exit 0.

Todos os cinco tipos foram PROCESSED em distributed e hot-wallet. Hot-drain
manteve 100 processadas e 50 INSUFFICIENT_FUNDS; SQS confirmou 100/100;
a reconciliação foi consistente nas 42 wallets. O downstream recebeu
19.355/19.355 eventIds com uma réplica e 14.926/14.926 com três: nenhum
ausente, desconhecido, malformado ou duplicado. Todos os processos encerraram
com exit 0 e a infraestrutura do projeto `jwp-load040` foi removida.

Os resultados não demonstram ganho de escala: o host é compartilhado, a carga
é limitada e o número de workers também varia. A mistura nova não é comparável
diretamente aos experimentos históricos BET/WIN. Os defaults gerais continuam
inalterados; estas rodadas usaram os parâmetros explícitos acima. O JSON de cada
rodada inclui contagens por kind e outcome, latências, drain e checks.

## Métricas e definições

- **Vazão**: requisições concluídas divididas pelo tempo de parede do cenário, incluindo a cauda das requisições em voo. No `sqs`, mensagens processadas divididas pelo intervalo entre o primeiro envio e o último `processed_at` da inbox.
- **Latência HTTP**: do envio até a leitura completa da resposta, no cliente. Os percentis usam nearest-rank sobre todas as requisições do cenário.
- **Latência SQS fim a fim**: do `SendMessageBatch` até o `processed_at` gravado na inbox.
- **Erro**: transporte falho ou qualquer status HTTP fora de 200/202/422. Um 422 `INSUFFICIENT_FUNDS` é resposta de contrato, contada à parte nos `outcomes`.
- **Conflitos de concorrência**: delta de `jwp_retries_total` e `jwp_sql_transient_failures_total` somado nas réplicas; máximo de `pg_locks` não concedidos e de sessões em `wait_event_type='Lock'`, amostrados a cada 1 s; rejeições `INSUFFICIENT_FUNDS` do `hot-drain`.
- **Outbox lag**: `published_at - created_at` de cada evento do cenário. `created_at` é o `now()` da transação financeira. Também são medidos o tempo para a outbox zerar depois do fim da carga e a taxa de publicação (eventos / janela entre o primeiro e o último `published_at`).
- **CPU**: núcleos ocupados em média durante a carga. Réplicas por `/proc/<pid>/stat`; PostgreSQL e LocalStack pelo `cpu.stat` do cgroup do container; gerador pelo próprio `process.cpuUsage()`.

## Invariantes verificadas ao final

Cada uma reprova a execução com exit 1:

- `wallets.balance` = soma assinada do ledger, para todas as wallets;
- `wallets.balance` = `balance_after` do último lançamento;
- nenhum saldo negativo e nenhum lançamento duplicado por transação;
- toda BET/WIN/REFUND/ROLLBACK PROCESSED tem ledger, e nenhuma transação não processada tem ledger;
- LOSS tem valor 0.00, sem referencia e sem ledger; reversoes deste experimento apontam para BET confirmada de mesma wallet/provider/round e valor;
- distributed e hot-wallet confirmam os cinco tipos PROCESSED; a distribuicao observada aparece em kinds e kindOutcomes;
- nenhuma transação PENDING ou PENDING_REFERENCE restante, e outbox totalmente publicada;
- `POST /wallets/:id/reconciliation` retorna `consistent: true` para todas as wallets da carga;
- `hot-drain` termina com saldo 0.00 e exatamente 100 BETs processadas;
- DLQs vazias e fila de comandos drenada;
- respostas definitivas do cliente batem com as linhas do banco por status e failureCode (respostas 5xx/timeout podem ou não ter commit e entram só como margem).

## Ambiente das execuções históricas registradas

Máquina de desenvolvimento única; condições abaixo referem-se às execuções históricas iniciais. As novas rodadas V2/V3 registram separadamente o estado do host e começam sem outros containers de carga ativos:


- Intel i7-11800H com 16 CPUs lógicas e 15,4 GiB de RAM, kernel Linux 7.0.0, Docker 29.1.3;
- Bun 1.3.14, PostgreSQL 17.6 (imagem alpine, configuração padrão) e LocalStack 4.9.2;
- código do produto: cada execução faz `bun run build` da árvore de trabalho; a versão medida está indicada em cada tabela, com os rótulos abaixo;
- gerador de carga, réplicas, PostgreSQL e LocalStack no mesmo host, via loopback;
- ao mesmo tempo havia de 19 a 21 outros containers ativos (stacks de outros trabalhos), com load average de 1 minuto entre 1,9 e 4,7 no início das execuções;
- pool MikroORM de 4 conexões por réplica, `JWP_WORKER_INTERVAL_MS=250` (padrão) e logs JSON por requisição gravados em arquivo.

Versões do código medidas ao longo do documento:

| Rótulo | O que mudou |
|---|---|
| V1 | Versão inicial: publisher da outbox com pausa de 250 ms entre lotes, mesmo com backlog |
| V2 | V1 + consumer SQS com backoff de visibilidade em falha transitória e heartbeat a cada 10 s |
| V3 | V2 + outbox volta a reivindicar imediatamente quando o lote vem cheio e todo publicado |
| V4 | V3 + ordem estrita de publicação por wallet, consumer sem pausa com lote cheio e mix HTTP completo. Antecede a mudança de modo de lock financeiro (LockMode) de 13/10/2026, aplicada depois, sem nova varredura |

| Execução | Início (UTC) | Versão |
|---|---|---|
| `run-3-replicas` | 07:27:33 | V1, sem mudanças locais em `src/` |
| `run-1-replica` | 07:38:31 | V1; a mudança do consumer (V2) entrou durante a execução, então o build pode tê-la incluído |
| `run-6-replicas` | 08:01:46 | V2 |

A diferença entre V1 e V2 afeta só o consumer SQS. O caminho HTTP, a outbox e o processamento financeiro não mudaram, por isso os cenários HTTP das três execuções são comparáveis. Os números do cenário `sqs` com 1 e 6 réplicas foram medidos com o consumer novo (no caso de 1 réplica, possivelmente). Não houve execução repetida na mesma versão para isolar esse efeito.

Foi feita uma execução por configuração, sem repetição. As diferenças pequenas entre execuções, como 252,8 contra 258,1 contra 264,1 req/s no `hot-wallet`, estão dentro do ruído esperado e não devem ser lidas como tendência.

## Resultados históricos

Comandos executados:

- `LOAD_OUT_DIR=test-results/load-test/run-3-replicas bun run test:load`;
- o mesmo com `LOAD_REPLICAS=1` (`run-1-replica`);
- o mesmo com `LOAD_REPLICAS=6` (`run-6-replicas`).

As três terminaram com exit 0. A saída completa está em `results.json`, `summary.md` e `*.console.txt` de cada diretório.

Esses arquivos brutos de `test-results/load-test/` não acompanham a entrega: são saídas locais, fora do pacote. Para conferir as tabelas é preciso regenerá-los com `bun run test:load`, apontando `LOAD_OUT_DIR` para o diretório desejado (sem a variável, a saída vai para um diretório temporário). Uma nova execução produz números próprios, não uma cópia dos registrados aqui.

### HTTP, 32 VUs

| Réplicas | Cenário | Requisições | req/s | p50 ms | p95 ms | p99 ms | max ms | Erros |
|---|---|---|---|---|---|---|---|---|
| 1 | distributed | 14073 | 702,6 | 41,3 | 73,5 | 106,9 | 162,9 | 0% |
| 3 | distributed | 21878 | 1092,8 | 24,8 | 61,7 | 114,5 | 240,5 | 0% |
| 6 | distributed | 23372 | 1164,5 | 24,8 | 49,5 | 67,4 | 139,2 | 0% |
| 1 | hot-wallet | 5188 | 258,1 | 112,9 | 183,1 | 266,4 | 312,4 | 0% |
| 3 | hot-wallet | 5084 | 252,8 | 121,1 | 264,1 | 296,0 | 444,7 | 0% |
| 6 | hot-wallet | 5312 | 264,1 | 81,4 | 472,6 | 516,4 | 660,7 | 0% |
| 1 | hot-drain | 500 | 312,3 | 102,4 | 141,4 | 145,2 | 145,8 | 0% |
| 3 | hot-drain | 500 | 306,2 | 105,4 | 155,3 | 171,5 | 195,1 | 0% |
| 6 | hot-drain | 500 | 323,3 | 84,8 | 173,3 | 214,4 | 235,2 | 0% |

### Concorrência e CPU durante a carga

Os núcleos ocupados aparecem na ordem réplicas (soma) / PostgreSQL / LocalStack / gerador.

| Réplicas | Cenário | Retries / falhas SQL transitórias | Máx. sessões esperando lock | Núcleos ocupados |
|---|---|---|---|---|
| 1 | distributed | 0 / 0 | 0 | 1,11 / 1,31 / 0,06 / 0,17 |
| 3 | distributed | 0 / 0 | 1 | 3,00 / 2,86 / 0,21 / 0,29 |
| 6 | distributed | 0 / 0 | 3 | 5,92 / 4,21 / 0,42 / 0,28 |
| 1 | hot-wallet | 0 / 0 | 3 | 0,59 / 0,54 / 0,04 / 0,13 |
| 3 | hot-wallet | 0 / 0 | 11 | 1,15 / 0,60 / 0,12 / 0,13 |
| 6 | hot-wallet | 0 / 0 | 18 | 1,90 / 0,73 / 0,26 / 0,13 |

No `hot-drain` as três execuções terminaram com 100 PROCESSED, 400 REJECTED/`INSUFFICIENT_FUNDS` e saldo 0.00, com até 23 sessões esperando o mesmo lock na execução de 6 réplicas.

### Outbox

Cada transação processada grava dois eventos na outbox.

| Réplicas | Cenário | Eventos | Lag p50 / p95 / p99 | Drenagem após o fim da carga | Publicação observada |
|---|---|---|---|---|---|
| 1 | distributed | 28146 | 385,7 s / 728,2 s / 758,5 s | 765,9 s | 35,8 ev/s |
| 3 | distributed | 43756 | 201,7 s / 384,9 s / 401,6 s | 405,1 s | 102,9 ev/s |
| 6 | distributed | 46744 | 112,0 s / 209,0 s / 218,1 s | 220,3 s | 194,5 ev/s |
| 3 | hot-wallet | 10168 | 44,2 s / 78,3 s / 81,5 s | 81,2 s | 99,7 ev/s |
| 3 | sqs | 2000 | 3,4 s / 6,4 s / 6,7 s | 5,8 s | 107,7 ev/s |

### SQS (1000 comandos, 100 wallets)

O envio dos 1000 comandos levou cerca de 0,3 s, então a latência fim a fim é dominada pela espera na fila.

| Réplicas | Vazão observada | Fim a fim p50 / p95 / p99 | Erros / DLQ |
|---|---|---|---|
| 1 | 30,5 msg/s | 16,5 s / 31,1 s / 32,3 s | 0 / 0 |
| 3 | 83,1 msg/s | 6,0 s / 11,2 s / 11,6 s | 0 / 0 |
| 6 | 140,8 msg/s | 3,4 s / 6,5 s / 6,8 s | 0 / 0 |

### Invariantes

Todas passaram nas três execuções:

- 302 wallets por execução com `balance` igual à soma do ledger e ao último `balance_after`;
- 302/302 reconciliações `consistent`;
- nenhum lançamento duplicado, saldo negativo ou transação pendente;
- outbox publicada por inteiro e DLQs vazias;
- respostas definitivas do cliente iguais às linhas do banco.

As réplicas encerraram com exit 0 após SIGTERM. Os únicos warnings de log foram os 400 `http.request` dos 422 esperados do `hot-drain`.

## Análise histórica

**Hot wallet: o teto é a serialização no lock da linha.** A vazão ficou entre 253 e 264 req/s com 1, 3 ou 6 réplicas, ou seja, cerca de 3,8–4,0 ms de seção crítica por transação: lock da wallet, transação, ledger, dois eventos de outbox e commit. Nenhum componente estava perto de saturar a CPU. Mais réplicas só trazem mais conexões para a fila do mesmo lock: o máximo de sessões esperando foi de 3 para 11 e depois para 18, e o p95 foi de 183 para 264 e depois para 473 ms. O pool de 4 conexões por réplica limita quantas transações esperam no PostgreSQL. O restante espera no pool da aplicação, e por isso o `lock_timeout` de 1 s não foi atingido e não houve retry nem 503 nesta carga. Esse é o comportamento previsto pelo desenho (ADR de lock por wallet): correção em vez de paralelismo na mesma wallet.

**Distributed: escala até cerca de 3 réplicas e depois estabiliza.**

- Com 1 réplica, o processo Bun ocupou ~1,1 núcleo, o teto prático do thread JavaScript. A CPU da aplicação limita.
- Com 3 réplicas, a vazão subiu 1,56× (702 → 1093 req/s).
- Com 6 réplicas, subiu só mais 7% (1164 req/s), embora as réplicas somassem 5,9 núcleos e o PostgreSQL 4,2. O custo de CPU por requisição cresceu (de 1,6 para 5,1 ms nas réplicas e de 1,9 para 3,6 ms no PostgreSQL), o que é compatível com os workers de cada réplica (publisher da outbox, consumer e scheduler) disputando CPU e conexões com o caminho HTTP enquanto há backlog.
- Com 32 VUs em laço fechado, a vazão também é limitada por `32 / latência`: o p50 ficou em 24,8 ms com 3 e com 6 réplicas.
- Essas execuções históricas não tinham varredura de VUs nem medição de I/O. A varredura posterior (seção "Varredura de VUs") mostra que o processo HTTP satura em ~1 núcleo por réplica e que, com 3 réplicas, a vazão ainda sobe até 64 VUs; I/O do PostgreSQL (commits síncronos, WAL) continua não medido.

**Outbox na versão inicial: o gargalo era o frescor dos eventos.** Cada réplica publica cerca de 35 eventos/s (35,8 com 1 réplica, 102,9 com 3 e 194,5 com 6), independentemente da carga. O publisher reivindica lotes de 10, faz um `SendMessage` por evento e dorme 250 ms entre lotes, mesmo com backlog. Como cada transação gera 2 eventos, acima de ~17 transações/s por réplica o backlog cresce linearmente: no `distributed` com 3 réplicas chegou a ~40 mil eventos pendentes, e o p50 do lag passou de 3 minutos. Nenhum evento se perdeu ou foi publicado antes do commit, e a outbox zerou em todas as execuções, mas assinantes veriam os eventos com minutos de atraso sob carga sustentada. A correção está na seção seguinte.

**Consumer SQS nas versões iniciais: limitado por processo.** A vazão foi de 30,5, 83,1 e 140,8 msg/s com 1, 3 e 6 réplicas, ou seja, ~23–30 por réplica. O consumer processa as até 10 mensagens de cada receive em sequência e pausa 250 ms entre polls. Com `MessageGroupId=walletId`, réplicas diferentes avançam grupos diferentes. A latência fim a fim de vários segundos é tempo de fila, porque os 1000 comandos chegaram em ~0,3 s.

## Varredura histórica de VUs

**Pergunta:** onde a vazão para de subir e o que satura, com HTTP e workers em processos separados (`LOAD_WORKERS` = réplicas), PostgreSQL e LocalStack reais. Sem meta de RPS; uma execução por ponto.

**Configuração:** 8, 32 e 64 VUs × 1 e 3 réplicas HTTP (com 1 e 3 processos de workers); 10 s por cenário HTTP, aquecimento de 2 s, 30 wallets no `distributed`, 150 BETs no `hot-drain` e 100 comandos SQS em 10 wallets (mesmos parâmetros da validação do mix completo). Mix HTTP completo: BET, WIN, LOSS 0.00, REFUND e ROLLBACK. Execução sequencial às 15:10–15:21 (UTC−3), no mesmo host descrito em "Ambiente", com os containers de outros trabalhos parados antes do início (load average de 1 min = 1,21 na primeira rodada). Código do produto: V4, sem mudanças locais em `src/` nem no script de carga durante a sequência. A varredura é anterior à mudança de LockMode de 13/10/2026 e não foi repetida depois dela. A evidência atual do código com LockMode é a [execução de referência](#execução-de-referência-histórica-13102026) (configuração padrão, 17/17 invariantes, 42703/42703 eventos; [docs/evidence](evidence/README.md)). A canary curta no fim deste documento é histórica.

| HTTP + workers | VUs | distributed req/s | p50 / p95 / p99 ms | hot-wallet req/s | p50 / p95 / p99 ms | ms/tx na hot wallet (≈1/vazão) | máx. sessões esperando lock (hot) | erros | retries + falhas SQL transitórias | gates | downstream: recebidos/esperados, dup, faltando |
|---|---:|---:|---|---:|---|---:|---:|---:|---:|---|---|
| 1 + 1 | 8 | 641,1 | 11,5 / 16,1 / 26,5 | 250,1 | 31,4 / 42,6 / 54,5 | 4,0 | 3 | 0 | 0 | 17/17 | 19.065/19.065, 0, 0 |
| 1 + 1 | 32 | 662,6 | 47,1 / 63,2 / 72,4 | 263,6 | 122,4 / 144,7 / 161,3 | 3,8 | 3 | 0 | 0 | 17/17 | 19.960/19.960, 0, 0 |
| 1 + 1 | 64 | 528,4 | 103,4 / 197,1 / 726,3 | 264,3 | 240,0 / 288,9 / 371,5 | 3,8 | 3 | 0 | 0 | 17/17 | 17.352/17.352, 0, 0 |
| 3 + 3 | 8 | 659,8 | 10,4 / 23,9 / 48,7 | 263,4 | 32,9 / 47,0 / 55,0 | 3,8 | 7 | 0 | 0 | 17/17 | 19.786/19.786, 0, 0 |
| 3 + 3 | 32 | 738,5 | 35,8 / 90,1 / 169,8 | 208,5 | 152,3 / 266,7 / 346,0 | 4,8 | 11 | 0 | 0 | 17/17 | 19.924/19.924, 0, 0 |
| 3 + 3 | 64 | 852,4 | 71,5 / 143,8 / 231,6 | 238,7 | 259,2 / 429,1 / 575,6 | 4,2 | 11 | 0 | 0 | 17/17 | 23.264/23.264, 0, 0 |

CPU média por componente durante a carga, outbox e SQS:

| HTTP + workers | VUs | distributed: CPU app / PostgreSQL / LocalStack / gerador (núcleos) | hot-wallet: CPU app / PostgreSQL / LocalStack | outbox lag p99 distributed / hot-wallet (s) | drenagem da outbox após a carga distributed / hot-wallet (s) | SQS (100 msgs) msg/s |
|---|---:|---|---|---|---|---:|
| 1 + 1 | 8 | 1,6 / 1,6 / 0,7 / 0,4 | 1,1 / 0,7 / 0,7 | 33,3 / 16,6 | 33,5 / 16,8 | 96,2 |
| 1 + 1 | 32 | 1,8 / 1,7 / 0,7 / 0,4 | 1,1 / 0,8 / 0,7 | 39,1 / 16,6 | 39,5 / 16,1 | 92,4 |
| 1 + 1 | 64 | 1,6 / 1,5 / 0,7 / 0,4 | 1,1 / 0,8 / 0,7 | 26,9 / 17,6 | 26,2 / 17,1 | 98,9 |
| 3 + 3 | 8 | 4,1 / 2,8 / 0,9 / 0,5 | 1,8 / 0,9 / 0,7 | 20,0 / 16,1 | 20,0 / 16,3 | 146,8 |
| 3 + 3 | 32 | 3,9 / 2,9 / 0,7 / 0,4 | 1,7 / 0,8 / 0,6 | 25,9 / 13,3 | 26,0 / 12,6 | 133,5 |
| 3 + 3 | 64 | 4,5 / 3,6 / 0,9 / 0,5 | 1,8 / 0,9 / 0,7 | 29,9 / 16,3 | 28,9 / 15,6 | 133,2 |

Conflitos de concorrência: nenhum retry nem falha SQL transitória em nenhuma rodada; o único sinal de contenção são as sessões esperando o lock da linha (amostradas a cada 1 s). Em todas as rodadas os 17 gates passaram, com 0 erros HTTP e downstream completo, sem duplicados nem faltantes.

### Condições da implementação de referência (64 VUs, 30 s, 200 wallets, 3 réplicas)

Mesmo script, com `LOAD_WALLETS=200`, `LOAD_DURATION_S=30` e 3.000 comandos SQS em 100 wallets. Foram três execuções:
- `ref-a`: árvore de trabalho com uma alteração local no scheduler de referências pendentes, que o mix não aciona (nenhuma referência pendente é criada);
- `ref-b`: código limpo da mesma versão, mas com outra carga rodando no mesmo host;
- `ref-c`: código limpo, iniciado com o host ocioso.

| Execução (64 VUs, 30 s, 200 wallets, 3 + 3) | load 1 min no início | distributed req/s | p50 / p95 / p99 ms | hot-wallet req/s | p50 / p95 / p99 ms | ms/tx hot | SQS 3.000 msgs msg/s | outbox: drenagem distributed / hot (s) | gates | downstream |
|---|---:|---:|---|---:|---|---:|---:|---|---|---|
| `ref-a` | 5,44 | 950,7 | 35,3 / 170,4 / 231,6 | 221,9 | 285,4 / 374,4 / 417,2 | 4,5 | 148,3 | 74,5 / 127,8 | 17/17 | 74112/74112, dup 0, faltando 0 |
| `ref-b` | 5,22 | 630,2 | 77,5 / 265,2 / 472,8 | 149,9 | 409,7 / 594,0 / 806,4 | 6,7 | 155,1 | 62,3 / 47,0 | 17/17 | 52361/52361, dup 0, faltando 0 |
| `ref-c` | 1,93 | 958,7 | 60,3 / 135,9 / 173,1 | 246,8 | 237,8 / 376,7 / 436,6 | 4,1 | 103,4 | 69,9 / 172,4 | 17/17 | 76083/76083, dup 0, faltando 0 |

A carga de 1 min no início foi lida com `uptime` antes de cada execução. As execuções sem disputa de host concordam (951 e 959 req/s no `distributed`; 222 e 247 req/s na hot wallet). A execução com outra carga no mesmo host perdeu ~34% de vazão e quase dobrou o p99. **Diferenças abaixo de ~30% entre execuções isoladas neste host compartilhado não devem ser lidas como efeito de código.**

### Análise da varredura

**Ponto de saturação.**
- **1 réplica:** o `distributed` já está saturado com 8 VUs. De 8 para 32 VUs a vazão fica em ~640–660 req/s e o p50 sobe de 11,5 para 47 ms; com 64 VUs a vazão cai para 528 req/s e o p99 chega a 726 ms. A latência segue a lei de Little (latência média ≈ VUs / vazão: 8/641 ≈ 12 ms, 32/663 ≈ 48 ms): acima do joelho, VUs extras viram fila, não vazão. A CPU dos processos da aplicação fica em ~1,6–1,8 núcleo no total (processo HTTP + processo de workers), ou seja, o processo HTTP está no teto prático de um thread JavaScript. O componente que satura é a aplicação.
- **3 réplicas:** a vazão ainda sobe com os VUs (660 → 739 → 852 req/s em 10 s com 30 wallets; ~950 req/s em 30 s com 200 wallets). Com 64 VUs a CPU da aplicação chega a ~4,5–4,7 núcleos e o PostgreSQL a 3,2–3,6, de 16 lógicos. Ainda não há um patamar nítido, mas o p99 cresce mais que a vazão (49 → 170 → 232 ms). Com 30 wallets e 64 VUs há mais colisão por wallet (até 7 sessões esperando lock no `distributed`) do que com 200 wallets (1), o que explica parte da diferença entre 852 e ~950 req/s.

**Hot wallet: o teto é o tempo da seção crítica.** A vazão ficou entre 208 e 264 req/s em todas as configurações, ou seja, ~3,8–4,8 ms por transação serializada (≈ 1/vazão): lock da linha, idempotência, transação, ledger, dois eventos de outbox e commit com os triggers diferidos. Mais VUs não aumentam a vazão; só aumentam a fila (p50 de 31 → 122 → 240 ms com 8, 32 e 64 VUs em 1 réplica, exatamente VUs/vazão). Mais réplicas trazem mais sessões para a fila do mesmo lock: no máximo 3 esperando com 1 réplica (o pool de 4 conexões limita) e 11 com 3 réplicas (3 × 4 − 1). Nenhum componente satura CPU nesse cenário (aplicação ≤ 2 núcleos, PostgreSQL ≤ 1,3).

**Outbox: frescor, não correção.** Com a ordem estrita por wallet (Migration202610120007), uma wallet quente é publicada por um publisher de cada vez. Depois dos 30 s de hot wallet com 64 VUs, nas duas execuções sem disputa de host, a outbox levou 128–172 s para zerar; no `distributed`, com eventos espalhados por 200 wallets, 70–75 s (62 s na execução com disputa de host). O downstream recebeu todos os eventos, sem duplicados, em todas as rodadas. É o custo assumido da ordem por wallet: a vazão de publicação de uma única wallet não escala com réplicas.

**SQS:** 92–99 msg/s com 1 processo de workers e 133–147 msg/s com 3 (100 comandos); 103–155 msg/s com 3.000 comandos em 100 wallets. O LocalStack usou no máximo ~1,0 núcleo: neste host, ele não foi o gargalo.

### Comparação com outra implementação do desafio

Outra entrega do mesmo desafio publica um teste de carga com 64 VUs, 30 s, 200 wallets e 3 réplicas: ~335–398 req/s no cenário distribuído (p99 ~360–500 ms) e ~123–138 req/s na hot wallet (p99 ~1,0–1,2 s), com 0 erros e reconciliação consistente. **Os números crus não são comparáveis:**
- hardware e virtualização diferentes: desktop de 6 núcleos sem HT com Docker Desktop/WSL2 lá; i7-11800H com 16 CPUs lógicas e Docker nativo no Linux aqui;
- topologia diferente: nginx na frente de 3 réplicas com todos os papéis e pool de 20 conexões lá; gerador fazendo round-robin direto em 3 processos HTTP + 3 de workers com pool de 4 conexões aqui;
- mix diferente: 60% BET, 20% WIN e 20% LOSS lá; ciclos BET, WIN, LOSS, REFUND, BET e ROLLBACK aqui, com reversões contra BET confirmada;
- emulador SQS diferente (MiniStack lá, saturado em ~1 núcleo; LocalStack aqui, até ~1,0 núcleo).

As conclusões estruturais coincidem: a hot wallet é limitada pelo tempo da seção crítica por transação (lá ~8 ms, aqui ~4–5 ms, com hops de rede virtual a mais lá); laço fechado com 64 VUs mede principalmente fila; e a correção (0 erros, saldo = ledger, nada duplicado) se mantém sob saturação nas duas.

## Histórico: o que a carga encontrou → corrigimos

**Achado.** O publisher da outbox tinha teto fixo de ~35 eventos/s por réplica: lote de 10, um `SendMessage` por evento e pausa de 250 ms entre lotes **mesmo com backlog**. Com 2 eventos por transação, qualquer carga acima de ~17 tx/s por réplica acumulava atraso linear (lag p50 de minutos).

**Correção (V3).** O laço do worker volta a reivindicar imediatamente quando o lote anterior veio cheio **e** inteiramente `PUBLISHED`; qualquer `RETRY`/`LOST_LEASE` ou lote parcial volta ao intervalo normal, para não martelar um SQS com falha. Nada mudou no claim/lease, no `MessageGroupId=walletId`, na deduplicação por `eventId` nem na ordem (um lote por vez por réplica, como antes). `SendMessageBatch` e lanes paralelas por agregado ficaram de fora: o ganho medido já removeu o gargalo e evitam mexer na semântica de falha por mensagem coberta pela matriz de testes.

**Nova verificação.** O script agora tem um assinante downstream que drena `wager-events.fifo` em 4 lanes FIFO durante toda a execução, conta cada `eventId` e reprova se houver `eventId` duplicado, evento da outbox não recebido, ou mensagem desconhecida/malformada (`downstreamNoDuplicateEventId`, `downstreamReceivedEveryEvent`).

### Nova comparação V2/V3 em snapshots imutáveis

Foram medidas novamente uma réplica combinada (`http,workers`) em V2 e a mesma configuração em V3: 32 VUs, 20 s por cenário HTTP, aquecimento descartado de 5 s, 200 wallets, 500 BETs no hot-drain, 1.000 comandos SQS em 100 wallets e limite de drain de 900 s. O mix HTTP é BET/WIN dos snapshots históricos, anterior ao mix completo de V4. PG e SQS são reais; o assinante downstream opera nas duas. Não havia outros containers nem geradores de carga ativos antes das rodadas. Uptime/load, configuração, console, resultados brutos e hashes dos arquivos compilados foram preservados. Os snapshots usam o mesmo runner e diferem somente na exposição do tamanho do lote do publisher e na repetição imediata de lote cheio e publicado.

Estes são resultados novos; substituem os números anteriores de 35,8 → 351,9 ev/s nesta comparação. Os resultados brutos antigos dessa comparação não foram versionados e não são usados como prova. O ganho abaixo é observado em uma execução por configuração, com workload aleatório e no host local; não é SLA nem estimativa de produção.

| Cenário | Métrica | Antes (V2, nova execução) | Depois (V3, nova execução) |
|---|---|---|---|
| distributed | requisições / eventos | 16146 / 32292 | 13496 / 26992 |
| distributed | req/s | 806,1 | 673,5 |
| distributed | outbox ev/s | 35,9 | 358,3 |
| distributed | lag p50 / p95 | 441,9 s / 836,4 s | 36,9 s / 53,5 s |
| distributed | drenagem após a carga | 880,2 s | 55,5 s |
| hot-wallet | req/s | 315,7 | 310,8 |
| hot-wallet | outbox ev/s / drenagem | 35,4 / 337,1 s | 298,5 / 21,2 s |
| hot-drain | drenagem | 15,1 s | 0,5 s |
| sqs | outbox lag p95 | 22,00 s | 0,26 s |
| downstream | entregas / distintos / duplicados / faltando | 55978 / 55978 / 0 / 0 | 48676 / 48676 / 0 / 0 |

Invariantes financeiras e downstream passaram nas duas novas rodadas, e todos os processos encerraram com exit 0. A publicação observada no distributed mudou de 35,9 para 358,3 ev/s; o drain passou de 880,2 para 55,5 s. As rodadas completaram quantidades diferentes de requisições/eventos em laço fechado; os tempos de drain não comparam volumes idênticos. Não atribuímos a variação de req/s exclusivamente ao publisher. Nenhuma alteração local de produto entrou no build dos snapshots.

### Nova execução V3 com três réplicas combinadas

Mesmos parâmetros de V3 acima, mudando apenas o número de réplicas para três. A coluna V1 conserva a execução histórica com artefato bruto e sem assinante downstream; ela não forma um experimento controlado de antes/depois com esta rodada nova.

| Cenário | Métrica | V1 histórico, com artefato | V3 nova execução |
|---|---|---|---|
| distributed | req/s / p99 | 1092,8 / 114,5 ms | 1188,4 / 46,3 ms |
| distributed | outbox ev/s / drenagem | 102,9 / 405,1 s | 570,4 / 63,4 s |
| hot-wallet | req/s / p99 | 252,8 / 296,0 ms | 251,3 / 351,9 ms |
| hot-wallet | outbox ev/s / drenagem | 99,7 / 81,2 s | 498,4 / 0,0 s |
| sqs | msg/s / outbox lag p95 | 83,1 / 6,4 s | 89,9 / 0,25 s |

A nova rodada passou as invariantes financeiras e de downstream; 69244/69244 eventIds distintos recebidos, sem duplicados nem faltantes. O publisher ainda compartilha pool/CPU com o HTTP no modo combinado. Uma execução por configuração não separa esse custo do ruído do host.

### Isolamento histórico: medição sem artefato bruto versionado

As duas configurações de isolamento abaixo não têm `results.json` versionado. Os valores e as conclusões de ganho são relatos históricos, não evidência verificável. A mudança de topologia no runner foi versionada depois da ordem estrita por wallet; não há snapshot identificado do build original que sustente classificá-lo como V3 puro. Por isso não são usados como resultado da nova comparação V2/V3. Para conferir isolamento, é preciso medir combinado e separado sobre o mesmo snapshot, com os resultados e o estado do host preservados.

**Papéis separados no script.** O script passou a subir `LOAD_REPLICAS` processos `APP_ROLES=http` e `LOAD_WORKERS` (padrão = réplicas) processos `APP_ROLES=workers`, cada um com seu pool de 4 conexões. Nenhum código de produto mudou: é a topologia já suportada por `APP_ROLES`. Mesma configuração padrão, uma execução cada:

| Cenário | Métrica | Histórico 3 réplicas (V1, com artefato) | Combinado 3 (histórico sem artefato) | Isolado 3 HTTP + 3 workers (histórico sem artefato) |
|---|---|---|---|---|
| distributed | req/s / p99 | 1092,8 / 114,5 ms | 733,8 / 96,7 ms | 858,0 / 118,8 ms |
| distributed | outbox ev/s / drenagem | 102,9 / 405 s | 429 / 48,7 s | 550,3 / 42,7 s |
| hot-wallet | req/s / p99 | 252,8 / 296 ms | 160,8 / 994,7 ms | 232,7 / 236,9 ms |
| hot-wallet | outbox ev/s / drenagem | 99,7 / 81 s | 309,2 / 0,3 s | 466,7 / 0 s |
| hot-drain | req/s / p99 | — | 257,6 / 239,5 ms | 306,6 / 155,7 ms |
| sqs | msg/s / outbox lag p95 | 83,1 / 6,4 s | 60,6 / 0,28 s | 85,6 / 0,26 s |

Com 1 réplica (1 HTTP + 1 worker contra o combinado): distributed 694,6 → 627,7 req/s (p99 75 → 80 ms), hot-wallet 282,9 → 253,6 req/s (p99 151 → 148 ms), outbox 351,9 → 412,6 ev/s no distributed e 267,5 → 455,9 no hot-wallet. Com uma réplica o pool não era o gargalo; a variação de req/s fica no ruído de execução única num host compartilhado.

Leitura: separar os papéis remove a disputa de pool/CPU no processo HTTP. Com 3 réplicas o hot-wallet volta ao patamar histórico (p99 ~1 s → 237 ms, +45% req/s), o distributed recupera ~17% de req/s, e a outbox ainda drena mais rápido que no modo combinado. Distributed segue abaixo do histórico: o publisher continua disputando o PostgreSQL (2,9 núcleos) e o host, só não mais o pool do processo HTTP. Invariantes financeiras e downstream (0 duplicados, 0 faltando): PASSED nas duas.

A versão compilada dessas medições de isolamento não foi preservada com artefato bruto. Não afirmamos que antecede a ordem estrita por wallet (V4), que serializa a publicação de uma wallet quente em um publisher por vez; os números verificáveis de V4 estão na varredura de VUs. Continua sem `SendMessageBatch`: o teto da outbox é o claim mais um `markPublished` por evento, em transações curtas.

## Canary histórica de correção (13/10/2026)

Histórica: foi substituída como evidência do código com LockMode pela [execução de referência](#execução-de-referência-histórica-13102026), publicada em [docs/evidence](evidence/README.md) (3 HTTP + 3 workers, 32 VUs, 20 s por cenário; 17/17 invariantes; downstream 42703/42703 eventIds, 0 duplicados e 0 faltantes). Os números abaixo ficam só como registro.

Depois da mudança de LockMode e antes da execução de referência, o gate final rodou `bun run test:load` sobre o pacote de entrega, com 3 HTTP + 3 workers, 4 VUs, 8 wallets, 3 s por cenário HTTP, aquecimento de 1 s, 500 BETs no hot-drain e 20 comandos SQS em 4 wallets. **É uma canary de correção, não benchmark nem SLA:** a janela de 3 s e os 4 VUs servem para exercitar o código final sob concorrência real, não para medir capacidade. Os req/s abaixo não se comparam às tabelas anteriores nem as substituem.

| Cenário | Vazão | p50 / p95 / p99 | Erros HTTP |
|---|---:|---|---:|
| distributed | 227,5 req/s | 16 / 31,6 / 39,9 ms | 0% |
| hot-wallet | 133,4 req/s | 28,1 / 47,9 / 58,9 ms | 0% |
| hot-drain | 166 req/s | 22,8 / 33,7 / 41,7 ms | 0% |
| sqs (fim a fim) | 58 msg/s | 154 / 330 / 344 ms | — |

As 17 invariantes passaram e o downstream recebeu 2980/2980 eventIds, sem duplicados nem faltantes. O resultado bruto desta canary também fica fora da entrega.

## Limites do experimento

- **Não é SLA nem capacidade de produção.** Os números valem para um host de desenvolvimento compartilhado com outras cargas, com gerador, banco e fila na mesma máquina e PostgreSQL sem tuning.
- LocalStack não reproduz a latência, os limites de vazão nem o throttling do SQS da AWS. A latência real de rede entre serviço, banco e fila também não foi medida.
- O modelo é de laço fechado: um cliente só envia depois da resposta anterior. Ele não mede chegadas abertas e subestima a latência sob saturação (coordinated omission).
- Uma execução por configuração e 10–30 s por cenário: não há intervalo de confiança nem teste de longa duração, e ficam de fora vazamentos, crescimento de tabelas e autovacuum. Três execuções da mesma configuração (64 VUs, 30 s) variaram ~34% quando outra carga disputava o host.
- A carga HTTP (`distributed` e `hot-wallet`) cobre BET, WIN, LOSS 0.00, REFUND e ROLLBACK contra BET confirmada; `hot-drain` segue só BET e `sqs` segue BET/WIN. Replay idempotente e referência pendente sob carga não foram medidos; a correção desses fluxos está na matriz de testes de integração, não neste experimento.
- O lag da outbox usa o `created_at` da transação (`now()` no início da transação) contra o relógio da aplicação no `published_at`, ambos no mesmo host.
