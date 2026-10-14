# Evidências da execução de referência

## Execução de referência de 14/10/2026

Execução de referência atual de `bun run check` e `bun run test:integration`, sobre o código com multimoeda e `LockMode.PESSIMISTIC_WRITE`. Medida em 2026-10-14, das 02:58:40 às 03:05:52 BRT (America/Sao_Paulo), na mesma máquina descrita em [Ambiente](#ambiente), com o host compartilhado. Uma única execução de cada comando, sem outra carga pesada em paralelo; a reprodução usou sufixo e portas próprios (`JWP_REPRO_SUFFIX` e `JWP_REPRO_*_PORT`).

- sourceCommit executado: `9eae78972477b528714f85f8e66631668f8b4e96`. As mudanças posteriores a esse commit são apenas de documentação e evidências.

| Comando | Resultado medido | Exit code | Evidência |
|---|---|---:|---|
| bun run check | 133 unidade + 2 smoke, 0 falhas, 925 asserções (920 + 5); typecheck/build passaram | 0 | [reference-2026-10-14/check-summary.json](reference-2026-10-14/check-summary.json) |
| bun run test:integration | 83 etapas conforme esperado; 438 testes positivos, 0 falhas, 4251 asserções | 0 | [reference-2026-10-14/integration-summary.json](reference-2026-10-14/integration-summary.json) |

As duas execuções passaram na primeira tentativa; não houve reexecução. As contagens da reprodução foram recalculadas a partir dos logs de cada etapa e coincidem com o total impresso pelo script (campo `recomputedFromStepLogs`). Os JSONs trazem só resultados agregados e o veredito de cada etapa, sem caminhos do host nem credenciais.

## Carga da candidata final (14/10/2026)

Referência atual de carga. Em 14/10/2026 a candidata final (commit `9679919`) foi medida com [scripts/load/final-candidate.sh](../../scripts/load/final-candidate.sh) nas políticas `per_type` e `per_reference` (3 réplicas HTTP + 3 workers, 32 VUs, 20 s), concluindo às 14:16 BRT (America/Sao_Paulo). As 17 invariantes financeiras passaram nas duas políticas, com 0% de erros. Distribuído: 583/543 req/s, p95 105/130 ms, p99 142/183 ms, contra 718 req/s, p95 93 ms e p99 235 ms na medição histórica de 13/10, anterior à multimoeda; SQS 1000 comandos a 109/88 msg/s (antes ~142). A queda de ~20–35% é compatível com o custo da multimoeda, mas a máquina era compartilhada e a comparação não é controlada.

| Política | Cenário | Requisições | req/s | p50 / p95 / p99 (ms) | máx (ms) | Erros HTTP | Lag outbox p50 / p95 / p99 (ms) | Drenagem após carga (ms) |
|---|---|---:|---:|---|---:|---:|---|---:|
| per_type | distributed | 11696 | 583.5 | 50.3 / 104.9 / 142.4 | 225.2 | 0% | 19362 / 28368 / 29070.6 | 29326 |
| per_type | hot-wallet | 2916 | 144.0 | 197.7 / 420.6 / 726.9 | 1081.3 | 0% | 18879.5 / 28456 / 28696.7 | 27974 |
| per_type | hot-drain | 500 | 242.0 | 123.3 / 174.3 / 203 | 213.8 | 0% | 1141.5 / 1567 / 1594 | 754 |
| per_reference | distributed | 10897 | 543.3 | 51.6 / 129.5 / 183 | 349.3 | 0% | 20720 / 28629.3 / 29696.3 | 29879 |
| per_reference | hot-wallet | 3351 | 166.1 | 166.8 / 358.4 / 407.4 | 510.5 | 0% | 18677.5 / 35987.1 / 36760.8 | 36102 |
| per_reference | hot-drain | 500 | 202.9 | 148.2 / 242.1 / 307.6 | 347.8 | 0% | 1159 / 1710 / 1746 | 1257 |

| Política | SQS (1000 comandos) | Ponta a ponta p50 / p95 / p99 (ms) | Downstream | Invariantes | Exit dos 6 processos |
|---|---|---|---|---|---|
| per_type | 9.16 s, 109.1 msg/s | 4348 / 8542 / 8894 | 33349/33349 eventIds distintos, 0 duplicados, 0 faltantes | 17/17 | 0 |
| per_reference | 11.36 s, 88.1 msg/s | 6186 / 10630 / 10971 | 32496/32496 eventIds distintos, 0 duplicados, 0 faltantes | 17/17 | 0 |

As duas políticas tiveram desempenho equivalente dentro do ruído: cada uma rodou uma vez, em máquina compartilhada, e as diferenças (10–20%) vão para os dois lados (`per_type` à frente no distribuído, hot-drain e SQS; `per_reference` à frente no hot-wallet). O custo extra do `per_reference` é uma checagem indexada mais um advisory lock por referência no gatilho (`pg_advisory_xact_lock`); seu objetivo é impedir que REFUND seguido de ROLLBACK devolva a aposta duas vezes ([ADR-005](../../ARCHITECTURE.md#jwp-adr-005--referências-estados-e-política-de-reversão)), não ganhar desempenho.

Mesma máquina descrita em [Ambiente](#ambiente), host compartilhado (média de carga de 1 min no início: 2.86 em `per_type` e 4.99 em `per_reference`). Uma rodada por política, sem SLA nem estimativa de capacidade AWS. Outputs: [run.json](load/final-2026-10-14/run.json); `per_type` [summary.md](load/final-2026-10-14/per_type/summary.md) e [results.json](load/final-2026-10-14/per_type/results.json); `per_reference` [summary.md](load/final-2026-10-14/per_reference/summary.md) e [results.json](load/final-2026-10-14/per_reference/results.json). Como na execução histórica, apenas as amostras de linhas de log foram omitidas dos results.json; o hash do bruto consta de `publication`.

## Execução de referência de 13/10/2026 (histórico)

As seções abaixo descrevem a execução publicada anteriormente, preservada como histórico. Ela antecede a multimoeda; as contagens de check e reprodução integral atuais são as da seção anterior. Compose continua sendo a medição publicada desta execução; a carga padrão abaixo é histórica e foi substituída como referência pela [carga da candidata final](#carga-da-candidata-final-14102026).

Resultados medidos em 2026-10-13T19:32:29.548025-03:00 até 2026-10-13T19:41:57.802879-03:00, BRT (America/Sao_Paulo). Os JSONs desta pasta acompanham o pacote público. São resumos dos comandos concluídos, não resultados planejados.

## Identidade do código

- Execução histórica, anterior à multimoeda, à política de reversão configurável e às métricas de contenção. O commit medido não faz parte do histórico publicado deste repositório, então os resumos abaixo não são atribuídos a esta versão; os campos de commit dos JSONs foram trocados por `historical-pre-multicurrency`.
- O código medido já utilizava LockMode.PESSIMISTIC_WRITE. A lista de arquivos exportados e o digest da árvore ficaram em [product-tree.json](product-tree.json), como registro daquela execução.
- A versão final é medida com [scripts/load/final-candidate.sh](../../scripts/load/final-candidate.sh); ver [docs/LOAD_TEST.md](../LOAD_TEST.md#execução-da-versão-final).

## Ambiente

- CPU: 11th Gen Intel(R) Core(TM) i7-11800H @ 2.30GHz; 16 CPUs lógicas.
- RAM: 15.36 GiB.
- SO: Ubuntu 26.04.1 LTS; kernel Linux 7.0.0-38-generic.
- Bun: 1.3.14; Docker Engine: 29.1.3; Compose: 2.40.3+ds1-0ubuntu1.
- PostgreSQL: 17.6; LocalStack: localstack/localstack:4.9.2.
- Aplicação, banco, emulador SQS e gerador na mesma máquina. Host compartilhado; serviços não relacionados permaneceram ativos. Média de carga de 1 min no início: 2.09.
- Antes da carga, somente containers remanescentes com prefixo do projeto foram encerrados. Nenhum serviço de entrada compartilhada nem filtro DNS foi alterado.
- Metadados adicionais: [environment.json](environment.json).

## Comandos e resultados

| Comando | Resultado medido | Exit code | Evidência |
|---|---|---:|---|
| bun install --frozen-lockfile && bun run check | 120 unidade + 2 smoke, 0 falhas, 777 asserções; typecheck/build passaram | 0 | [check/summary.json](check/summary.json) |
| bun run test:integration | 80 etapas conforme esperado; 398 testes positivos, 0 falhas, 4022 asserções | 0 | [integration/summary.json](integration/summary.json) |
| bun run test:compose | 18/18 checks; 3 réplicas e limpeza concluída | 0 | [compose/summary.json](compose/summary.json) |
| bun run test:load | 17/17 invariantes; todos os processos encerrados com 0 | 0 | [load/summary.json](load/summary.json), [load/results.json](load/results.json) |

A primeira tentativa integral não passou: 80 etapas, 397 testes positivos, 2 falhas e 4015 asserções, exit 1. M14 e seu hook de limpeza excederam timeout. A causa não foi estabelecida; o produto e os testes não foram alterados antes da segunda reprodução. Preservamos [o resumo dessa tentativa](integration/attempt-1-summary.json). A tabela acima apresenta a segunda reprodução integral, sem omitir a primeira. A causa foi identificada depois (seção abaixo). O resumo da 1ª tentativa ganhou o campo `causeAnalysis` e mantém a classificação original.

As contagens da reprodução somam testes e asserções dos passos cuja expectativa é sucesso. As tentativas sem infraestrutura precisam falhar e têm seus exits/verdicts registrados separadamente. Não são falhas inesperadas nem testes omitidos.

## Causa da falha de M14 na 1ª tentativa

Identificada depois desta execução. A correção alterou só o harness de testes, não o produto.

- **Causa.** No helper `Child.stop()` da matriz, depois do exit do processo, a espera pelo fim dos pipes stdout/stderr não tinha prazo. O guarda SIGTERM→SIGKILL de 20 s não cobria essa espera. Na 1ª tentativa, o Bun informou "killed 3 dangling processes" no timeout de M14. O Bun 1.3.14 conta todos os subprocessos ainda vivos do arquivo, o que conferimos com um teste mínimo. Os 3 vivos eram as APIs, que M14 não para. Logo, os 7 workers já tinham saído e M14 estava preso nessa espera. O `afterAll` chamou `stop()` de novo, esperou o mesmo dreno até 120 s e não executou a limpeza (down das migrations e DROP ROLE).
- **Limite da prova.** A causa foi deduzida dessa contagem; o travamento não foi reproduzido. Nenhum dos testes abaixo travou:
  - cerca de 980 ciclos sintéticos de processos filhos, com e sem stress de CPU, dentro e fora do `bun test`;
  - 28 execuções reais da matriz.
  O motivo de o EOF do pipe não ter chegado naquela execução continua desconhecido.
- **Correção.**
  - `stop()` idempotente: M14 e `afterAll` compartilham a mesma parada.
  - Dreno limitado a 5 s após o exit (`DRAIN_GRACE_MS` em `tests/matrix/delivery/support.ts`). Se o prazo vence, o helper registra a linha de prova `{"proof":"child-drain-timeout",...}` e devolve `drained:false`. Exit code e sinal continuam verificados.
  - `afterAll` com `Promise.allSettled`, para a limpeza sempre rodar.
  - `AbortSignal.timeout` de 20 s nas chamadas HTTP da matriz, acima do budget de 15 s da TX da API.
  - Teste determinístico [child-stop.test.ts](../../tests/matrix/delivery/child-stop.test.ts) com um pipe que nunca chega ao EOF.
- **Independência da causa.** A correção não depende do motivo de o EOF faltar. Qualquer espera de dreno agora termina em no máximo 5 s, deixa rastro e não bloqueia a limpeza. Nenhum timeout de teste foi aumentado.
- **Resultados após a correção.** A correção faz parte do código medido na [execução de referência de 14/10/2026](#execução-de-referência-de-14102026), cuja reprodução integral passou na primeira tentativa, com M14 e o teste child-stop incluídos (83 etapas, 438 testes, 0 falhas).
- **Escopo das evidências.** A execução desta pasta é do sourceCommit acima. [product-tree.json](product-tree.json) continua descrevendo a árvore medida e não foi recalculado, pois os testes mudaram depois. Nenhum outro arquivo desta pasta tem manifesto de hash que inclua o resumo da 1ª tentativa.

## Carga padrão após LockMode

Configuração sem redução: 3 HTTP + 3 workers, 32 VUs, 20 s por distributed/hot-wallet, 5 s de aquecimento descartado, 200 wallets distribuídas, 500 apostas hot-drain e 1000 comandos SQS em 100 wallets. Pool de 4 conexões por processo.

| Cenário | Requisições | req/s | p50 / p95 / p99 (ms) | Erros HTTP | Retries / falhas SQL transitórias | Máximos amostrados no banco | Lag outbox p50 / p95 / p99 (ms) | Drenagem após carga (ms) |
|---|---:|---:|---|---:|---|---|---|---:|
| distributed | 14384 | 718.2 | 34.3 / 93.4 / 235.4 | 0% | 0 / 0 | {"outbox_pending": 19181, "lock_waiters": 1, "active_sessions": 25, "waiting_on_lock": 1} | 20606 / 32378 / 33291 | 33581 |
| hot-wallet | 4611 | 229.1 | 136.9 / 231.6 / 282.9 | 0% | 0 / 0 | {"outbox_pending": 5695, "lock_waiters": 11, "active_sessions": 15, "waiting_on_lock": 11} | 24907.5 / 43656.2 / 44439 | 43819 |
| hot-drain | 500 | 259.9 | 112.4 / 168.1 / 221.3 | 0% | 0 / 0 | {"outbox_pending": 155, "lock_waiters": 11, "active_sessions": 12, "waiting_on_lock": 11} | 787 / 1126.1 / 1166 | 1255 |

Os máximos amostrados no banco incluem esperas de lock; não representam necessariamente conflitos que resultaram em retry. Os valores completos e a série temporal estão em results.json. Erro HTTP segue a definição do runner: transporte ou status fora de 200/202/422; rejeições de negócio esperadas são contabilizadas nos outcomes.

Invariantes: saldo materializado igual à soma do ledger, nenhum saldo negativo, nenhum lançamento duplicado, eventos completamente publicados, reconciliação HTTP consistente e filas/DLQs verificadas. Downstream: 42703/42703 eventIds distintos, 0 duplicados, 0 faltantes. As 6 réplicas/processos encerraram com exit 0.

Uma rodada em host de desenvolvimento com LocalStack; sem SLA, comparação entre máquinas ou estimativa de capacidade AWS. O modelo de carga é de laço fechado e não mede chegadas abertas. Esta execução substitui a canary curta como evidência da configuração padrão neste código; as medições antigas continuam históricas.

## Preservação e sanitização

Os outputs integrais permanecem em cache local privado. Publicamos contagens por suíte, todos os resultados agregados da carga e hashes. Amostras de linhas de log foram omitidas de results.json; métricas financeiras, configuração, tempos, outcomes e invariantes não foram alterados. O hash do resultado bruto e a descrição dessa transformação constam de publication no JSON. Sem dados de autenticação ou identidade interna da máquina. Reexecutar os comandos produz uma medição nova, não cópia numérica desta.
