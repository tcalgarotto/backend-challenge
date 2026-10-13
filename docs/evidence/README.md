# Evidências da execução de referência

Resultados medidos em 2026-10-13T19:32:29.548025-03:00 até 2026-10-13T19:41:57.802879-03:00, BRT (America/Sao_Paulo). Os JSONs desta pasta acompanham o pacote público. São resumos dos comandos concluídos, não resultados planejados.

## Identidade do código

- sourceCommit executado: `8175cfebdb87a002417a28f4d2779a458fb58caf`.
- Entrega pública comparada: `42d3083f1c56f3cbbe21f6e4775869ed28dd5edd`.
- Produto exportado: 142 arquivos, byte a byte idênticos; nenhuma diferença em src, tests, scripts, Compose, package.json ou bun.lock. Lista e digest em [product-tree.json](product-tree.json).
- Digest da árvore protegida exportada: `a2c622e84f0f3a012bda7b93a402eef936e4e66f213fd2f3a89c4a8ca5b948ae`.
- O código medido já utiliza LockMode.PESSIMISTIC_WRITE. As únicas mudanças posteriores desta tarefa são documentação, evidências e a regra de exportação desta pasta; nenhuma mudança no produto.

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

A primeira tentativa integral não passou: 80 etapas, 397 testes positivos, 2 falhas e 4015 asserções, exit 1. M14 e seu hook de limpeza excederam timeout. A causa não foi estabelecida; o produto e os testes não foram alterados antes da segunda reprodução. Preservamos [o resumo dessa tentativa](integration/attempt-1-summary.json). A tabela acima apresenta a segunda reprodução integral, sem omitir a primeira.

As contagens da reprodução somam testes e asserções dos passos cuja expectativa é sucesso. As tentativas sem infraestrutura precisam falhar e têm seus exits/verdicts registrados separadamente. Não são falhas inesperadas nem testes omitidos.

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
