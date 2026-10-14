# Registro das hipóteses avaliadas

Os 14 registros da auditoria resumida em [README.md](README.md): 0 confirmados, 2 que precisam de validação e 12 rejeitados com evidência. Nenhum registro tem severidade atribuída. Um registro rejeitado vale só para o cenário testado; não é prova de ausência de falha em outros caminhos.

| Identificador | Hipótese | Veredito | Evidência ou bloqueio |
|---|---|---|---|
| `jwp.deploy.local-identity-exposure` | Exposição do modo local depende do bind e do adapter | precisa de validação | O repositório não registra o ingress, o bind efetivo nem o adapter injetado fora deste ambiente local. |
| `jwp.deploy.sqs-producer-iam` | Autorização de quem publica na fila de comandos não está no repositório | precisa de validação | Política IAM, resource policy da fila e isolamento de rede da conta não estão no repositório e não foram consultados. |
| `jwp.http.ledger-cursor-idor` | Paginação com o cursor de ledger de outra wallet | rejeitado | No sandbox, o nextCursor da wallet A usado na wallet B retornou 400 INVALID_CURSOR. |
| `jwp.http.log-injection` | Injeção de log pelo correlationId | rejeitado | No sandbox, 500 linhas de log eram JSON. O header X-Correlation-Id do cliente não apareceu. O marcador com quebra de linha permaneceu dentro de um único objeto. |
| `jwp.http.sqli` | SQL injection em identificadores da aposta | rejeitado | No sandbox, os identificadores com aspas e comentário SQL foram gravados e relidos pelo par exato. A chamada durou 6 ms, sem pg_sleep, e a carteira continuou acessível. |
| `jwp.secrets.git-and-evidence` | Segredo de produção no histórico, em docs/evidence ou no gerador | rejeitado | A varredura não imprimiu valores. Não há commit de .env, nem AKIA, nem chave privada no histórico. docs/evidence não teve URL com senha. Os únicos itens dessa classe foram as credenciais locais já documentadas em compose.yml, compose.db.yml e .env.example. |
| `jwp.sqs.duplicate-delivery` | Entrega SQS duplicada debita duas vezes | rejeitado | No sandbox, depois das entregas duplicadas o saldo foi 4.50, exatamente um BET de 1.50 sobre 6.00. |
| `jwp.sqs.out-of-order-win` | WIN fora de ordem credita duas vezes | rejeitado | No sandbox o saldo final foi 4.50, compatível com um BET inicial, o BET referenciado e um único crédito do WIN. |
| `jwp.sqs.poison-cross-group` | Mensagem inválida bloqueia outra carteira | rejeitado | No sandbox a outra carteira chegou a 3.00. Dois corpos quebrados de 9 bytes estavam na DLQ de comandos e a fila principal ficou vazia. |
| `jwp.wager.concurrent-overspend` | Débito concorrente deixa saldo negativo | rejeitado | No sandbox houve 10 PROCESSED e 10 INSUFFICIENT_FUNDS, saldo 0.00, reconciliação consistent true e difference 0.00. |
| `jwp.wager.idempotency-replay-mismatch` | Replay da mesma chave com outro valor | rejeitado | No sandbox o replay com outro valor retornou 409 IDEMPOTENCY_CONFLICT, o replay idêntico veio com idempotentReplay true e o saldo refletiu um único débito de 1.00. |
| `jwp.wager.money-rounding` | Quantia não canônica é aceita e arredondada | rejeitado | No sandbox 1.001, 1.2, 1.230, 01.00, negativo, notação científica, NaN, Infinity e number receberam 400. A abertura 0.00 recebeu 201, que é o comportamento permitido em specs.md. |
| `jwp.wager.negative-balance` | Aposta negativa ou acima do saldo reduz a carteira além do disponível | rejeitado | No sandbox o amount negativo retornou 400, o overdraft retornou 422 INSUFFICIENT_FUNDS e o saldo permaneceu 5.00. |
| `jwp.wager.parallel-reversal` | Reversões paralelas creditam o saldo mais de uma vez | rejeitado | No sandbox um comando veio PROCESSED e os outros dois ALREADY_REVERSED. O saldo voltou ao valor anterior ao BET. |
