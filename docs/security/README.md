# Auditoria de segurança

Resumo curado da auditoria de segurança dos caminhos financeiros desta entrega. O registro de cada hipótese avaliada está em [FINDINGS.md](FINDINGS.md).

## Resultado

- **Achados confirmados: nenhum** dentro do escopo testado.
- **Precisam de validação (sem severidade): 2**, ambos dependem de fatos de implantação que não estão no repositório.
- **Hipóteses rejeitadas com evidência: 12**, por leitura de código e, quando o efeito era dinâmico, por teste em sandbox.

Este resultado **não** significa que a aplicação não tenha vulnerabilidades. Ele vale para a matriz descrita abaixo, nas condições descritas abaixo.

## Método

- Auditoria completa com o `security-audit-skill` (Cloudflare), em escopo fechado: `src`, migrations, workers, Dockerfile, Compose e o gerador do pacote de entrega.
- Reconhecimento em quatro leituras do código, mais um verificador independente sobre os controles financeiros, HTTP, SQS e o modo local.
- Os testes dinâmicos rodaram só em containers Docker numa rede interna, sem portas publicadas no host, sem rede externa, sem variáveis do ambiente do host e com o código montado somente leitura. Postgres 17.6 e LocalStack 4.9.2. Nenhum tráfego foi enviado a serviços reais (AWS, GitHub ou outros).
- `findings.json` (14 registros) e o ledger de cobertura passaram nos validadores do skill.

## O que foi testado e segurou

- **Débito concorrente:** 20 BET paralelos de 1.00 sobre saldo 10.00 resultaram em 10 processados e 10 `INSUFFICIENT_FUNDS`. Saldo final 0.00, reconciliação consistente, diferença 0.00.
- **Idempotência:** mesma `Idempotency-Key` com outro valor retorna 409 `IDEMPOTENCY_CONFLICT`. O replay idêntico volta como `idempotentReplay`, com um único débito.
- **Reversão paralela:** ROLLBACK, ROLLBACK e REFUND em paralelo sobre o mesmo BET. Um é processado, dois voltam `ALREADY_REVERSED` e o saldo é restaurado uma vez.
- **Quantias inválidas:**
  - amount negativo: 400;
  - saque acima do saldo: 422 `INSUFFICIENT_FUNDS`;
  - `1.001`, `1.2`, `1.230`, `01.00`, notação científica, `NaN`, `Infinity` e número JSON: 400;
  - abertura com 0.00: 201, como o contrato permite.
- **Identificadores com aspas e comentário SQL:** gravados e relidos como texto pelo par exato, sem atraso nem efeito colateral.
- **Paginação entre carteiras:** o cursor de ledger da carteira A usado na carteira B retorna 400 `INVALID_CURSOR`.
- **Log e `correlationId`:**
  - o `correlationId` HTTP é um UUID gerado pelo servidor;
  - o header de correlação enviado pelo cliente não entra no log;
  - quebras de linha não forjam linhas novas, e todas as linhas do log são JSON.
- **SQS:** entrega duplicada não debita de novo. WIN fora de ordem liquida uma vez. Corpo inválido vai para a DLQ sem bloquear outra carteira.
- **Segredos:** nenhum `.env` commitado, nenhuma chave de nuvem ou chave privada no histórico. As únicas credenciais são as locais de desenvolvimento, documentadas no Compose e em `.env.example`.

## Precisa de validação

Nenhum dos dois itens tem severidade atribuída. Ambos dependem de como a aplicação é implantada.

| Item | Onde | Por que fica em aberto | Como validar |
|---|---|---|---|
| Exposição do modo local | `src/shared/http/provider-identity.ts`, `Dockerfile`, `compose.yml` | A imagem define `HOST=0.0.0.0`; o Compose local publica a porta só em `127.0.0.1`. O bind e o adapter de identidade de uma implantação real não estão no repositório. | Confirmar o endereço de escuta e que a implantação injeta um adapter de IdP no lugar de `LocalProviderIdentity`. |
| Quem pode publicar na fila de comandos | `src/messaging/sqs-wager-consumer.ts` | O consumer trata a fila como canal interno e aplica o mesmo efeito financeiro do HTTP. A política IAM (`sqs:SendMessage`) não está no repositório. | Revisar a política da fila de comandos na conta real, sem enviar mensagens de prova. |

O guard de provedor sem autenticação (`LocalProviderIdentity`, guard no-op) é a extensão do desafio descrita no ADR-010 de [ARCHITECTURE.md](../../ARCHITECTURE.md), não um achado. Uma implantação pública precisa do adapter de IdP antes de expor a API.

## Escopo e limites

- **Cobertura:** o ledger de cobertura tem 13 unidades. 11 foram fechadas e 2 continuam abertas por dependerem da implantação (a tabela acima).
- **Sem crítico de cobertura:** a etapa de crítico de cobertura do skill **não** foi executada. Por isso a auditoria não declara cobertos os caminhos de `src` fora da matriz testada.
- **Fora do escopo:** IdP real, infraestrutura de nuvem, políticas IAM e ingress, imagem em produção.
- **Imagem:** a imagem não fixa `USER`, e o build usa a rede do host para instalar dependências. Nada disso foi exercitado nesta auditoria.
