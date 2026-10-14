# Política IAM mínima das filas

A fila de comandos `wager-transactions.fifo` é um **canal interno**: o consumer aplica o mesmo efeito financeiro do HTTP e não passa pelo `ProviderAuthGuard` (ADR-010 de [ARCHITECTURE.md](../../ARCHITECTURE.md)). Por isso, quem pode publicar nela é decidido pela política IAM. Isso é um **requisito de implantação**: o repositório não cria contas nem políticas, e o exemplo abaixo não foi aplicado numa conta AWS real.

Os ARNs usam marcadores (`REGION`, `ACCOUNT_ID`). Cada papel recebe só as ações que o código chama (`src/messaging/`, `src/observability/observability.ts`, `src/observability/readiness.ts`). A operação de DLQ não tem código no repositório; suas ações são o mínimo para inspecionar e drenar pelo console ou CLI.

| Principal | Fila | Ações |
|---|---|---|
| Produtor de comandos (serviço interno que gera apostas) | `wager-transactions.fifo` | `sqs:SendMessage` |
| Worker (`APP_ROLES` com `workers`) | `wager-transactions.fifo` | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes` |
| Worker, publisher da outbox | `wager-events.fifo` | `sqs:SendMessage` |
| Consumer de eventos (`SqsEventConsumer`) | `wager-events.fifo` | `sqs:ReceiveMessage`, `sqs:DeleteMessage` |
| Readiness/métricas (toda réplica) | filas e DLQs monitoradas | `sqs:GetQueueAttributes` |
| Operação de DLQ (inspeção e drenagem) | `*-dlq.fifo` | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` |
| Redrive de DLQ (humano ou job, opcional) | DLQ de origem do move; fila de destino | DLQ: `sqs:StartMessageMoveTask`, `sqs:CancelMessageMoveTask`, `sqs:ListMessageMoveTasks`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`, `sqs:ListDeadLetterSourceQueues`; destino: `sqs:SendMessage` |

O consumer de eventos não altera visibilidade nem lê atributos: falha transitória deixa a mensagem expirar e voltar pela redrive policy. Se uma réplica sem `workers` atender `/health/ready`, ela usa o papel de readiness/métricas, não o do consumer. Só o worker de comandos chama `ChangeMessageVisibility` (heartbeat de visibilidade, retry com atraso e devolução da mensagem) e `GetQueueAttributes` na própria fila (lê `RedrivePolicy`).

Nenhum papel recebe `sqs:SendMessage` nas DLQs: o SQS move a mensagem pela redrive policy. O papel de redrive só envia à fila de destino (a origem ou um destino FIFO explícito), nunca à DLQ, e não pertence à aplicação. Nenhum recebe `sqs:*`, `CreateQueue`, `DeleteQueue`, `PurgeQueue` ou `SetQueueAttributes`.

## Produtor de comandos

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SendWagerCommandsOnly",
      "Effect": "Allow",
      "Action": ["sqs:SendMessage"],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:wager-transactions.fifo"
    }
  ]
}
```

## Worker

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConsumeWagerCommands",
      "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:wager-transactions.fifo"
    },
    {
      "Sid": "PublishOutboxEvents",
      "Effect": "Allow",
      "Action": ["sqs:SendMessage"],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:wager-events.fifo"
    },
    {
      "Sid": "ObserveQueueDepth",
      "Effect": "Allow",
      "Action": ["sqs:GetQueueAttributes"],
      "Resource": [
        "arn:aws:sqs:REGION:ACCOUNT_ID:wager-events.fifo",
        "arn:aws:sqs:REGION:ACCOUNT_ID:wager-transactions-dlq.fifo",
        "arn:aws:sqs:REGION:ACCOUNT_ID:wager-events-dlq.fifo"
      ]
    }
  ]
}
```

## Consumer de eventos

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConsumeWagerEvents",
      "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage"],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:wager-events.fifo"
    }
  ]
}
```

## Operação da DLQ

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "InspectAndDrainDlq",
      "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
      "Resource": [
        "arn:aws:sqs:REGION:ACCOUNT_ID:wager-transactions-dlq.fifo",
        "arn:aws:sqs:REGION:ACCOUNT_ID:wager-events-dlq.fifo"
      ]
    }
  ]
}
```

## Redrive da DLQ (opcional)

Necessário só para devolver mensagens com `StartMessageMoveTask` (console "Start DLQ redrive" ou API). Sem `DestinationArn`, o destino é a fila de origem. Exemplo para a DLQ de comandos voltando a `wager-transactions.fifo`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RedriveFromCommandsDlq",
      "Effect": "Allow",
      "Action": ["sqs:StartMessageMoveTask", "sqs:CancelMessageMoveTask", "sqs:ListMessageMoveTasks", "sqs:ReceiveMessage",
        "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ListDeadLetterSourceQueues"],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:wager-transactions-dlq.fifo"
    },
    {
      "Sid": "SendRedrivenToSource",
      "Effect": "Allow",
      "Action": ["sqs:SendMessage"],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:wager-transactions.fifo"
    }
  ]
}
```

Antes do redrive de comandos, a causa da falha permanente precisa estar resolvida; a inbox torna a reentrega idempotente, mas uma mensagem venenosa volta à DLQ após `maxReceiveCount`.

## Criptografia SSE-SQS e SSE-KMS

Fontes: AWS SQS Developer Guide, [Key management](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-key-management.html), [Encryption at rest](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html) e [Configuring queue permissions for dead-letter queue redrive](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-dead-letter-queue-redrive.html); AWS KMS, [kms:ViaService](https://docs.aws.amazon.com/kms/latest/developerguide/conditions-kms.html#conditions-kms-via-service). Nada abaixo foi aplicado numa conta real.

- **SSE-SQS.** Chaves gerenciadas pelo SQS; nenhuma permissão KMS.
- **SSE-KMS com a chave gerenciada pela AWS (`aws/sqs`).** A key policy não pode ser alterada e já autoriza os principais da própria conta que podem usar o SQS; as políticas IAM abaixo continuam válidas, mas não há como restringir a key policy nem usar a chave entre contas.
- **SSE-KMS com chave gerenciada pelo cliente.** Cada papel precisa das permissões KMS abaixo, além das ações SQS acima, em **todas** as chaves que protegem as mensagens que ele envia ou lê. Na tabela, `K_CMD` é a chave de `wager-transactions.fifo`, `K_EVT` a de `wager-events.fifo`, `K_CMD_DLQ` e `K_EVT_DLQ` as das DLQs. As chaves podem coincidir; o guia não presume isso.

| Papel | Operação | Permissões KMS | Chaves (`Resource`) |
|---|---|---|---|
| Produtor de comandos | `SendMessage` em `wager-transactions.fifo` | `kms:GenerateDataKey`, `kms:Decrypt` | `K_CMD` |
| Worker, consumer de comandos | `ReceiveMessage` em `wager-transactions.fifo` | `kms:Decrypt` | `K_CMD` e chaves anteriores ainda presentes em mensagens retidas |
| Worker, publisher da outbox | `SendMessage` em `wager-events.fifo` | `kms:GenerateDataKey`, `kms:Decrypt` | `K_EVT` |
| Consumer de eventos | `ReceiveMessage` em `wager-events.fifo` | `kms:Decrypt` | `K_EVT` e chaves anteriores ainda presentes |
| Readiness/métricas | `GetQueueAttributes` | nenhuma | — |
| Inspeção/drenagem de DLQ | `ReceiveMessage` em `*-dlq.fifo` | `kms:Decrypt` | `K_CMD_DLQ`, `K_EVT_DLQ` **e as chaves das filas de origem** (`K_CMD`, `K_EVT`), inclusive anteriores |
| Redrive de DLQ | move da DLQ para o destino | `kms:Decrypt` nas chaves da DLQ e da origem (como acima); `kms:GenerateDataKey` e `kms:Decrypt` na chave do destino | ex.: `K_CMD_DLQ`, `K_CMD` (origem e destino) |

- **Por que Decrypt no envio.** Ao expirar o período de reuso da data key, o próximo `SendMessage`/`SendMessageBatch` chama `kms:GenerateDataKey` e `kms:Decrypt`; o Decrypt verifica a integridade da nova data key. Por isso quem envia (produtor, publisher da outbox, redrive no destino) precisa das duas.
- **Por que as chaves da origem na DLQ.** Mover a mensagem para a DLQ preserva a criptografia com que ela foi gravada. Pela AWS, se a fila atua como DLQ, o consumer precisa de `kms:Decrypt` também em toda chave usada nas mensagens da fila de origem; o mesmo vale para o redrive. Isso inclui uma DLQ sem SSE que recebe mensagens já criptografadas na origem.
- **Troca de chave.** Mensagens existentes continuam protegidas pela chave antiga. Mantenha a chave antiga habilitada, com `kms:Decrypt` e `kms:GenerateDataKey` permitidos ao SQS na key policy, e com Decrypt para os consumers, até que todas as mensagens antigas (filas e DLQs, dentro da retenção) tenham sido processadas.
- **`Resource`.** A AWS exige o ARN completo da chave, com região, no `Resource` das políticas IAM para KMS; não use `*` nem curingas de chave. Prefira a condição `kms:ViaService` = `sqs.REGION.amazonaws.com`, para que a chave só seja usada por meio do SQS.
- **Key policy.** Toda chave KMS tem key policy. Numa chave gerenciada pelo cliente, ela precisa permitir os mesmos principais e ações, diretamente ou pela declaração que delega à política IAM da conta. A AWS distingue solicitantes por credencial (conta, usuário ou role, inclusive a mesma role com políticas de escopo diferentes), mas não por `RoleSessionName`; não baseie a key policy só no nome da sessão.
- **Entre contas.** Se produtor, workers ou operação ficarem em outra conta, é preciso chave gerenciada pelo cliente: a key policy autoriza o principal externo e a política IAM na conta dele concede as mesmas ações no ARN da chave, além da queue policy na conta das filas. `aws/sqs` não serve para esse caso.
- **Serviços AWS como origem.** Não se aplica hoje: só a aplicação publica nas filas. Se EventBridge, S3 ou SNS passarem a publicar, a key policy precisa permitir o principal de serviço (`kms:GenerateDataKey`, `kms:Decrypt`), com `aws:SourceArn`/`aws:SourceAccount` contra confused deputy.

## Defesa adicional

- Na queue policy da fila de comandos, negar `sqs:SendMessage` a qualquer principal diferente do produtor (`Deny` com `aws:PrincipalArn` fora da lista). Se o redrive de comandos for usado, o principal de redrive precisa constar da exceção, senão o move para a origem falha.
- Se as filas forem restritas por VPC (`aws:SourceVpc`), o redrive exige exceção para chamadas do próprio SQS (`aws:CalledViaLast` = `sqs.amazonaws.com` ou `aws:ViaAWSService`), na fila de origem e na DLQ.
- Validar as políticas IAM, a queue policy e as key policies na conta real com o IAM Access Analyzer ou o policy simulator, sem enviar mensagens de prova.
