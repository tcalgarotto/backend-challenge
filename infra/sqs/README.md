# SQS (LocalStack) bootstrap
LocalStack 4.9.2 community, SQS only, no auth token. The ready hook
`init/01-create-queues.sh` creates the FIFO topology declared in
`contracts/messaging/topology.json`; the healthcheck turns healthy only after
the hook reports SUCCESSFUL, so `--wait` returns with the queues in place.

    docker compose -p jwp-sqs -f compose.sqs.yml up -d --wait
    bun run test:sqs
    docker compose -p jwp-sqs -f compose.sqs.yml down -v

Endpoint: http://127.0.0.1:54566 (override the host port with JWP_SQS_PORT and
point tests at it with JWP_SQS_ENDPOINT). Region us-east-1, any credentials.

| Queue | Role | Visibility | Long poll | Retention | Redrive |
|---|---|---|---|---|---|
| wager-transactions.fifo | commands | 30s | 20s | 4 days | DLQ after 5 receives |
| wager-transactions-dlq.fifo | commands DLQ | default | default | 14 days | none |
| wager-events.fifo | outbox events | 30s | 20s | 4 days | DLQ after 5 receives |
| wager-events-dlq.fifo | events DLQ | default | default | 14 days | none |

All queues are FIFO with ContentBasedDeduplication=false: producers set
MessageGroupId=walletId and MessageDeduplicationId=messageId (commands) or
eventId (events). maxReceiveCount 5 and visibility 30s are the values chosen
in ARCHITECTURE.md (JWP-ADR-007); the inbox, resilience and recovery suites confirm them.

`bun run test:sqs` fails (it never skips) when SQS is unreachable. Behaviour tests
create throwaway copies of the contract queues (`test-<id>-...`) and delete them,
so they do not consume messages from a running application.
