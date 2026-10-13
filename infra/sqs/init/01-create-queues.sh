#!/usr/bin/env bash
# LocalStack ready hook: creates the FIFO topology declared in
# contracts/messaging/topology.json. tests/integration/sqs checks that both match.
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"
ATTRS="$(mktemp)"
trap 'rm -f "$ATTRS"' EXIT

create_dlq() {
  cat > "$ATTRS" <<JSON
{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}
JSON
  awslocal sqs create-queue --queue-name "$1" --attributes "file://$ATTRS" >/dev/null
}

create_source() {
  local dlq_arn="arn:aws:sqs:${REGION}:${ACCOUNT}:$2"
  cat > "$ATTRS" <<JSON
{
  "FifoQueue": "true",
  "ContentBasedDeduplication": "false",
  "VisibilityTimeout": "30",
  "ReceiveMessageWaitTimeSeconds": "20",
  "MessageRetentionPeriod": "345600",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${dlq_arn}\",\"maxReceiveCount\":\"5\"}"
}
JSON
  awslocal sqs create-queue --queue-name "$1" --attributes "file://$ATTRS" >/dev/null
}

create_dlq wager-transactions-dlq.fifo
create_source wager-transactions.fifo wager-transactions-dlq.fifo
create_dlq wager-events-dlq.fifo
create_source wager-events.fifo wager-events-dlq.fifo

echo "jwp: SQS FIFO topology ready"
