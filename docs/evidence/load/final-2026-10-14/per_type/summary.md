# Load run 2026-10-14T17:11:04.681Z

Commit 96799194a2ce4a49d24294108eea983ea80b56ea (0 dirty files), 16 CPUs, Bun 1.3.14, PostgreSQL 17.6, 3 HTTP replicas + 3 worker processes.

| Scenario | VUs | Requests | s | req/s | p50 ms | p95 ms | p99 ms | max ms | errors | outbox lag p50/p95/p99 ms | outbox drain after load ms | outbox ev/s | CPU cores replicas/pg/sqs/gen |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| distributed | 32 | 11696 | 20.04 | 583.5 | 50.3 | 104.9 | 142.4 | 225.2 | 0% | 19362 / 28368 / 29070.6 | 29326 | 435.5 | 4.76 / 2.31 / 1 / 0.47 |
| hot-wallet | 32 | 2916 | 20.26 | 144 | 197.7 | 420.6 | 726.9 | 1081.3 | 0% | 18879.5 / 28456 / 28696.7 | 27974 | 109.5 | 2.03 / 0.85 / 0.61 / 0.49 |
| hot-drain | 32 | 500 | 2.07 | 242 | 123.3 | 174.3 | 203 | 213.8 | 0% | 1141.5 / 1567 / 1594 | 754 | 166.1 | 2.23 / 0.63 / 0.77 / 0.54 |

SQS: 1000/1000 commands in 9.16s -> 109.1 msg/s; end-to-end p50/p95/p99 4348/8542/8894 ms; outbox lag p95 657.1 ms; CPU cores replicas/pg/sqs/gen 2.18 / 0.7 / 0.99 / 0.46.

Downstream subscriber: 33349 deliveries, 33349/33349 distinct eventIds, 0 duplicates, 0 missing.

Invariants: PASSED

- balanceEqualsLedgerSum: ok
- balanceEqualsLastLedgerEntry: ok
- noNegativeBalance: ok
- noDuplicateLedgerEntry: ok
- everyProcessedHasLedger: ok
- noLedgerForUnprocessed: ok
- lossZeroWithoutReferenceOrLedger: ok
- loadReversalsMatchConfirmedBet: ok
- noNonTerminalLeft: ok
- outboxFullyPublished: ok
- reconciliationEndpointConsistent: ok
- hotDrainExactlyBalance: ok
- noDeadLetters: ok
- commandQueueDrained: ok
- httpMixedKindsProcessed: ok
- downstreamNoDuplicateEventId: ok
- downstreamReceivedEveryEvent: ok
