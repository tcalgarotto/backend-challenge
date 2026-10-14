# Load run 2026-10-14T17:13:39.663Z

Commit 96799194a2ce4a49d24294108eea983ea80b56ea (0 dirty files), 16 CPUs, Bun 1.3.14, PostgreSQL 17.6, 3 HTTP replicas + 3 worker processes.

| Scenario | VUs | Requests | s | req/s | p50 ms | p95 ms | p99 ms | max ms | errors | outbox lag p50/p95/p99 ms | outbox drain after load ms | outbox ev/s | CPU cores replicas/pg/sqs/gen |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| distributed | 32 | 10897 | 20.06 | 543.3 | 51.6 | 129.5 | 183 | 349.3 | 0% | 20720 / 28629.3 / 29696.3 | 29879 | 402 | 4.73 / 2.27 / 0.98 / 0.46 |
| hot-wallet | 32 | 3351 | 20.18 | 166.1 | 166.8 | 358.4 | 407.4 | 510.5 | 0% | 18677.5 / 35987.1 / 36760.8 | 36102 | 107.6 | 2.14 / 0.94 / 0.66 / 0.52 |
| hot-drain | 32 | 500 | 2.46 | 202.9 | 148.2 | 242.1 | 307.6 | 347.8 | 0% | 1159 / 1710 / 1746 | 1257 | 145.4 | 2.24 / 0.72 / 0.73 / 0.54 |

SQS: 1000/1000 commands in 11.36s -> 88.1 msg/s; end-to-end p50/p95/p99 6186/10630/10971 ms; outbox lag p95 954 ms; CPU cores replicas/pg/sqs/gen 2.12 / 0.57 / 0.94 / 0.43.

Downstream subscriber: 32496 deliveries, 32496/32496 distinct eventIds, 0 duplicates, 0 missing.

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
