## 0.4.0

- Add observability hooks to `Worker.connect`. `onError` fires on every handler
  failure, with the 1-based attempt number and whether a retry follows;
  `onDeadLetter` fires when a task is given up on after its retries. Both default
  to null and are isolated, so a throwing callback cannot take the worker down.
  Before this, a handler exception was swallowed silently, which is rarely what a
  production queue wants.
- Restore scheduled tasks (`enqueue` with `processAt` or `processIn`), which the
  0.3.1 release removed by accident. 0.3.1 was published as a docs-only change
  but also dropped the 0.3.0 scheduling feature; if you schedule tasks for a
  future time, move to 0.4.0 (or pin 0.3.0) rather than 0.3.1.

## 0.3.0

- Scheduled tasks. `enqueue` takes an optional `processAt` (an absolute time)
  or `processIn` (a delay from now), set at most one, to hold a task until a
  future time.
- No new machinery: a scheduled task is scored into the same per-queue delayed
  sorted set the 0.2.0 retry backoff uses, and the same atomic Lua due-mover
  promotes it once due. The worker is unchanged.
- The inherited bounds apply: a due task starts up to about a second past its
  due time (the mover runs once per poll-loop pass), and only while a worker
  polling that queue is running.
- `enqueue` without either parameter behaves exactly as in 0.2.0.

## 0.2.0

- Exponential backoff for retries. A failed task is no longer re-enqueued
  immediately: it goes into a per-queue delayed sorted set
  (`<prefix>:<queue>:delayed`) scored with the time it becomes due, and waits
  `min(cap, base * 2^(retry-1))` plus jitter before being retried.
- Configurable backoff on `Worker.connect`: `backoffBase` (default 1s),
  `backoffCap` (default 60s), and `backoffJitter` (default 0.1).
- The worker's poll loop now runs a due-mover each pass that promotes delayed
  tasks whose score has passed back onto their pending list. The move is a
  single atomic Redis Lua script (`ZRANGEBYSCORE` + `ZREM` + `LPUSH`), so a
  task can't be lost or duplicated, even with multiple workers.
- Dead-letter behaviour is unchanged; weighted-queue behaviour is unchanged.

## 0.1.0

- Initial release.
- Enqueue tasks from a producer, process them in a worker.
- Retries with a dead-letter list after maxRetries.
- Weighted queues to avoid starvation.
