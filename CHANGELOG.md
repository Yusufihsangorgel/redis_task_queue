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
