## 0.8.0

- The dead-letter list is now inspectable and manageable, which the README has
  claimed since the start without an API to back it. A dead-lettered task is
  stored with the error that gave up on it, not as a bare envelope, and
  `QueueClient` gains three methods: `deadLetters({limit})` returns the entries
  as `DeadLetter` (task, queue, error text, attempt count, dead-at time),
  `replayDeadLetter(id)` re-enqueues one for a fresh set of attempts, and
  `purgeDeadLetters()` clears them. A replay removes the entry and re-enqueues
  in one atomic step, so it can't be dropped without landing back on its queue
  or double-enqueued by two concurrent callers.
- Additive and backward-compatible: `DeadLetter.decode` reads a pre-0.8.0 entry
  (a bare envelope with no stored error) too, so an existing dead-letter list
  still reads after the upgrade.

## 0.7.1

- Make task ids unique across producer processes. The id was
  `identityHashCode(this)` plus a per-process counter, and both reset or repeat
  when a new process starts, so two producers could mint the same id. Since the
  id is the deduplication key an idempotent handler writes against, a collision
  meant one task was silently taken for a repeat of another and skipped: data
  loss in the exact place the package tells you to rely on the id. Measured: of
  100,000 fresh clients, several produced an identical first id. Ids now pair a
  per-client 96-bit secure-random prefix with the counter, so the prefix
  separates processes and the counter orders within one. No new dependency;
  no API change. A test enqueues from 200 clients and asserts every id is
  distinct.

## 0.7.0

- **Breaking:** `onError` and `onDeadLetter` now receive the run's
  `TaskContext` as their second argument, and the `attempt`/`willRetry` named
  parameters are gone: `context.attempt` and `!context.isLastAttempt` carry the
  same information, so there is one shape to learn instead of two.
  The reason is `context.id`. The observers had the task type and the error but
  no id, so a failure log or a dead-letter alert could name a *kind* of job and
  never the job itself. That is the first thing anyone wants during triage, and
  it was the one place 0.6.0's `TaskContext` had not reached: the handler got it,
  the observability seam did not.
  Migration: `(task, error, stack, {attempt, willRetry})` becomes
  `(task, context, error, stack)`, reading `context.attempt` and
  `context.isLastAttempt`.

## 0.6.1

- Install instructions now say `pub add` instead of pinning a version. The
  pinned number was stale by several releases and would have been stale again
  after the next one: the README ships frozen in the archive, so a hand-edited
  version line is wrong the moment anything is published. This one cannot go
  out of date.

## 0.6.0

- **Breaking:** a handler now takes the run's context as a second argument,
  `(Task task, TaskContext context)`. Existing handlers migrate by adding the
  parameter: `(task) async { ... }` becomes `(task, _) async { ... }`.
- Handlers can see which run they are in. `TaskContext` carries the task's `id`,
  the `queue` it came from, the 1-based `attempt`, `maxAttempts` (the first run
  plus its retries) and `isLastAttempt`, which is true on exactly the run whose
  failure dead-letters the task.
  The `id` is the point. Delivery is at-least-once, so the package has always
  asked handlers to be idempotent, but until now it gave them nothing stable to
  deduplicate on: the id lived in the envelope and never reached the handler,
  which left callers inventing their own key inside the payload. It is assigned
  at enqueue and is unchanged across retries and crash recoveries, so it is the
  value to record with the effect. `attempt` and `isLastAttempt` cover the other
  half, taking a slower or safer path on a late try and getting one last chance
  to record something before the task is given up on.
- Examples now demonstrate the delivery guarantees instead of describing them.
  `example/crash_recovery.dart` kills a worker mid-task in a child process and
  shows the task completing after recovery. `example/at_least_once.dart` stages
  the same crash twice, with a careless handler and with one keyed off
  `context.id`, and counts the effect: applied twice, then once. There is also
  an `example/README.md` covering the worker-id contract, the idempotency
  pattern, watching the dead-letter list, and where this queue does not fit.

## 0.5.0

- Crash-safe at-least-once delivery. The worker now claims a task by atomically
  moving it (`LMOVE`) onto a per-worker in-flight list and only removes it once
  the task is done, retried, or dead-lettered. A worker that dies mid-task
  leaves the envelope on its in-flight list and requeues it on its next `run`,
  so a crash, OOM kill, or lost node no longer loses the task in progress.
  Previously the worker popped with `BRPOP`, so a task being handled when the
  process died was gone. Delivery is at-least-once: a task can run again after a
  crash, so **handlers must be idempotent**.
- New `workerId` on `Worker.connect` (default: the host name) names the
  in-flight list a restarted worker recovers from. Set it to something stable
  across restarts (a pod or service name); two workers must never share one. See
  the README's "Recovery and worker ids" for the one case this doesn't cover on
  its own (a worker that never restarts under the same id).
- Real weighted fair scheduling. The worker draws each queue's turn in
  proportion to its weight with a rotating cursor over the weighted order, so
  `{'critical': 6, 'default': 3, 'low': 1}` is served roughly 6:3:1 under load
  and, unlike strict priority, a flood of critical jobs can't fully starve
  `low`. The previous `BRPOP` over a repeated key list was really strict
  priority; the weights only set the order.
- Docs: the flow and state diagrams now show the in-flight list and the
  crash-recovery path.

## 0.4.1

- Docs: replace the two README mermaid diagrams with rendered PNGs. pub.dev does
  not render mermaid, so the diagrams showed as raw source there; they now display
  as images on both pub.dev and GitHub.

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
