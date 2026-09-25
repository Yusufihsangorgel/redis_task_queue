# AGENTS

Notes for working on this package, or for adding it to a Dart backend. Missed
contracts here drop or double work instead of throwing.

## 1. What it is

`redis_task_queue` is a Redis-backed task queue for server-side Dart: enqueue a
`Task` (type string + JSON map) with `QueueClient` and process it in a separate
`Worker` with retries, a dead-letter list, and weighted queues. Delivery is
at-least-once — a task held when the worker process dies is recovered on the
next `Worker.run` under the same `workerId`, not deleted.

It does not provide cron/recurring schedules, unique-task dedup, exactly-once
delivery, a web UI, workflows, or workers in other languages. One-shot
`processAt` / `processIn` exist; the dead-letter list is never drained for you.
Requires Dart 3.5+ (`sdk: ^3.5.0`) and a reachable Redis that supports
`LMOVE`/`BLMOVE` (this repo tests against `redis:7`). Default connect is
`localhost:6379`, key prefix `rtq`. Durability is Redis durability.

## 2. Shortest usage

Producer and worker from `example/redis_task_queue_example.dart`. They only
agree on the type string and payload; they are separate processes.

```dart
import 'package:redis_task_queue/redis_task_queue.dart';

final client = await QueueClient.connect();
await client.enqueue(
  Task('email:welcome', {'user_id': '42'}),
  queue: 'default',
  maxRetries: 5,
);
await client.close();
```

```dart
final worker = await Worker.connect(workerId: 'example-worker');
worker.handle('email:welcome', (task, context) async {
  // Key the effect on context.id so a redelivery is a no-op.
});
await worker.run(); // blocks until stop()
```

On SIGTERM: `await worker.stop(); await worker.close();`

## 3. Contracts that lose data

- **Handlers must be idempotent.** Delivery is at-least-once: the handler can
  finish, then the process die before the ack `LREM`. The stable redelivery key
  is `TaskContext.id` — assigned in `QueueClient.enqueue` (the returned value),
  identical on every retry and every crash recovery. Crash recovery does **not**
  increment `TaskContext.attempt`; only a throwing handler does. See
  `example/at_least_once.dart`.
- **Claim / release.** `Worker.run` claims via `_claim`: `LMOVE` (then a 1s
  `BLMOVE` if every queue is empty) `RIGHT LEFT` from pending onto
  `$prefix:inflight:$workerId`. The envelope leaves that list only on handler
  return (`LREM`), on retry (`_retryScript`), or on dead-letter (`_deadScript`).
  `Worker.stop` stops new claims and waits for the in-progress handler.
- **Process death.** The envelope stays on that in-flight list. The next
  `Worker.run` under the **same** `workerId` requeues it (`_recoverOrphans`). A
  new id never reads the old list — those tasks are stranded. Two
  `Worker.connect` calls with the same id in one isolate throw `StateError`;
  across processes it cannot be detected, and each requeues the other's
  in-flight work. `workerId` is required, must be stable across restarts, and
  unique per worker.
- **Close on shutdown.** `await worker.stop(); await worker.close();` and
  `QueueClient.close`. `Worker.close` is terminal (`run` afterwards throws
  `StateError`) and awaits `stop`. Skipping `close` leaves the Redis socket
  open and can keep the isolate alive.
- **Weighted pick.** `Worker.connect(queues:)` is name → weight (default
  `{critical: 6, default: 3, low: 1}`). `_weightedOrder` repeats each name by
  weight; `_claim` rotates `_claimCursor` so each queue **leads** as many
  sweeps as its weight, then `LMOVE`s in that order — first hit wins. Idle:
  1s `BLMOVE` on the highest-weight queue. The worker only polls keys in
  `queues`; a task on any other name sits pending forever.

## 4. Easy mistakes

- **Handler is not idempotent.** Symptom: a charge, email, or write happens
  twice after a crash (both runs can be `attempt == 1`). Fix: persist
  `TaskContext.id` with the effect in one transaction / unique constraint.
- **`QueueClient.connect` per request.** Symptom: a TCP handshake (and a leaked
  socket if `close` is skipped) on every enqueue. Fix: one client for the
  process lifetime; `enqueue` is a single `LPUSH` (or `ZADD` if scheduled).
- **No `stop` / `close`.** Symptom: a rolling deploy abandons the current task
  to recovery; the process may never exit. Fix: `await worker.stop(); await
  worker.close();` — stop drains the in-flight handler first.
- **Assume a failed task retries forever.** Symptom: work disappears from the
  worker and never runs again. Fix: default `maxRetries` is 5, so
  `TaskContext.maxAttempts` is 6 (`maxRetries + 1`); the next throw after that
  goes to the dead-letter list. Use `context.isLastAttempt` and `onDeadLetter`.
  Inspect with `QueueClient.deadLetters`, re-enqueue one with
  `replayDeadLetter`, drop with `purgeDeadLetters`. A missing `handle` is a
  failure, not a skip. `maxRetries: 0` dead-letters on the first throw.

## 5. Where things live

- `lib/redis_task_queue.dart` — public exports. Implementation: `lib/src/`
  (`client.dart`, `worker.dart`, `task.dart`, `exceptions.dart`, `stats.dart`).
- `example/` — enqueue/worker, HTTP-backend wiring, crash recovery, idempotency, dead-letter triage.
- `test/` — integration tests against a real Redis.

Tests talk to Redis on **6399** (not 6379):

```
docker run --rm -p 6399:6379 redis:7
dart test
```

Override with `REDIS_PORT`. No listener → one `StateError` with that docker
command.

Without Redis: `dart run example/no_redis_demo.dart` walks three attempts of
the `TaskContext` contract. Other examples need Redis on 6379 (or `REDIS_PORT`):
`dart run example/redis_task_queue_example.dart worker` then `... enqueue`.

## Contributing

Before changing this repository, read [CONTRIBUTING.md](CONTRIBUTING.md), [package engineering rules](docs/engineering/package.md), and the [debt register](docs/engineering/debt.json). These requirements apply to every contributor. The usage guidance above remains the consumer contract.
