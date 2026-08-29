# Examples

The first program is the shortest thing that works. Three more answer the
questions you should put to any queue before it carries work you care about,
and they answer them by running, not by asserting. `backend.dart` is how
the producer sits in an HTTP handler and the worker in its own process —
the wiring a dart_frog or shelf reader otherwise has to invent.
`no_redis_demo.dart` walks the handler contract with nothing listening.

Every program that talks to Redis looks on `localhost:6379`. If you do not
have one:

```
docker run --rm -p 6379:6379 redis:7
```

That container keeps nothing when it stops. The test suite deliberately uses
6399 instead, so `dart test` cannot touch a Redis you are already using. The
Redis-backed programs read `REDIS_PORT`, so one already running on 6399
serves them too: `REDIS_PORT=6399 dart run example/triage.dart`.

## Start here

```
dart run example/redis_task_queue_example.dart worker    # one terminal
dart run example/redis_task_queue_example.dart enqueue   # another
```

`redis_task_queue_example.dart` enqueues one task and processes it. Producers
use `QueueClient`, workers use `Worker`, and the two only agree on a type string
and a JSON payload, so the enqueuing side never has to import the handler's
code.

## Where does the client live in a dart_frog or shelf app?

```
dart run example/backend.dart serve     # one terminal
dart run example/backend.dart worker    # another
curl -X POST http://127.0.0.1:8080/jobs
```

Not in the handler. `QueueClient.connect` per request is a TCP handshake
every time, and a leaked Redis socket if the handler returns without
`close()`. The HTTP process constructs one client next to the server
(the same place dart_frog's `init` / middleware, or a shelf pipeline,
holds shared state) and the handler only calls `enqueue`. The worker is
a second executable, not a route.

`dart:io`'s `HttpServer` stands in so this package does not grow a
framework dependency. With no args the program starts both processes,
POSTs a job, then SIGTERMs the worker while the handler is still
running, which is what `await worker.stop()` is for: the in-flight task
finishes, then the process leaves. SIGKILL would have been crash
recovery instead. Real output:

```
two processes, the way this sits in a Dart HTTP backend

1. the worker is its own process, not a route
   [worker] worker running (SIGTERM to stop)

2. the HTTP process holds one QueueClient for its lifetime
   [serve] listening on http://127.0.0.1:54706

3. a request enqueues and returns; the handler does not do the work
   [serve] enqueued 319baeb8459ac2540a0b6226-1
   POST /jobs -> 202 319baeb8459ac2540a0b6226-1
   [worker] holding 319baeb8459ac2540a0b6226-1

4. SIGTERM arrives while the handler is still running
   SIGTERM sent to worker pid 49415
   [worker] stop(): waiting for the task in flight (SIGTERM)
   [worker] sent welcome email for user 42 (task 319baeb8459ac2540a0b6226-1)
   worker exited 0 after finishing the task

the in-flight task was not abandoned. SIGKILL would have been
crash recovery; this was a drain.
```

## Does a dying worker lose the task?

```
dart run example/crash_recovery.dart
```

This is the question, and the demo answers it by actually killing a worker.
A child process claims the task and exits mid-flight with no chance to unwind,
the way a process goes down under SIGKILL or an out-of-memory kill. Then a
worker starts again and the task finishes. Real output:

```
enqueued 39b79823-1 on "crash-demo"

1. a worker claims the task, then dies holding it
   [victim pid 97245] holding march-invoices, dying now
   the process is gone (exit 137) and the task never finished

2. the task is on no pending list, so nothing will poll it up,
   and no live worker holds it. It is parked on the in-flight list
   that belongs to "crash-demo-worker", waiting for that worker to return.

3. a worker starts again under the same id
   recovered and ran march-invoices

the task survived a worker that died mid-flight.
```

What makes that work: a worker does not copy a task off the pending list, it
moves it, in one Redis step, onto an in-flight list of its own. The task is
never on neither list and never on both. If the worker finishes, the envelope
is dropped from that list. If the worker dies, the envelope stays there, and
the next worker to start under the same id puts it back.

## The task ran twice. Now what?

```
dart run example/at_least_once.dart
```

Recovery has a consequence people meet in production rather than in the README.
A worker can finish the work and then die before recording that it finished. No
queue can close that gap, because the acknowledgement is a separate step from
the work and something can always happen in between. So the task comes back and
the work runs a second time.

`at_least_once.dart` stages that exact crash twice, once with a careless
handler and once with a handler keyed off the task id:

```
== a handler with no defence ==
   attempt 1: charged INV-4417
   attempt 1: charged INV-4417
   the task ran twice, and the invoice was charged 2 times

== a handler keyed off the task id ==
   attempt 1: charged INV-4417
   attempt 1: already charged, skipping
   the task ran twice, and the invoice was charged 1 time
```

Note that both runs are `attempt 1`. A crash does not spend a retry, because
nothing failed: the task is handed back in the state it was claimed in. The
attempt counter moves only when a handler throws.

## Some work never succeeds. Where does it go?

```
dart run example/triage.dart
```

To the dead-letter list, which is the one structure here with no worker attached
to it: what arrives stays until a person decides otherwise. `triage.dart` fails
three webhook deliveries for good and then does that deciding, which is three
calls — `deadLetters()` to see what died and why, `replayDeadLetter()` for the
one whose cause has been fixed, `purgeDeadLetters()` for the two that never can.
It prints the queue depth at every step. Real output:

```
three webhook deliveries go on the queue
   acme     cd2ba80eb0d7681a13423184-1
   globex   cd2ba80eb0d7681a13423184-2
   initech  cd2ba80eb0d7681a13423184-3
   [queued] pending 3 · delayed 0 · in flight 0 · dead 0

1. a worker runs them. Every attempt throws, so every one gives up
   gave up on acme after 2 attempts
   gave up on globex after 2 attempts
   gave up on initech after 2 attempts
   [gave up] pending 0 · delayed 0 · in flight 0 · dead 3

2. the dead-letter list is what is left. Read it before deciding
   initech  cd2ba80eb0d7681a13423184-3
      2 attempts, died 2026-08-08T13:29:06.305
      Bad state: no signing key for "initech"; refusing to POST https://initech.example/hooks/orders unsigned
   globex   cd2ba80eb0d7681a13423184-2
      2 attempts, died 2026-08-08T13:29:06.304
      Bad state: POST https://globex.example/hooks/orders -> 410 Gone
   acme     cd2ba80eb0d7681a13423184-1
      2 attempts, died 2026-08-08T13:29:06.300
      Bad state: POST https://acme.example/hooks/orders -> 410 Gone

3. two of those are 410 Gone: the endpoint is deleted, so replaying
   them is a round trip back to this list. The third is our own
   missing config. Fix that, then put the one task back.
   replayDeadLetter -> true
   [replayed] pending 1 · delayed 0 · in flight 0 · dead 2
   one call moved it: off the dead list and onto the queue inside a
   single Lua step. Doing it by hand is two commands with a gap in
   the middle, and a crash in that gap either loses the task or
   enqueues it twice.

4. a worker starts again, and this time the delivery goes through
   delivered to initech (signed with key-3)
   [delivered] pending 0 · delayed 0 · in flight 0 · dead 2

5. the two that can never succeed are cleared
   purgeDeadLetters -> 2 entries
   [purged] pending 0 · delayed 0 · in flight 0 · dead 0

what never succeeds is not lost. It is parked where a human can read it,
and putting one back is one call.
```

The order matters more than it looks. A replayed task starts its retry budget
again from zero, so replaying something whose cause is still there just walks it
back onto the list a few seconds later. `initech` only got through in step 4
because step 3 added the signing key it was missing first.

The four numbers on every line are one `stats()` call. It reads counters rather
than tasks, so it is cheap enough to sit behind a dashboard or an alert. Name the
queues you want when you call it:

```dart
final depth = await client.stats(queues: ['webhooks']);
if (depth.deadLetter > 0) alert('$depth');
```

With no argument `stats()` discovers queues by scanning for their keys, and Redis
drops a list as soon as it empties — so a queue that is merely quiet vanishes
from the report rather than reporting zero. A named queue always reports, which
is what a fixed dashboard needs. `deadLetter` and `inFlight` are totals across
the whole prefix either way: one dead-letter list is shared by every queue, and
in-flight is summed over every worker.

## Three things to settle before this carries real work

### The worker id is an identity, not a nonce

Recovery is scoped to one worker's own in-flight list, so a worker only ever
reclaims tasks it was holding itself, never a live peer's. That is what keeps
two workers from running the same task at once, and it is also the catch: a
worker only finds its orphans if it comes back under the same id.

```dart
// A pod name from the environment: stable across restarts, unique per worker.
final worker = await Worker.connect(
  workerId: Platform.environment['HOSTNAME'],
);
```

The default is the host name, which is right for a process pinned to a machine
and right for a Kubernetes StatefulSet. It is wrong for a random UUID generated
at startup: every restart would begin a fresh, empty in-flight list and leave
the previous one orphaned forever. Two workers must never share an id.

### Handlers have to be idempotent

Delivery is at-least-once, as the demo above shows. `TaskContext.id` is assigned
at enqueue and is identical on every attempt and every recovery, so it is the
value to write against:

```dart
worker.handle('invoice:charge', (task, context) async {
  await db.execute(
    // The id is a unique column, so the second delivery cannot insert a row.
    // The duplicate is not detected, it is made impossible.
    'INSERT INTO charges (task_id, invoice, cents) VALUES (?, ?, ?) '
    'ON CONFLICT (task_id) DO NOTHING',
    [context.id, task.payload['invoice'], task.payload['cents']],
  );
});
```

A separate "have I done this?" check before the work is the same idea with a
race left in it: the process can still die between the check and the effect.
Recording the id in the same transaction as the result is what closes it.

### Something has to say the list grew

The triage above began with somebody already looking at the dead-letter list. In
production nobody is looking, so the give-up has to announce itself. These two
callbacks are where it does:

```dart
final worker = await Worker.connect(
  onError: (task, context, error, stack) {
    log.warning('${task.type} ${context.id} failed on attempt '
        '${context.attempt} of ${context.maxAttempts}');
  },
  onDeadLetter: (task, context, error, stack) {
    alert('gave up on ${task.type}, task ${context.id}', error);
  },
);
```

Both observers get the same `TaskContext` the handler got, so `context.id` is
the id `enqueue` returned. That is what makes the alert actionable: without it a
page at 3am says a task type failed, and the job it is talking about cannot be
found.

`TaskContext.isLastAttempt` is the other half: it is true on exactly the run
whose failure gives up, which is the last chance to record a partial result or
attach the detail a human will want.

## Where this fits, and where it does not

Reach for it when work should leave the request path and must not be dropped:
sending mail, calling a third-party API that rate limits or times out,
generating a report or a thumbnail, running something at a chosen time.

Do not reach for it when you need exactly-once effects with no idempotency work
on your side, when you need a workflow engine with fan-out, dependencies or
compensation, or when you need workers in other languages, which would mean
reimplementing the envelope format and the Lua scripts.

One caveat worth stating plainly: durability here is Redis durability. Tasks
survive a worker crash by construction, but they survive a Redis crash only as
well as that Redis is configured to, so check your persistence settings before
relying on it for work that cannot be recreated.
