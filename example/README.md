# Examples

Three programs. The first is the shortest thing that works. The other two
answer the questions you should put to any queue before it carries work you
care about, and they answer them by running, not by asserting.

All three need a Redis reachable on `localhost:6379`.

## Start here

```
dart run example/redis_task_queue_example.dart worker    # one terminal
dart run example/redis_task_queue_example.dart enqueue   # another
```

`redis_task_queue_example.dart` enqueues one task and processes it. Producers
use `QueueClient`, workers use `Worker`, and the two only agree on a type string
and a JSON payload, so the enqueuing side never has to import the handler's
code.

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

### Retries end somewhere, and something has to watch where

A task that keeps throwing is retried with an exponential backoff until its
attempts run out, then it moves to the dead-letter list. Nothing drains that
list for you, and a queue nobody reads is an outage nobody hears about.

```dart
final worker = await Worker.connect(
  onError: (task, error, stack, {required attempt, required willRetry}) {
    log.warning('${task.type} failed on attempt $attempt, retry: $willRetry');
  },
  onDeadLetter: (task, error, stack) {
    alert('gave up on ${task.type}', error);
  },
);
```

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
