# Package engineering rules: redis_task_queue

Rules-Version: redis_task_queue/556b5391954473b84585f4aaa65616e39930a44568ab2010306f5962789a4b72
Core-Version: 1
Core-Digest: 1825fa7ff346dca23e65b1b3bf9b2e3e06959f1414bae9952d596d2f62f09b8f
Survey-Digest: f90f45c8a172068c3ed3b9488ba5a7cb4e58efa93c380d2d9a70b399349ec35e
Evidence-Revision: a19ad4b
Verified-Revision: unverified

Read CONTRIBUTING.md and docs/engineering/debt.json before editing.

## Current architecture
HEAD a19ad4b (2026-08-29), v1.1.4, sdk ^3.5.0 (short formatter style), one runtime dependency redis ^4.0.0. A small two-sided queue library: the producer and management front end `QueueClient` (enqueue, dead-letter listing/replay/purge, stats) and the consumer runtime `Worker` (weighted claim, due-mover, retry/backoff, dead-letter, orphan recovery, graceful stop). Both talk to Redis over `package:redis` `Command.send_object` through a private `_send` that reconnects once. Every transition that touches more than one key is atomic: LMOVE/BLMOVE or a `static const` Lua script (EVAL) next to its caller. The wire format (`Envelope` JSON) and the key layout (`Keys`) are internal single-source modules. `package:redis`/`dart:io` errors are converted into a sealed `TaskQueueException` hierarchy by `wrapRedisError`. Delivery is at-least-once; the idempotency key is `TaskContext.id`. `lib/src` is flat, 7 files, ~1,320 lines: two roles plus support modules; scale must not force an extra layer.

## Layers and responsibilities
- lib/redis_task_queue.dart: Exports 8 types + 3 typedefs through an explicit `show` list (lines 8-17); internal helpers stay hidden.
- lib/src/exceptions.dart: sealed `TaskQueueException` + final `TaskQueueConnectionException`/`TaskQueueServerException`; `wrapRedisError(error, operation)` converter (not exported).
- lib/src/task.dart: `Task` (type+payload), `TaskContext` (id/attempt/maxAttempts), `DeadLetter` (triage record, reads the old format), internal `Envelope` (encode/decode/encodeDead).
- lib/src/keys.dart: `Keys`: pending/delayed/deadLetter/inFlight key builders, prefix namespace (internal).
- lib/src/schedule.dart: `dueAtMillis` (now injected as a parameter, pure function, internal).
- lib/src/stats.dart: `QueueStats` instantaneous depth snapshot.
- lib/src/client.dart: `QueueClient`: connect, enqueue (immediate/scheduled), deadLetters, replayDeadLetter (Lua), purgeDeadLetters, stats (SCAN), close; cross-process unique id generation.
- lib/src/worker.dart: `Worker`: connect (workerId uniqueness guard), handle, run loop, weighted claim, promote/retry/dead/recover Lua scripts, backoff, observer isolation (`_notify`), stop/close.

## Public API and dependency direction
Public: `QueueClient`, `Worker`, `Task`, `TaskContext`, `DeadLetter`, `QueueStats`, `TaskQueueException`, `TaskQueueConnectionException`, `TaskQueueServerException`, typedef `TaskHandler`, `TaskErrorCallback`, `TaskDeadLetterCallback` (lib/redis_task_queue.dart:8-17). Constructors are private (`QueueClient._`, `Worker._`); entry points are `static Future<X> connect({...})`. Still internal: `Envelope`, `Keys`, `dueAtMillis`, `wrapRedisError`. Leak: `DeadLetter.decode` (task.dart:168) and `Task.toJson/fromJson` (task.dart:17-22) are exposed as public members.

client.dart → {exceptions, keys, schedule, stats, task, package:redis, dart:math}; worker.dart → {exceptions, keys, task, package:redis, dart:async, dart:math}; exceptions.dart → package:redis (only for the RedisError/RedisRuntimeError mapping); task.dart → dart:convert; keys/schedule/stats → no internal module (leaves). client and worker do not import each other (only a dartdoc attribution). No cycles. `package:redis` is limited to 3 files (exceptions, client, worker). Tests use the barrel + package:redis (raw inspection); two pure tests import `src/`.

## Error, state and platform contracts
- Private constructor + `static Future<T> connect()`; connection errors are wrapped with `Error.throwWithStackTrace(wrapRedisError(e, 'connect'), s)` (client.dart:42-54, worker.dart:219-267).
- `_send` reconnects once: `on RedisError` means a server rejection, no retry; everything else counts as a dead socket, redial + a single repeat, then the error is wrapped (client.dart:75-93, worker.dart:287-305; two copies).
- Atomic multi-key transitions: `static const _xScript` Lua + KEYS/ARGV dartdoc (worker.dart:129-182, client.dart:187-198) and LMOVE/BLMOVE claim (worker.dart:428-444).
- Sealed error hierarchy + converter; `cause` is kept for logging but sits outside the contract (exceptions.dart:16-31, 52-69).
- Argument errors use `ArgumentError.value(v, 'ad', 'must ...')` (client.dart:139, worker.dart:232); lifecycle misuse raises `StateError` (worker.dart:239-244, 332-336); mutually exclusive parameters are checked by assert only (schedule.dart:11-14).
- Observer isolation: callback errors are swallowed inside `_notify` (worker.dart:592-603).
- Time injection exists only in the pure function (schedule.dart `now`); the runtime reads `DateTime.now()` directly (client.dart:126, worker.dart:479, 541, 560).
- Streaming/cancellation: no CancelToken; `stop()` is cooperative; with `Completer _drained` the blocking window is at most ~1 s plus any task in flight (worker.dart:120-122, 605-626).
- Single exception of isolate-wide static state: `_liveIds` (worker.dart:85-90), released on close.
- Backward-compatible wire reading: the pre-0.8.0 dead-letter format is tolerated (task.dart:166-196).
- No FFI; no platform checks; lib does not import `dart:io` (the network boundary is package:redis).
- Long dartdoc that explains the 'why': every public member names the failure it prevents (exceptions.dart:3-15, client.dart:59-74, worker.dart:196-218).

## Package rules
### redis_task_queue/RTQ-1 [MUST]
Build every Redis key through `Keys` in lib/src/keys.dart. Code outside keys.dart must not spell key fragments such as `:queue:`, `:inflight:` or `:delayed`; add a method to `Keys` instead.
Reason: keys.dart:1 keeps the key scheme in one place 'so client and worker do not diverge'. Today client.dart:235 and 249-250 violate this (debt RTQ-B1); the rule preserves the existing intent.
Evidence: lib/src/keys.dart:1-31; ihlal lib/src/client.dart:235, 249-250
Evidence role: both
Existing violation: redis_task_queue-D001

### redis_task_queue/RTQ-2 [MUST]
A change that moves a task between keys (pending, in-flight, delayed, dead-letter) happens in one atomic Redis operation: `LMOVE`/`BLMOVE` or one `EVAL` script kept as a `static const _<name>Script` next to its caller, with a dartdoc that lists KEYS and ARGV. Never split such a move into two commands.
Reason: A process that crashes between two commands loses or duplicates a task; the package's reliability promise rests on this atomicity. All 5 current transitions follow this form.
Evidence: lib/src/worker.dart:129-182, 399-444, 536-551, 556-571; lib/src/client.dart:154-198; violation lib/src/client.dart:202-206 (debt redis_task_queue-D003)
Evidence role: both
Existing violation: redis_task_queue-D003

### redis_task_queue/RTQ-3 [MUST]
Send every Redis command through the owning class's `_send`, which reconnects once and converts failures with `wrapRedisError(error, operation)` and `Error.throwWithStackTrace`. A public method throws only `TaskQueueException` subtypes, `ArgumentError` for a bad argument and `StateError` for lifecycle misuse; no type from `package:redis` or `dart:io` reaches a caller.
Reason: package:redis errors do not implement Exception and have inconsistent types (exceptions.dart:5-12); users catch a single type. All call sites use `_send` today.
Evidence: lib/src/exceptions.dart:3-15, 52-69; lib/src/client.dart:48-52, 75-93, 139; lib/src/worker.dart:231-252, 287-305, 332-336
Evidence role: current-pattern
Existing violation: none

### redis_task_queue/RTQ-4 [MUST]
Treat `TaskQueueException` as a closed set. Adding a subtype breaks exhaustive switches in user code, which makes it a major-version change with a CHANGELOG entry.
Reason: The class is deliberately sealed and the dartdoc promises that the switch stays exhaustive.
Evidence: lib/src/exceptions.dart:14-16, 39, 48
Evidence role: current-pattern
Existing violation: none

### redis_task_queue/RTQ-5 [MUST]
Keep stored data readable across versions. A change to the envelope or dead-letter JSON keeps decoding the previous shape, and test/wire_format_test.dart gains a case for both the old and the new shape.
Reason: Records in the queue and the dead-letter list must survive a version upgrade; the package still reads the pre-0.8.0 format today and pins it with a test.
Evidence: lib/src/task.dart:152-196; test/wire_format_test.dart:93-175
Evidence role: current-pattern
Existing violation: none

### redis_task_queue/RTQ-6 [MUST]
The public API is the `show` list in lib/redis_task_queue.dart. `Envelope`, `Keys`, `dueAtMillis` and `wrapRedisError` stay unexported, and a new public type enters that list on purpose.
Reason: Export discipline is done today with explicit `show`; internal wire and key types stay hidden.
Evidence: lib/redis_task_queue.dart:8-17
Evidence role: current-pattern
Existing violation: redis_task_queue-D007

### redis_task_queue/RTQ-7 [MUST]
Test Redis-dependent behavior against a real Redis through `requireRedis()` in test/_redis.dart. Logic that does not need Redis (time math, wire format) lives in a pure function or class and gets a unit test that runs without Redis.
Reason: If a test resolves both sides from the same store there is no boundary (shared rules §3); here the boundary is real Redis. The pure parts were pulled into a separate module and are tested without Redis.
Evidence: test/_redis.dart:19-45; lib/src/schedule.dart:1-17 + test/schedule_test.dart:1; test/wire_format_test.dart:3
Evidence role: current-pattern
Existing violation: none

### redis_task_queue/RTQ-8 [MUST_NOT]
Do not add a runtime dependency besides `package:redis`. Write a small helper here instead, as `_newId` does for task IDs.
Reason: The code made this choice explicitly (the UUID library is deliberately absent) and the 1.1.4 CHANGELOG says 'No new dependency'.
Evidence: pubspec.yaml dependencies (only redis ^4.0.0); lib/src/client.dart:285-302; CHANGELOG.md 1.1.4
Evidence role: current-pattern
Existing violation: none

### redis_task_queue/RTQ-9 [MUST]
Keep task IDs stable. An ID is minted once in `enqueue` and reused by every retry, recovery and replay, because handlers deduplicate on `TaskContext.id`.
Reason: Id stability is the only mechanism that turns at-least-once delivery into an exactly-once effect; replay keeps the old id while it builds a new envelope.
Evidence: lib/src/task.dart:28-47; lib/src/client.dart:112, 164-172; lib/src/worker.dart:520-525
Evidence role: current-pattern
Existing violation: none

### redis_task_queue/RTQ-10 [MUST]
Call observer callbacks (`onError`, `onDeadLetter`) only through `_notify`. A throwing observer must never abort retry or dead-letter handling.
Reason: The typedef dartdoc promises that observer errors are swallowed; the processing path is isolated from the observer.
Evidence: lib/src/worker.dart:29-31, 509, 552, 592-603
Evidence role: current-pattern
Existing violation: none

### redis_task_queue/RTQ-11 [MUST_NOT]
Do not add static or top-level mutable state. `Worker._liveIds` is the single documented exception: an isolate-local guard against duplicate worker IDs, released in `close`.
Reason: Global mutable state breaks tests and multiple instances; the single current exception is documented and tied to the life cycle.
Evidence: lib/src/worker.dart:85-90, 237-245, 640
Evidence role: current-pattern
Existing violation: none

### redis_task_queue/RTQ-12 [SHOULD]
Give each operational number (claim window, promote batch, SCAN COUNT, backoff defaults) either a parameter or a one-sentence reason next to it.
Reason: Package adaptation of the shared J5 P-AYAR / D1 KONFIG-LITERAL lens. `_promoteBatch` and the 1 s claim window are justified; SCAN COUNT '100' is not.
Evidence: lib/src/worker.dart:124-127, 404-412, 443; lib/src/client.dart:275
Evidence role: both
Existing violation: none

### redis_task_queue/RTQ-13 [SHOULD]
Write typed catch clauses: `on RedisError`, `on FormatException`, or `on Object catch` with a comment that names why everything is caught.
Reason: Shared J10 P-CATCH-TYPED / D11. The code already uses typed catches (client.dart:50, 78) but there are seven bare catches (debt RTQ-B5).
Evidence: lib/src/client.dart:50, 78, 82; lib/src/worker.dart:249, 290, 294, 365, 379, 462, 508, 600
Evidence role: both
Existing violation: redis_task_queue-D005

### redis_task_queue/RTQ-14 [SHOULD]
Public dartdoc states the contract a caller relies on (at-least-once delivery, reconnect-once, idempotency key, stop and close order) and the failure it prevents.
Reason: The package's dominant documentation pattern; contracts (data loss, double execution) are read from dartdoc rather than from code.
Evidence: lib/src/exceptions.dart:3-15; lib/src/client.dart:59-74; lib/src/worker.dart:196-218, 605-635
Evidence role: current-pattern
Existing violation: none

## Required verification
- Working directory: repository root; command: dart pub get; conditions: ci.yml job test; evidence: .github/workflows/ci.yml:39.
- Working directory: repository root; command: dart analyze --fatal-infos; conditions: ci.yml job test; evidence: .github/workflows/ci.yml:41.
- Working directory: repository root; command: dart test; conditions: ci.yml job test; evidence: .github/workflows/ci.yml:43.
Not verified by the survey:
- I did not run `dart analyze` and `dart test`: read-only (they write .dart_tool) and the integration tests need a live Redis. RTQ-B3 and RTQ-B4 behaviors were derived by reading.
- I could not measure how many findings an extended analysis_options would produce.
- example/*.dart (7 files, 1,096 lines) content was not read; only the file list.
- README.md and AGENTS.md content was not read; only the AGENTS.md headings: '1. What it is', '2. Shortest usage', '3. Contracts that lose data', '4. Easy mistakes', '5. Where things live' (117 lines).
- GitHub Actions latest run result and pub.dev score (network not permitted).
- Whether the shared detectors (kod-kapisi.py D1-D17) fire on this package: per dart-kod-kurallari.md:130-133 the filter is `pkgs/dart_mcp/lib/**.dart`; the script was not on the reading list, I did not open it.

## Existing debt
The complete register is docs/engineering/debt.json.
- redis_task_queue-D001 | small | lib/src/client.dart:235, 249-250 (↔ lib/src/keys.dart:1-31) | duplicated logic / single-source violation
  Fix: Add `inFlightPattern()`, `queuePattern()` and `queueOf(String key)`, which extracts the queue name from a key, to keys.dart; have the client use them; write a pure keys test without Redis.
  Closure: client.dart builds every key through the new Keys helpers and spells no raw fragment such as :queue: or :inflight:. A pure keys test that runs without Redis covers the helpers and passes.
- redis_task_queue-D002 | medium | lib/src/client.dart:56-57, 75-93 ↔ lib/src/worker.dart:269-305 | duplicated logic
  Fix: A small class that holds host/port/Command in a non-exported lib/src/connection.dart (send, reconnect, close); QueueClient and Worker delegate to it. test/connection_drop_test.dart already runs both paths.
  Closure: QueueClient and Worker delegate dialing and sending to one non-exported connection.dart class. test/connection_drop_test.dart still passes over both paths.
- redis_task_queue-D003 | small | lib/src/client.dart:202-206 | atomicity / correctness
  Fix: A single EVAL script (`local n = redis.call('LLEN', KEYS[1]) redis.call('DEL', KEYS[1]) return n`) as `static const _purgeScript`; add a count test to dead_letter_admin_test.
  Closure: purgeDeadLetters deletes and counts the list in one atomic _purgeScript EVAL. The new count test in dead_letter_admin_test passes.
- redis_task_queue-D004 | medium | lib/src/worker.dart:365-381, 462-464, 494 | mixed error classes (poison message)
  Fix: Catch decode separately in `_process` with `on FormatException`/`on TypeError`, move the raw record to dead-letter through `_deadScript` with the error text; narrow the loop catch to `on TaskQueueException`; add an integration test that LPUSHes a corrupt record.
  Closure: A corrupt record LPUSHed to the in-flight list moves to dead-letter through _deadScript with the decode error text and no longer inflates stats().inFlight. The new integration test that LPUSHes a corrupt record passes.
- redis_task_queue-D005 | small | lib/src/client.dart:82; lib/src/worker.dart:294, 365, 379, 462, 508, 600 | untyped catch (J10/D11)
  Fix: `on Object catch` plus the existing rationale comment; narrow 462 to `on FormatException` and 365 via RTQ-B4.
  Closure: No bare catch remains at the listed lines and each uses a typed on clause or on Object with a rationale comment. The package tests still pass.
- redis_task_queue-D006 | small | lib/src/task.dart:89-95 ↔ 113-123 | duplicated logic
  Fix: A private `Map<String, Object?> _toJson()`; both use it. wire_format_test already pins the output.
  Closure: encode and encodeDead build their JSON map through one private _toJson. wire_format_test still passes.
- redis_task_queue-D007 | medium | lib/src/task.dart:166-168 (+ lib/redis_task_queue.dart:14) | leaking public API
  Fix: Now: a non-API note in the dartdoc + CHANGELOG. Next major: move it to a function inside src. `package:meta` @internal would add a dependency and is rejected because of RTQ-8.
  Closure: The dartdoc and the CHANGELOG mark DeadLetter.decode as not part of the API. The next major version moves it to a function inside src that the show list does not export.
- redis_task_queue-D008 | small | lib/src/worker.dart:189, 224-227, 313-321 | documented range not validated
  Fix: `ArgumentError.value` checks at the start of `Worker.connect` + tests.
  Closure: Worker.connect throws ArgumentError.value for backoffJitter outside 0..1, for weights of zero or less and for backoffBase above backoffCap. The new argument tests pass.
- redis_task_queue-D009 | small | .github/workflows/ci.yml:39-43 | CI gap
  Fix: Add a format step before analyze.
  Closure: ci.yml runs dart format --output=none --set-exit-if-changed before the analyze step. The format step passes on a clean checkout.
- redis_task_queue-D010 | medium | analysis_options.yaml:1; lib/src/client.dart:283; lib/src/task.dart:9, 17, 19, 143 | lint / documentation debt
  Fix: Enable the resilience set and close the findings in the same change.
  Closure: analysis_options.yaml enables the strict lint set and dart analyze reports no findings. Every public member named in the item has dartdoc and the task.dart reference resolves to [QueueClient.enqueue].
