import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:redis/redis.dart';

import 'keys.dart';
import 'task.dart';

/// A handler processes one task type. Throwing signals failure, which triggers
/// a retry (up to the envelope's maxRetries); returning normally marks it done.
///
/// [context] carries what the worker knows about this particular run: the
/// task's stable id to deduplicate on, which attempt this is, and whether it is
/// the last one. Handlers that don't need any of it can ignore the parameter.
typedef TaskHandler = FutureOr<void> Function(Task task, TaskContext context);

/// Called each time a task handler throws, before the worker decides what to do
/// next.
///
/// [context] describes the run that failed, the same value the handler was
/// given. `context.id` is what makes the callback useful in production: it is
/// the id [QueueClient.enqueue] returned, so a failure log or a metric can be
/// correlated back to a specific job rather than only to a task type.
/// `context.attempt` is the 1-based attempt that just failed, and
/// `context.isLastAttempt` is true when this failure sends the task to the
/// dead-letter list rather than scheduling another retry.
///
/// This is the hook for logging failures or emitting metrics; without it a
/// handler exception is invisible. An exception thrown by the callback itself is
/// caught and ignored, so a faulty observer cannot take the worker down.
typedef TaskErrorCallback = void Function(
  Task task,
  TaskContext context,
  Object error,
  StackTrace stackTrace,
);

/// Called when a task has exhausted its retries and been moved to the
/// dead-letter list.
///
/// This is the terminal-failure signal, the right place to alert a human or
/// record that a job was given up on. [context] carries the task's id, so the
/// alert can name the job somebody will have to go and find; [error] and
/// [stackTrace] are from the task's last failed attempt. Like
/// [TaskErrorCallback], a throwing callback is caught and ignored.
typedef TaskDeadLetterCallback = void Function(
  Task task,
  TaskContext context,
  Object error,
  StackTrace stackTrace,
);

/// Consumes tasks from Redis and runs their handlers.
///
/// The worker drains queues by weight: with `{'critical': 6, 'default': 3,
/// 'low': 1}` it checks `critical` roughly six times as often as `low`, so a
/// flood of low-priority jobs can't starve important ones. On handler failure
/// it re-enqueues with an incremented attempt count, held back by an
/// exponential backoff (see [backoffBase]/[backoffCap]) in a per-queue delayed
/// sorted set; once attempts reach maxRetries the envelope goes to the
/// dead-letter list instead of looping forever. The same delayed set also
/// holds tasks scheduled for a future time; the poll loop's due-mover promotes
/// both once due.
final class Worker {
  Worker._(
    this._command,
    this._keys,
    this._queues,
    this._workerId,
    this._host,
    this._port, {
    required Duration backoffBase,
    required Duration backoffCap,
    required double backoffJitter,
    TaskErrorCallback? onError,
    TaskDeadLetterCallback? onDeadLetter,
  })  : _backoffBase = backoffBase,
        _backoffCap = backoffCap,
        _backoffJitter = backoffJitter,
        _onError = onError,
        _onDeadLetter = onDeadLetter;

  Command _command;
  final Keys _keys;
  final Map<String, int> _queues;
  final String _host;
  final int _port;

  /// Names this worker's in-flight list. A restarted worker with the same id
  /// reclaims the tasks it was mid-way through when it died; see [connect].
  final String _workerId;

  final _handlers = <String, TaskHandler>{};

  /// Notified on every handler failure; null if the caller wants no callback.
  final TaskErrorCallback? _onError;

  /// Notified when a task is dead-lettered; null if the caller wants no callback.
  final TaskDeadLetterCallback? _onDeadLetter;

  /// The first retry waits [_backoffBase]; each further retry doubles the wait
  /// up to [_backoffCap]. [_backoffJitter] adds a random positive fraction of
  /// that wait so a burst of simultaneous failures doesn't re-fire in lockstep.
  final Duration _backoffBase;
  final Duration _backoffCap;
  final double _backoffJitter;
  final _random = Random();

  var _running = false;

  /// Completes when the run loop exits, so [stop] can hand back a future that
  /// resolves once the worker has drained. Null before the first [run].
  Completer<void>? _drained;

  /// How many due tasks the mover promotes per queue per loop pass. Bounded so
  /// a huge backlog coming due at once can't block the loop in one giant move;
  /// the remainder is picked up on the next pass.
  static const _promoteBatch = 100;

  /// Promotes due members from a queue's delayed set onto its pending list.
  ///
  /// Runs entirely inside Redis so the whole `ZRANGEBYSCORE` + `ZREM` + `LPUSH`
  /// sequence is atomic: no other command interleaves, so a task can't be lost
  /// or duplicated. The `ZREM == 1` guard means that even if several workers
  /// run this concurrently, only the one that actually removes a member pushes
  /// it (Redis serialises scripts, so the others simply won't see it).
  ///
  /// KEYS[1] delayed zset, KEYS[2] pending list; ARGV[1] now-millis, ARGV[2]
  /// batch size. Returns how many it moved.
  static const _promoteScript = '''
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local moved = 0
for i = 1, #due do
  if redis.call('ZREM', KEYS[1], due[i]) == 1 then
    redis.call('LPUSH', KEYS[2], due[i])
    moved = moved + 1
  end
end
return moved
''';

  /// Removes the in-flight envelope and schedules its retry in one atomic step,
  /// so a crash can't drop the task from in-flight without the retry landing (or
  /// vice versa). KEYS[1] in-flight list, KEYS[2] delayed zset; ARGV[1] the
  /// exact in-flight envelope to remove, ARGV[2] the re-encoded envelope with
  /// the incremented attempt, ARGV[3] its due-time score.
  static const _retryScript = '''
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
return 1
''';

  /// Removes the in-flight envelope and dead-letters it in one atomic step.
  /// KEYS[1] in-flight list, KEYS[2] dead-letter list; ARGV[1] the in-flight
  /// envelope to remove, ARGV[2] the dead-letter entry to store (the envelope
  /// wrapped with the failure that gave up on it).
  static const _deadScript = '''
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[2])
return 1
''';

  /// Moves one orphaned envelope from the in-flight list back to the tail of its
  /// pending queue, where it is the next one picked up. The `LREM == 1` guard
  /// makes it a no-op if something already reclaimed it. KEYS[1] in-flight list,
  /// KEYS[2] the envelope's pending list; ARGV[1] the envelope.
  static const _recoverScript = '''
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 1 then
  redis.call('RPUSH', KEYS[2], ARGV[1])
  return 1
end
return 0
''';

  /// Connects a worker. [queues] maps queue name to weight; higher weight is
  /// drained more often.
  ///
  /// [backoffBase]/[backoffCap] bound the retry backoff: the first retry waits
  /// [backoffBase], each further retry doubles it up to [backoffCap].
  /// [backoffJitter] (0..1) is the fraction of that delay added at random.
  ///
  /// [onError] is called every time a handler throws, and [onDeadLetter] when a
  /// task is finally given up on; both default to null (no callback). They are
  /// the worker's observability seam: without them a handler exception is
  /// swallowed silently, which is rarely what a production queue wants.
  ///
  /// [workerId] names this worker's in-flight list. A task is claimed by
  /// atomically moving it there and removed once it finishes, so a worker that
  /// dies mid-task leaves the envelope on the in-flight list rather than losing
  /// it; on its next [run] a worker requeues everything left on its own
  /// in-flight list. For that recovery to fire, a restarted worker must reuse
  /// the same id, so it defaults to the host name and should be set to a value
  /// that is stable across restarts (a pod or service name). Two workers must
  /// never share an id. Delivery is therefore at-least-once: a task can run
  /// again after a crash, so handlers must be idempotent.
  static Future<Worker> connect({
    String host = 'localhost',
    int port = 6379,
    String prefix = 'rtq',
    Map<String, int> queues = const {'critical': 6, 'default': 3, 'low': 1},
    Duration backoffBase = const Duration(seconds: 1),
    Duration backoffCap = const Duration(seconds: 60),
    double backoffJitter = 0.1,
    String? workerId,
    TaskErrorCallback? onError,
    TaskDeadLetterCallback? onDeadLetter,
  }) async {
    final command = await _dial(host, port);
    return Worker._(
      command,
      Keys(prefix),
      queues,
      workerId ?? Platform.localHostname,
      host,
      port,
      backoffBase: backoffBase,
      backoffCap: backoffCap,
      backoffJitter: backoffJitter,
      onError: onError,
      onDeadLetter: onDeadLetter,
    );
  }

  static Future<Command> _dial(String host, int port) =>
      RedisConnection().connect(host, port);

  /// Opens a fresh connection and swaps it in, discarding whatever socket
  /// [_command] was wrapping. Used both by [_send]'s one-shot retry and by
  /// [run]'s poll loop after that retry has already failed once.
  Future<void> _reconnect() async {
    _command = await _dial(_host, _port);
  }

  /// Sends [command] to Redis, reconnecting and retrying once if it fails.
  ///
  /// Same rationale as [QueueClient._send]: the underlying `redis` package
  /// never redials a dead socket on its own, and its errors for that aren't a
  /// consistent type, so any failure here is treated as "the socket might be
  /// dead" rather than pattern-matched. A second failure after the retry means
  /// Redis is actually down, so it is rethrown; [run]'s poll loop is what
  /// catches that and keeps retrying instead of crashing the worker.
  Future<dynamic> _send(List<Object> command) async {
    try {
      return await _command.send_object(command);
    } catch (_) {
      await _reconnect();
      return await _command.send_object(command);
    }
  }

  /// Registers [handler] for tasks of [type]. A task with no registered handler
  /// is treated as a failure and retried, so a missing handler surfaces loudly.
  void handle(String type, TaskHandler handler) => _handlers[type] = handler;

  /// The order in which queues are polled, one full pass per [_weightedOrder]
  /// call — each queue appears as many times as its weight.
  List<String> _weightedOrder() {
    final order = <String>[];
    _queues.forEach((queue, weight) {
      for (var i = 0; i < weight; i++) {
        order.add(queue);
      }
    });
    return order;
  }

  /// Runs the poll loop until [stop] is called. First requeues anything a
  /// previous run of this worker left in flight, then, each iteration, promotes
  /// any delayed tasks that have come due and claims the next one to run.
  ///
  /// A dropped Redis connection (a restart, a failover, a proxy's idle
  /// timeout) does not end the loop: it reconnects and keeps polling, running
  /// recovery again first in case anything was in flight when the connection
  /// dropped. What ends the loop is [stop].
  Future<void> run() async {
    _running = true;
    _drained = Completer<void>();
    try {
      await _recoverOrphans();
      // The weighted order (each queue repeated by its weight) and the single
      // highest-weight queue, computed once. _claim rotates through the
      // weighted order so the queue that gets first look is drawn in proportion
      // to weight.
      final weighted = _weightedOrder();
      final topQueue = weighted.isEmpty
          ? ''
          : _queues.entries.reduce((a, b) => b.value > a.value ? b : a).key;
      while (_running) {
        try {
          // Promote due tasks (scheduled tasks and elapsed retry backoffs) before
          // claiming. Pairing this with the short (1s) blocking claim below keeps
          // the claim from starving the mover: it parks for at most a second,
          // then the loop cycles back here, so a due task waits no longer than
          // roughly that timeout past its scheduled time. No separate
          // timer/isolate needed.
          await _promoteDue();
          final claimed = await _claim(weighted, topQueue);
          // Nothing was ready within the blocking window; loop and promote again.
          if (claimed == null) continue;
          // A task claimed just before stop() still runs to completion: it has
          // already been moved onto the in-flight list, so finishing it is the
          // clean end, not leaving it for recovery.
          await _process(claimed);
        } catch (_) {
          // Every call above already went through _send, which reconnects and
          // retries once on its own; getting here means that retry failed too,
          // so the connection isn't just blipped, it's still down. Back off
          // before hammering a Redis that may not be back yet, then reconnect
          // and rerun recovery: a fresh socket doesn't invalidate anything
          // already sitting on the in-flight list on the Redis side, but the
          // worker should treat "just reconnected" like "just started" for
          // recovery purposes, the same as the very first line of this method.
          if (!_running) break;
          await Future<void>.delayed(_backoffBase);
          try {
            await _reconnect();
            await _recoverOrphans();
          } catch (_) {
            // Still down; the next pass through this loop retries again.
          }
        }
      }
    } finally {
      _drained?.complete();
    }
  }

  /// Where the next weighted sweep starts, advanced once per claim so that over
  /// a full cycle of the weighted order each queue leads as many sweeps as its
  /// weight — a weight-6 queue six, a weight-1 queue one. That's what makes the
  /// weighting real: a busy high-priority queue is served more often, but a
  /// low-priority queue still gets its share and is never starved.
  var _claimCursor = 0;

  /// Atomically takes the next task off a pending queue and onto this worker's
  /// in-flight list, returning its raw envelope, or null if nothing was ready.
  ///
  /// The task is moved, not copied: `LMOVE ... RIGHT LEFT` pops the tail of a
  /// pending list (the oldest task, matching the producer's head LPUSH) and
  /// pushes it onto the in-flight list in one step, so it is never off both
  /// lists at once. Each call sweeps the queues starting from a rotating cursor
  /// over the weighted order, so first look is given to each queue in proportion
  /// to its weight; the first queue with a task wins. Only when every queue is
  /// empty does it park in a one-second blocking claim on the top-weight queue,
  /// so an idle worker doesn't spin. A task enqueued to another queue while the
  /// worker is idle is picked up by the next sweep, at most about a second
  /// later; under load a sweep always hits first and adds no latency.
  Future<String?> _claim(List<String> weighted, String topQueue) async {
    final inFlight = _keys.inFlight(_workerId);
    if (weighted.isEmpty) {
      await Future<void>.delayed(const Duration(seconds: 1));
      return null;
    }
    // Rotate the weighted order by the cursor, then dedupe: the queue leading
    // this sweep rotates through the weighted list, so its lead-share matches
    // its weight.
    final n = weighted.length;
    final seen = <String>{};
    final order = <String>[];
    for (var i = 0; i < n; i++) {
      final queue = weighted[(_claimCursor + i) % n];
      if (seen.add(queue)) order.add(queue);
    }
    _claimCursor = (_claimCursor + 1) % n;

    for (final queue in order) {
      final raw = await _send(
        ['LMOVE', _keys.pending(queue), inFlight, 'RIGHT', 'LEFT'],
      );
      if (raw != null) return raw as String;
    }
    final raw = await _send(
      ['BLMOVE', _keys.pending(topQueue), inFlight, 'RIGHT', 'LEFT', '1'],
    );
    // A hit returns the moved envelope as a String; on timeout the client
    // returns `[null]`. Anything that isn't a String means nothing was claimed.
    return raw is String ? raw : null;
  }

  /// Requeues envelopes left on this worker's in-flight list by a previous run
  /// that died mid-task. Each goes back to the tail of its own pending queue, so
  /// it is the next task picked up rather than a lost one. Runs once, before the
  /// poll loop, when the list is the worker's alone to touch; a live peer never
  /// shares a [_workerId], so this can't steal work in progress elsewhere.
  Future<void> _recoverOrphans() async {
    final inFlight = _keys.inFlight(_workerId);
    final orphans = await _send(['LRANGE', inFlight, '0', '-1']) as List;
    for (final raw in orphans) {
      final Envelope env;
      try {
        env = Envelope.decode(raw as String);
      } catch (_) {
        continue; // Unparseable; leave it rather than crash recovery.
      }
      await _send([
        'EVAL',
        _recoverScript,
        '2',
        inFlight,
        _keys.pending(env.queue),
        raw,
      ]);
    }
  }

  /// Moves every due task, scheduled or backed-off, from each queue's delayed
  /// set back onto its pending list, via the atomic [_promoteScript].
  Future<void> _promoteDue() async {
    final now = DateTime.now().millisecondsSinceEpoch;
    for (final queue in _queues.keys) {
      await _send([
        'EVAL',
        _promoteScript,
        '2',
        _keys.delayed(queue),
        _keys.pending(queue),
        '$now',
        '$_promoteBatch',
      ]);
    }
  }

  Future<void> _process(String raw) async {
    final env = Envelope.decode(raw);
    final handler = _handlers[env.task.type];
    // `attempt` counts from 1 while the envelope's counts retries from 0, so
    // the run in progress is `attempt + 1`, and the total is the first run plus
    // its retries. Both match the arithmetic _retryOrDeadLetter uses to decide
    // when to give up, so isLastAttempt is true on exactly the run whose
    // failure dead-letters the task. The handler and the observers are given
    // the same value, so a log line and the task it describes agree.
    final context = _contextFor(env);
    try {
      if (handler == null) {
        throw StateError('no handler for task type "${env.task.type}"');
      }
      await handler(env.task, context);
    } catch (error, stackTrace) {
      _notify(() => _onError?.call(env.task, context, error, stackTrace));
      await _retryOrDeadLetter(raw, env, context, error, stackTrace);
      return;
    }
    // Done: drop the envelope from the in-flight list. If the worker dies before
    // this runs, recovery requeues the task on the next start and an idempotent
    // handler absorbs the repeat.
    await _send(
      ['LREM', _keys.inFlight(_workerId), '1', raw],
    );
  }

  /// The run in progress, as the handler and the observers both see it.
  TaskContext _contextFor(Envelope env) => TaskContext(
        id: env.id,
        queue: env.queue,
        attempt: env.attempt + 1,
        maxAttempts: env.maxRetries + 1,
      );

  Future<void> _retryOrDeadLetter(
    String raw,
    Envelope env,
    TaskContext context,
    Object error,
    StackTrace stackTrace,
  ) async {
    final inFlight = _keys.inFlight(_workerId);
    if (env.attempt >= env.maxRetries) {
      // Remove from in-flight and dead-letter in one atomic step so a crash
      // can't do one without the other. The stored entry carries the error, so
      // the dead-letter list is inspectable instead of holding a bare task.
      final deadEntry = env.encodeDead(
        error.toString(),
        DateTime.now().millisecondsSinceEpoch,
      );
      await _send(
        [
          'EVAL',
          _deadScript,
          '2',
          inFlight,
          _keys.deadLetter(),
          raw,
          deadEntry
        ],
      );
      _notify(() => _onDeadLetter?.call(env.task, context, error, stackTrace));
      return;
    }
    env.attempt++;
    // Hold the retry back with an exponential backoff instead of re-enqueuing
    // immediately: score it with the time it becomes due and drop it in the
    // queue's delayed set, atomically with removing it from the in-flight list.
    // The run loop's mover promotes it once due.
    final dueAt = DateTime.now().millisecondsSinceEpoch +
        _backoffFor(env.attempt).inMilliseconds;
    await _send([
      'EVAL',
      _retryScript,
      '2',
      inFlight,
      _keys.delayed(env.queue),
      raw, // ARGV[1]: the exact in-flight envelope to remove
      env.encode(), // ARGV[2]: re-encoded with the incremented attempt
      '$dueAt', // ARGV[3]: due-time score
    ]);
  }

  /// Backoff for the [retry]-th attempt (1-based: 1 is the first retry).
  ///
  /// `min(cap, base * 2^(retry-1))`, plus up to [_backoffJitter] of that as a
  /// random positive offset. Doubling stops once it reaches the cap, so a large
  /// retry count can't overflow.
  Duration _backoffFor(int retry) {
    final capMs = _backoffCap.inMilliseconds;
    var delayMs = _backoffBase.inMilliseconds;
    for (var i = 1; i < retry && delayMs < capMs; i++) {
      delayMs *= 2;
    }
    if (delayMs > capMs) delayMs = capMs;
    if (_backoffJitter > 0) {
      delayMs += (_random.nextDouble() * _backoffJitter * delayMs).round();
    }
    return Duration(milliseconds: delayMs);
  }

  /// Runs an observability callback, swallowing anything it throws.
  ///
  /// A bug in a caller's logging or metrics hook must never crash the worker or
  /// abort a task's retry/dead-letter handling, so the callback is fully
  /// isolated from the processing path.
  void _notify(void Function() callback) {
    try {
      callback();
    } catch (_) {
      // Intentionally ignored; an observer's failure is not the worker's.
    }
  }

  /// Stops claiming new tasks and returns a future that completes once the
  /// worker has drained: the task it was running, if any, has finished and the
  /// run loop has exited.
  ///
  /// This is the graceful-shutdown path. Awaiting it lets a task in progress
  /// complete rather than being abandoned to recovery, which is what you want
  /// on a rolling deploy or a SIGTERM:
  ///
  /// ```dart
  /// await worker.stop();
  /// await worker.close();
  /// ```
  ///
  /// The wait is bounded by the current task plus up to about a second, the
  /// blocking window of an in-progress claim. Calling it before [run], or
  /// after the loop has already exited, returns an already-completed future.
  Future<void> stop() {
    _running = false;
    final drained = _drained;
    if (drained == null || drained.isCompleted) return Future<void>.value();
    return drained.future;
  }

  Future<void> close() => _command.get_connection().close();
}
