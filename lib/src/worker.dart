import 'dart:async';
import 'dart:math';

import 'package:redis/redis.dart';

import 'keys.dart';
import 'task.dart';

/// A handler processes one task type. Throwing signals failure, which triggers
/// a retry (up to the envelope's maxRetries); returning normally marks it done.
typedef TaskHandler = FutureOr<void> Function(Task task);

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
class Worker {
  Worker._(
    this._command,
    this._keys,
    this._queues, {
    required Duration backoffBase,
    required Duration backoffCap,
    required double backoffJitter,
  })  : _backoffBase = backoffBase,
        _backoffCap = backoffCap,
        _backoffJitter = backoffJitter;

  final Command _command;
  final Keys _keys;
  final Map<String, int> _queues;
  final _handlers = <String, TaskHandler>{};

  /// The first retry waits [_backoffBase]; each further retry doubles the wait
  /// up to [_backoffCap]. [_backoffJitter] adds a random positive fraction of
  /// that wait so a burst of simultaneous failures doesn't re-fire in lockstep.
  final Duration _backoffBase;
  final Duration _backoffCap;
  final double _backoffJitter;
  final _random = Random();

  var _running = false;

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

  /// Connects a worker. [queues] maps queue name to weight; higher weight is
  /// drained more often.
  ///
  /// [backoffBase]/[backoffCap] bound the retry backoff: the first retry waits
  /// [backoffBase], each further retry doubles it up to [backoffCap].
  /// [backoffJitter] (0..1) is the fraction of that delay added at random.
  static Future<Worker> connect({
    String host = 'localhost',
    int port = 6379,
    String prefix = 'rtq',
    Map<String, int> queues = const {'critical': 6, 'default': 3, 'low': 1},
    Duration backoffBase = const Duration(seconds: 1),
    Duration backoffCap = const Duration(seconds: 60),
    double backoffJitter = 0.1,
  }) async {
    final conn = RedisConnection();
    final command = await conn.connect(host, port);
    return Worker._(
      command,
      Keys(prefix),
      queues,
      backoffBase: backoffBase,
      backoffCap: backoffCap,
      backoffJitter: backoffJitter,
    );
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

  /// Runs the poll loop until [stop] is called. Each iteration first promotes
  /// any delayed tasks that have come due, then does a blocking pop across the
  /// weighted queue order so an idle worker doesn't spin.
  Future<void> run() async {
    _running = true;
    final keys = _weightedOrder().map(_keys.pending).toList();
    while (_running) {
      // Promote due tasks (scheduled tasks and elapsed retry backoffs) before
      // blocking. Pairing this with the short (1s) BRPOP timeout below is what
      // keeps the blocking pop from starving the mover: the pop parks for at
      // most a second, then the loop cycles back here, so a due task waits no
      // longer than roughly that timeout past its scheduled time. No separate
      // timer/isolate needed.
      await _promoteDue();
      // BRPOP blocks up to 1s across the weighted list of keys, returning from
      // whichever has an item first. The weighting comes from how many times a
      // queue appears in `keys`.
      final res = await _command.send_object(['BRPOP', ...keys, '1']) as List;
      // On timeout the redis client returns `[null]`; on a hit it returns
      // `[key, value]`. Guard on the value so an empty poll just loops again.
      if (res.isEmpty || res.first == null) continue;
      await _process(Envelope.decode(res[1] as String));
    }
  }

  /// Moves every due task, scheduled or backed-off, from each queue's delayed
  /// set back onto its pending list, via the atomic [_promoteScript].
  Future<void> _promoteDue() async {
    final now = DateTime.now().millisecondsSinceEpoch;
    for (final queue in _queues.keys) {
      await _command.send_object([
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

  Future<void> _process(Envelope env) async {
    final handler = _handlers[env.task.type];
    try {
      if (handler == null) {
        throw StateError('no handler for task type "${env.task.type}"');
      }
      await handler(env.task);
    } catch (_) {
      await _retryOrDeadLetter(env);
    }
  }

  Future<void> _retryOrDeadLetter(Envelope env) async {
    if (env.attempt >= env.maxRetries) {
      await _command.send_object(['LPUSH', _keys.deadLetter(), env.encode()]);
      return;
    }
    env.attempt++;
    // Hold the retry back with an exponential backoff instead of re-enqueuing
    // immediately: score it with the time it becomes due and drop it in the
    // queue's delayed set. The run loop's mover promotes it once due.
    final dueAt = DateTime.now().millisecondsSinceEpoch +
        _backoffFor(env.attempt).inMilliseconds;
    await _command.send_object([
      'ZADD',
      _keys.delayed(env.queue),
      '$dueAt',
      env.encode(),
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

  /// Stops the poll loop after the current iteration.
  void stop() => _running = false;

  Future<void> close() => _command.get_connection().close();
}
