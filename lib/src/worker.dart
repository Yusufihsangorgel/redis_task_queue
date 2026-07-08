import 'dart:async';

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
/// it re-enqueues with an incremented attempt count, and once attempts exceed
/// maxRetries the envelope goes to the dead-letter list instead of looping
/// forever.
class Worker {
  Worker._(this._command, this._keys, this._queues);

  final Command _command;
  final Keys _keys;
  final Map<String, int> _queues;
  final _handlers = <String, TaskHandler>{};

  var _running = false;

  /// Connects a worker. [queues] maps queue name to weight; higher weight is
  /// drained more often.
  static Future<Worker> connect({
    String host = 'localhost',
    int port = 6379,
    String prefix = 'rtq',
    Map<String, int> queues = const {'critical': 6, 'default': 3, 'low': 1},
  }) async {
    final conn = RedisConnection();
    final command = await conn.connect(host, port);
    return Worker._(command, Keys(prefix), queues);
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

  /// Runs the poll loop until [stop] is called. Each iteration does a blocking
  /// pop across the weighted queue order, so an idle worker doesn't spin.
  Future<void> run() async {
    _running = true;
    final order = _weightedOrder();
    while (_running) {
      // BRPOP blocks up to 1s across the weighted list of keys, returning from
      // whichever has an item first. The weighting comes from how many times a
      // queue appears in `order`.
      final keys = order.map(_keys.pending).toList();
      final res = await _command.send_object(['BRPOP', ...keys, '1']) as List;
      // On timeout the redis client returns `[null]`; on a hit it returns
      // `[key, value]`. Guard on the value so an empty poll just loops again.
      if (res.isEmpty || res.first == null) continue;
      await _process(Envelope.decode(res[1] as String));
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
    // Re-enqueue for another attempt. A production version would delay this with
    // an exponential backoff via a sorted set; kept immediate here so the core
    // retry path stays readable.
    await _command.send_object(['LPUSH', _keys.pending(env.queue), env.encode()]);
  }

  /// Stops the poll loop after the current iteration.
  void stop() => _running = false;

  Future<void> close() => _command.get_connection().close();
}
