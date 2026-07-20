import 'dart:math';

import 'package:redis/redis.dart';

import 'keys.dart';
import 'schedule.dart';
import 'task.dart';

/// Enqueues tasks onto Redis. Safe to keep one client for the lifetime of your
/// app and reuse it: enqueuing is a single LPUSH (a single ZADD for a
/// scheduled task).
class QueueClient {
  QueueClient._(this._command, this._keys) : _idPrefix = _randomPrefix();

  final Command _command;
  final Keys _keys;

  /// A random prefix drawn once per client, so ids are unique across processes,
  /// not just within one. See [_newId].
  final String _idPrefix;

  /// Twelve bytes (96 bits) of secure randomness as hex, unique per client with
  /// overwhelming probability: two clients would have to draw the same 96-bit
  /// value to collide.
  static String _randomPrefix() {
    final random = Random.secure();
    final bytes = [for (var i = 0; i < 12; i++) random.nextInt(256)];
    return bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
  }

  /// Connects to Redis and returns a ready client. [prefix] namespaces all
  /// keys so the queue can share a Redis instance with other data.
  static Future<QueueClient> connect({
    String host = 'localhost',
    int port = 6379,
    String prefix = 'rtq',
  }) async {
    final conn = RedisConnection();
    final command = await conn.connect(host, port);
    return QueueClient._(command, Keys(prefix));
  }

  /// Enqueues [task] and returns its id. [queue] selects which queue it lands
  /// on (the worker drains queues by weight); [maxRetries] caps how many times
  /// the worker will retry it before moving it to the dead-letter list.
  ///
  /// [processAt] or [processIn] (set at most one) holds the task back until a
  /// future time: instead of the pending list it goes into the queue's delayed
  /// sorted set, scored with its due time, and the worker's due-mover promotes
  /// it once due. A time in the past runs promptly on the next mover pass.
  /// Setting both is a programmer error: an assert rejects it in debug builds,
  /// and with asserts compiled out [processAt] wins.
  Future<String> enqueue(
    Task task, {
    String queue = 'default',
    int maxRetries = 5,
    DateTime? processAt,
    Duration? processIn,
  }) async {
    final id = _newId();
    final env = Envelope(
      id: id,
      task: task,
      queue: queue,
      maxRetries: maxRetries,
    );
    if (processAt == null && processIn == null) {
      await _command.send_object(['LPUSH', _keys.pending(queue), env.encode()]);
      return id;
    }
    final dueAt = dueAtMillis(
      processAt: processAt,
      processIn: processIn,
      now: DateTime.now(),
    );
    await _command.send_object(
      ['ZADD', _keys.delayed(queue), '$dueAt', env.encode()],
    );
    return id;
  }

  Future<void> close() => _command.get_connection().close();

  // A time-free, collision-resistant-enough id for a job. Not a UUID library on
  // purpose — the queue shouldn't pull in a dependency for this.
  var _counter = 0;

  /// A task id, unique across producer processes.
  ///
  /// The id is the deduplication key an idempotent handler writes against
  /// ([TaskContext.id]), so a collision between two different tasks would make
  /// a handler treat the second as a repeat of the first and skip it: silent
  /// data loss. The old id was `identityHashCode(this)` plus a per-process
  /// counter, both of which reset or repeat across processes, so two producers
  /// could mint the same id. This pairs a per-client random prefix with the
  /// counter, so the prefix separates clients and the counter orders within
  /// one.
  String _newId() {
    _counter++;
    return '$_idPrefix-$_counter';
  }
}
