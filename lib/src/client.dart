import 'package:redis/redis.dart';

import 'keys.dart';
import 'task.dart';

/// Enqueues tasks onto Redis. Safe to keep one client for the lifetime of your
/// app and reuse it — enqueuing is a single LPUSH.
class QueueClient {
  QueueClient._(this._command, this._keys);

  final Command _command;
  final Keys _keys;

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
  Future<String> enqueue(
    Task task, {
    String queue = 'default',
    int maxRetries = 5,
  }) async {
    final id = _newId();
    final env = Envelope(
      id: id,
      task: task,
      queue: queue,
      maxRetries: maxRetries,
    );
    await _command.send_object(['LPUSH', _keys.pending(queue), env.encode()]);
    return id;
  }

  Future<void> close() => _command.get_connection().close();

  // A time-free, collision-resistant-enough id for a job. Not a UUID library on
  // purpose — the queue shouldn't pull in a dependency for this.
  var _counter = 0;
  String _newId() {
    _counter++;
    return '${identityHashCode(this).toRadixString(16)}-$_counter';
  }
}
