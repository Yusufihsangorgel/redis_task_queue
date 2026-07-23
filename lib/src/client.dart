import 'dart:math';

import 'package:redis/redis.dart';

import 'keys.dart';
import 'schedule.dart';
import 'stats.dart';
import 'task.dart';

/// Enqueues tasks onto Redis. Safe to keep one client for the lifetime of your
/// app and reuse it: enqueuing is a single LPUSH (a single ZADD for a
/// scheduled task). A dropped connection doesn't end that lifetime either: the
/// next call reconnects and retries once before giving up, so a Redis restart
/// or failover doesn't permanently break a client that survives it.
class QueueClient {
  QueueClient._(this._command, this._keys, this._host, this._port)
      : _idPrefix = _randomPrefix();

  Command _command;
  final Keys _keys;
  final String _host;
  final int _port;

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
    final command = await _dial(host, port);
    return QueueClient._(command, Keys(prefix), host, port);
  }

  static Future<Command> _dial(String host, int port) =>
      RedisConnection().connect(host, port);

  /// Sends [command] to Redis, reconnecting and retrying once if it fails.
  ///
  /// The underlying `redis` package holds one socket for the whole life of the
  /// connection and never redials it: once that socket dies (a Redis restart, a
  /// managed-Redis failover, a proxy's idle timeout), every call on it fails
  /// forever, even long after Redis is healthy again. The package's errors for
  /// this aren't even a consistent type across failures (a bare `"stream is
  /// closed"` String, a `StateError`, a real `SocketException`), so rather than
  /// pattern-match them, any failure here is treated as "the socket might be
  /// dead": open a fresh connection and retry the same command once. A second
  /// failure after that means Redis is actually down, not just blipped, so it
  /// is rethrown rather than retried forever.
  Future<dynamic> _send(List<Object> command) async {
    try {
      return await _command.send_object(command);
    } catch (_) {
      _command = await _dial(_host, _port);
      return await _command.send_object(command);
    }
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
      await _send(['LPUSH', _keys.pending(queue), env.encode()]);
      return id;
    }
    final dueAt = dueAtMillis(
      processAt: processAt,
      processIn: processIn,
      now: DateTime.now(),
    );
    await _send(
      ['ZADD', _keys.delayed(queue), '$dueAt', env.encode()],
    );
    return id;
  }

  /// The dead-letter entries, newest first, up to [limit].
  ///
  /// A task that exhausts its retries lands here with the error that gave up on
  /// it. Read them to see what failed and why; nothing drains the list on its
  /// own, so a queue nobody inspects is an outage nobody hears about.
  Future<List<DeadLetter>> deadLetters({int limit = 100}) async {
    if (limit < 1) {
      throw ArgumentError.value(limit, 'limit', 'must be at least 1');
    }
    final raw = await _send(
      ['LRANGE', _keys.deadLetter(), '0', '${limit - 1}'],
    ) as List;
    return [for (final entry in raw) DeadLetter.decode(entry as String)];
  }

  /// Re-enqueues the dead-letter entry with task id [id] onto its original
  /// queue for a fresh set of attempts, and removes it from the dead-letter
  /// list. Returns whether an entry with that id was found.
  ///
  /// The remove and the re-enqueue happen in one atomic step, so a task can't
  /// be dropped from the dead-letter list without landing back on its queue, or
  /// enqueued twice if two callers replay it at once. Do this after fixing what
  /// made the task fail; replaying an unfixed task just sends it back to the
  /// dead-letter list.
  Future<bool> replayDeadLetter(String id) async {
    final raw = await _send(
      ['LRANGE', _keys.deadLetter(), '0', '-1'],
    ) as List;
    for (final entry in raw.cast<String>()) {
      final dead = DeadLetter.decode(entry);
      if (dead.id != id) continue;
      // Rebuild the task fresh: a replay starts its retry budget over.
      final fresh = Envelope(
        id: dead.id,
        task: dead.task,
        queue: dead.queue,
        // maxRetries is not stored on DeadLetter, so recover it from attempts:
        // attempts == maxRetries + 1.
        maxRetries: dead.attempts - 1,
      );
      final moved = await _send([
        'EVAL',
        _replayScript,
        '2',
        _keys.deadLetter(),
        _keys.pending(dead.queue),
        entry, // ARGV[1]: the exact dead entry to remove
        fresh.encode(), // ARGV[2]: the fresh envelope to enqueue
      ]);
      return moved == 1;
    }
    return false;
  }

  /// Atomically removes one exact dead entry and enqueues a fresh envelope, so
  /// a concurrent replay of the same entry can't double-enqueue it. KEYS[1]
  /// dead-letter list, KEYS[2] pending list; ARGV[1] the dead entry to remove,
  /// ARGV[2] the fresh envelope to push. Returns 1 if it removed and enqueued,
  /// 0 if the entry was already gone.
  static const _replayScript = '''
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[2])
  return 1
end
return 0
''';

  /// Empties the dead-letter list and returns how many entries it removed. Use
  /// it once the entries have been triaged and are not worth replaying.
  Future<int> purgeDeadLetters() async {
    final count = await _send(['LLEN', _keys.deadLetter()]) as int;
    await _send(['DEL', _keys.deadLetter()]);
    return count;
  }

  /// A snapshot of queue depth: pending and delayed per queue, plus the total
  /// in-flight and dead-letter counts.
  ///
  /// Pass [queues] to count exactly those, which is one round trip per queue
  /// and no key scanning; a named queue with no keys reports zero rather than
  /// being left out, which is what a fixed dashboard wants. Omit it to discover
  /// the queues that currently have keys, with a `SCAN` (not `KEYS`, so it does
  /// not block Redis). Discovery only sees a queue that has a pending or delayed
  /// key right now: Redis deletes an empty list, so a queue whose only task is
  /// in flight has nothing to discover. Name it explicitly to always see it.
  /// Discovery also assumes a queue name does not itself end in `:delayed`.
  ///
  /// This reads counters, not the tasks, so it is cheap enough to poll for a
  /// dashboard or an alert on a growing backlog.
  Future<QueueStats> stats({Iterable<String>? queues}) async {
    final names = queues != null ? queues.toSet() : await _discoverQueues();

    final pending = <String, int>{};
    final delayed = <String, int>{};
    for (final queue in names) {
      pending[queue] = await _send(['LLEN', _keys.pending(queue)]) as int;
      delayed[queue] = await _send(['ZCARD', _keys.delayed(queue)]) as int;
    }

    final deadLetter = await _send(['LLEN', _keys.deadLetter()]) as int;

    var inFlight = 0;
    for (final key in await _scanKeys('${_keys.prefix}:inflight:*')) {
      inFlight += await _send(['LLEN', key]) as int;
    }

    return QueueStats(
      pending: pending,
      delayed: delayed,
      deadLetter: deadLetter,
      inFlight: inFlight,
    );
  }

  /// The queue names that currently have a pending list or a delayed set.
  Future<Set<String>> _discoverQueues() async {
    final queuePrefix = '${_keys.prefix}:queue:';
    const delayedSuffix = ':delayed';
    final names = <String>{};
    for (final key in await _scanKeys('$queuePrefix*')) {
      final rest = key.substring(queuePrefix.length);
      names.add(rest.endsWith(delayedSuffix)
          ? rest.substring(0, rest.length - delayedSuffix.length)
          : rest);
    }
    return names;
  }

  /// Cursor-based `SCAN` for every key matching [pattern]. Uses `SCAN` rather
  /// than `KEYS` so a large keyspace does not block Redis.
  Future<List<String>> _scanKeys(String pattern) async {
    final keys = <String>[];
    var cursor = '0';
    do {
      final result = await _send(
        ['SCAN', cursor, 'MATCH', pattern, 'COUNT', '100'],
      ) as List;
      cursor = result[0] as String;
      keys.addAll((result[1] as List).cast<String>());
    } while (cursor != '0');
    return keys;
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
