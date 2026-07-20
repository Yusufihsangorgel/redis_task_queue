import 'dart:async';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';
import 'package:test/test.dart';

/// Needs a Redis instance. Point REDIS_PORT at one (the CI/dev default is 6399
/// so it won't clobber a local 6379).
const _host = 'localhost';
final _port = int.parse(
  String.fromEnvironment('REDIS_PORT', defaultValue: '6399'),
);
const _prefix = 'rtq_stats_test';

Future<void> _flush() async {
  final conn = RedisConnection();
  final cmd = await conn.connect(_host, _port);
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) {
    await cmd.send_object(['DEL', ...keys.cast<String>()]);
  }
  await cmd.get_connection().close();
}

Future<bool> _pollUntil(
  FutureOr<bool> Function() check, {
  Duration timeout = const Duration(seconds: 10),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    if (await check()) return true;
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
  return false;
}

void main() {
  setUp(_flush);

  test('counts pending and delayed per queue, before any worker runs',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    try {
      await client.enqueue(Task('a', {}), queue: 'high');
      await client.enqueue(Task('b', {}), queue: 'high');
      await client.enqueue(Task('c', {}), queue: 'low');
      // A scheduled task goes to the delayed set, not pending.
      await client.enqueue(
        Task('later', {}),
        queue: 'high',
        processIn: const Duration(minutes: 5),
      );

      // Discovery finds both queues.
      final stats = await client.stats();
      expect(stats.pending['high'], 2);
      expect(stats.pending['low'], 1);
      expect(stats.delayed['high'], 1);
      expect(stats.delayed['low'], 0);
      expect(stats.totalPending, 3);
      expect(stats.totalDelayed, 1);
      expect(stats.deadLetter, 0);
      expect(stats.inFlight, 0);
    } finally {
      await client.close();
    }
  });

  test('an explicit queue list counts exactly those, with no scan', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    try {
      await client.enqueue(Task('a', {}), queue: 'orders');

      // 'payments' has no keys yet, but asking for it reports zero rather than
      // omitting it, which is what a fixed dashboard wants.
      final stats = await client.stats(queues: ['orders', 'payments']);
      expect(stats.pending, {'orders': 1, 'payments': 0});
      expect(stats.delayed, {'orders': 0, 'payments': 0});
    } finally {
      await client.close();
    }
  });

  test('reflects in-flight and dead-letter depth', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    // A worker that blocks on the task so it stays in flight while we look.
    final holding = Completer<void>();
    final release = Completer<void>();
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      backoffBase: const Duration(milliseconds: 5),
    );
    worker.handle('hold', (task, context) async {
      if (!holding.isCompleted) holding.complete();
      await release.future;
    });
    worker.handle('doomed', (task, context) => throw StateError('x'));
    final loop = worker.run();

    try {
      await client.enqueue(Task('hold', {}));
      await holding.future.timeout(const Duration(seconds: 5));

      // The held task is off pending and on the worker's in-flight list.
      // Ask for 'default' explicitly: discovery only finds queues that still
      // have a key, and an empty pending list is deleted by Redis, so a queue
      // whose only task is in flight has no key to discover.
      final busy = await client.stats(queues: ['default']);
      expect(busy.inFlight, 1);
      expect(busy.pending['default'], 0);

      // Release the held task so the single worker is free again, then
      // dead-letter one and watch the count.
      release.complete();
      await client.enqueue(Task('doomed', {}), maxRetries: 0);
      final dead = await _pollUntil(() async {
        final s = await client.stats();
        return s.deadLetter == 1;
      });
      expect(dead, isTrue);
    } finally {
      if (!release.isCompleted) release.complete();
      worker.stop();
      await loop;
      await worker.close();
      await client.close();
    }
  });

  test('rejects an out-of-range limit on deadLetters', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    expect(() => client.deadLetters(limit: 0), throwsArgumentError);
    await client.close();
  });
}
