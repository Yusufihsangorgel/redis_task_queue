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
const _prefix = 'rtq_dla_test';

Future<void> _flush() async {
  final conn = RedisConnection();
  final cmd = await conn.connect(_host, _port);
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) {
    await cmd.send_object(['DEL', ...keys.cast<String>()]);
  }
  await cmd.get_connection().close();
}

Future<Worker> _worker({int weight = 1}) => Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': weight},
      backoffBase: const Duration(milliseconds: 5),
      backoffCap: const Duration(milliseconds: 20),
    );

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

  test('a dead-lettered task is inspectable, with its error', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await _worker();
    worker.handle('doomed', (task, context) {
      throw StateError('upstream is on fire');
    });
    final loop = worker.run();

    final id = await client.enqueue(Task('doomed', {'n': 7}), maxRetries: 1);

    final landed = await _pollUntil(() async {
      final list = await client.deadLetters();
      return list.any((d) => d.id == id);
    });
    expect(landed, isTrue, reason: 'task should reach the dead-letter list');

    final dead = (await client.deadLetters()).firstWhere((d) => d.id == id);
    expect(dead.task.type, 'doomed');
    expect(dead.task.payload['n'], 7);
    expect(dead.queue, 'default');
    // The whole point of 0.8.0: the error is stored, so triage can see it.
    expect(dead.error, contains('upstream is on fire'));
    expect(dead.attempts, 2); // maxRetries 1 -> two attempts
    expect(dead.deadAt, isNotNull);

    worker.stop();
    await loop;
    await worker.close();
    await client.close();
  });

  test('replaying re-runs the task and clears it from the dead list', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    var attempts = 0;
    final succeeded = Completer<void>();
    final worker = await _worker();
    worker.handle('flaky', (task, context) {
      attempts++;
      // Fail while it is first enqueued (so it dead-letters), succeed after a
      // replay: the replay is a fresh task, so this fires again.
      if (attempts <= 2) throw StateError('not yet');
      if (!succeeded.isCompleted) succeeded.complete();
    });
    final loop = worker.run();

    final id = await client.enqueue(Task('flaky', {}), maxRetries: 1);
    await _pollUntil(() async =>
        (await client.deadLetters()).any((d) => d.id == id));

    // Replay it. It should leave the dead list and run again to success.
    expect(await client.replayDeadLetter(id), isTrue);
    await succeeded.future.timeout(const Duration(seconds: 10));
    expect(attempts, 3); // two before dead-letter, one after replay

    final gone = await _pollUntil(
      () async => (await client.deadLetters()).every((d) => d.id != id),
    );
    expect(gone, isTrue, reason: 'replayed entry should leave the dead list');

    worker.stop();
    await loop;
    await worker.close();
    await client.close();
  });

  test('replaying an unknown id returns false', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    expect(await client.replayDeadLetter('no-such-id'), isFalse);
    await client.close();
  });

  test('purge empties the list and reports the count', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await _worker();
    worker.handle('boom', (task, context) => throw StateError('always'));
    final loop = worker.run();

    for (var i = 0; i < 3; i++) {
      await client.enqueue(Task('boom', {'i': i}), maxRetries: 0);
    }
    await _pollUntil(() async => (await client.deadLetters()).length == 3);

    expect(await client.purgeDeadLetters(), 3);
    expect(await client.deadLetters(), isEmpty);

    worker.stop();
    await loop;
    await worker.close();
    await client.close();
  });
}
