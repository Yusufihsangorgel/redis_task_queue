import 'dart:async';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';
import 'package:test/test.dart';

/// These tests need a Redis instance. Point REDIS_PORT at one (the CI/dev
/// default is 6399 so it won't clobber a local 6379).
const _host = 'localhost';
final _port = int.parse(
  String.fromEnvironment('REDIS_PORT', defaultValue: '6399'),
);
const _prefix = 'rtq_test';

Future<void> _flush() async {
  final conn = RedisConnection();
  final cmd = await conn.connect(_host, _port);
  await cmd.send_object(['FLUSHDB']);
  await cmd.get_connection().close();
}

void main() {
  setUp(_flush);

  test('a valid task runs in the worker', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
    );

    final ran = Completer<String>();
    worker.handle('greet', (task) => ran.complete(task.payload['name'] as String));
    final loop = worker.run();

    await client.enqueue(Task('greet', {'name': 'jane'}));

    expect(await ran.future.timeout(const Duration(seconds: 5)), 'jane');
    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await client.close();
    await worker.close();
  });

  test('a failing task lands in the dead-letter list after maxRetries',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
    );

    var attempts = 0;
    worker.handle('boom', (_) {
      attempts++;
      throw StateError('always fails');
    });
    final loop = worker.run();

    await client.enqueue(Task('boom', {}), maxRetries: 2);

    // Poll the dead-letter list until the envelope shows up.
    final conn = RedisConnection();
    final cmd = await conn.connect(_host, _port);
    String? dead;
    for (var i = 0; i < 50 && dead == null; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 100));
      dead = await cmd.send_object(['RPOP', '$_prefix:dead']) as String?;
    }

    expect(dead, isNotNull, reason: 'envelope should reach the dead-letter list');
    expect(attempts, greaterThanOrEqualTo(3)); // initial + 2 retries

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await cmd.get_connection().close();
    await client.close();
    await worker.close();
  });
}
