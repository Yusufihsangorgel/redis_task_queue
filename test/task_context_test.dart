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
const _prefix = 'rtq_context_test';
const _workerId = 'context-test-worker';

// Delete only this suite's keys: test files run concurrently against one Redis,
// so a blanket FLUSHDB would wipe another suite mid-test.
Future<void> _flush() async {
  final conn = RedisConnection();
  final cmd = await conn.connect(_host, _port);
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) {
    await cmd.send_object(['DEL', ...keys.cast<String>()]);
  }
  await cmd.get_connection().close();
}

Future<Worker> _worker() => Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      workerId: _workerId,
      // Retries are the subject here, so don't spend real backoff waiting.
      backoffBase: const Duration(milliseconds: 5),
      backoffCap: const Duration(milliseconds: 20),
    );

void main() {
  setUp(_flush);

  test('the handler sees the id enqueue returned, unchanged across retries',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await _worker();
    final seen = <TaskContext>[];
    final done = Completer<void>();

    worker.handle('retrying', (task, context) {
      seen.add(context);
      // Fail the first two runs so the third one is a genuine retry.
      if (context.attempt < 3) throw StateError('not yet');
      if (!done.isCompleted) done.complete();
    });
    final loop = worker.run();

    final id = await client.enqueue(Task('retrying', {}), maxRetries: 5);
    await done.future.timeout(const Duration(seconds: 10));

    // A dedup key is only useful if it is the same value every time, including
    // the value the enqueuing side was handed.
    expect(seen.map((c) => c.id), everyElement(id));
    expect(seen.map((c) => c.attempt), [1, 2, 3]);
    expect(seen.map((c) => c.queue), everyElement('default'));

    worker.stop();
    await loop;
    await worker.close();
    await client.close();
  });

  test('maxAttempts is the first run plus its retries', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await _worker();
    final seen = Completer<TaskContext>();

    worker.handle('once', (task, context) {
      if (!seen.isCompleted) seen.complete(context);
    });
    final loop = worker.run();

    await client.enqueue(Task('once', {}), maxRetries: 5);
    final context = await seen.future.timeout(const Duration(seconds: 10));
    expect(context.maxAttempts, 6);
    expect(context.isLastAttempt, isFalse);

    worker.stop();
    await loop;
    await worker.close();
    await client.close();
  });

  test('isLastAttempt is true on exactly the run whose failure dead-letters',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await _worker();
    final flags = <bool>[];
    final deadLettered = Completer<void>();
    final workerWithHook = worker;

    workerWithHook.handle('doomed', (task, context) {
      flags.add(context.isLastAttempt);
      throw StateError('always fails');
    });
    final loop = workerWithHook.run();

    // maxRetries: 2 means three runs in all, and the third is the one whose
    // failure gives up. Anything else here is an off-by-one that would have a
    // last-ditch handler either never fire or fire while a retry is still
    // coming.
    await client.enqueue(Task('doomed', {}), maxRetries: 2);

    final conn = RedisConnection();
    final cmd = await conn.connect(_host, _port);
    final deadline = DateTime.now().add(const Duration(seconds: 10));
    while (DateTime.now().isBefore(deadline)) {
      final len = await cmd.send_object(['LLEN', '$_prefix:dead']) as int;
      if (len == 1) {
        deadLettered.complete();
        break;
      }
      await Future<void>.delayed(const Duration(milliseconds: 25));
    }
    await cmd.get_connection().close();

    expect(deadLettered.isCompleted, isTrue,
        reason: 'the task never reached the dead-letter list');
    expect(flags, [false, false, true]);

    workerWithHook.stop();
    await loop;
    await workerWithHook.close();
    await client.close();
  });
}
