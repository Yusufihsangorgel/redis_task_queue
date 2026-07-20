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

// Redis key layout the tests inspect directly, mirroring lib/src/keys.dart.
String _pendingKey(String queue) => '$_prefix:queue:$queue';
String _delayedKey(String queue) => '$_prefix:queue:$queue:delayed';
const _deadKey = '$_prefix:dead';

Future<Command> _connect() async {
  final conn = RedisConnection();
  return conn.connect(_host, _port);
}

// Delete only this suite's keys, not the whole DB: test files run concurrently
// against one Redis, so a blanket FLUSHDB would wipe another suite mid-test.
Future<void> _flush() async {
  final cmd = await _connect();
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) {
    await cmd.send_object(['DEL', ...keys.cast<String>()]);
  }
  await cmd.get_connection().close();
}

/// Polls [check] every 50ms until it returns true or [timeout] elapses.
/// Returns whether it became true, so callers can assert on the result.
Future<bool> _pollUntil(
  FutureOr<bool> Function() check, {
  Duration timeout = const Duration(seconds: 8),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    if (await check()) return true;
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
  return false;
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
    worker.handle('greet', (task, _) => ran.complete(task.payload['name'] as String));
    final loop = worker.run();

    await client.enqueue(Task('greet', {'name': 'jane'}));

    expect(await ran.future.timeout(const Duration(seconds: 5)), 'jane');
    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await client.close();
    await worker.close();
  });

  test('a failed task waits in the delayed set, not pending, before retry',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    // A long base backoff so the task stays in the delayed set long enough to
    // observe it there (the mover won't promote it for a minute).
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      backoffBase: const Duration(seconds: 60),
      backoffJitter: 0,
    );

    worker.handle('boom', (_, __) => throw StateError('always fails'));
    final loop = worker.run();

    await client.enqueue(Task('boom', {}), maxRetries: 5);

    final inspect = await _connect();
    // Wait until the worker has processed the task once and parked it in the
    // delayed set.
    final landed = await _pollUntil(() async {
      final n = await inspect.send_object(['ZCARD', _delayedKey('default')]);
      return (n as int) == 1;
    });
    expect(landed, isTrue, reason: 'failed task should land in the delayed set');

    // It must NOT be back on the pending list yet.
    final pending =
        await inspect.send_object(['LLEN', _pendingKey('default')]) as int;
    expect(pending, 0, reason: 'a backed-off retry must not be pending yet');

    // ...and its score must be in the future (the backoff hasn't elapsed).
    final withScores = await inspect.send_object(
      ['ZRANGE', _delayedKey('default'), '0', '-1', 'WITHSCORES'],
    ) as List;
    final score = int.parse(withScores[1] as String);
    expect(
      score,
      greaterThan(DateTime.now().millisecondsSinceEpoch),
      reason: 'the delayed task should be scheduled for the future',
    );

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await inspect.get_connection().close();
    await client.close();
    await worker.close();
  });

  test('the due-mover promotes a delayed task and the worker reprocesses it',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    // Small base so the backoff elapses quickly and the test stays fast.
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      backoffBase: const Duration(milliseconds: 50),
      backoffJitter: 0,
    );

    var attempts = 0;
    final succeeded = Completer<void>();
    worker.handle('flaky', (_, __) {
      attempts++;
      if (attempts == 1) throw StateError('fails once');
      succeeded.complete(); // second run succeeds
    });
    final loop = worker.run();

    await client.enqueue(Task('flaky', {}), maxRetries: 5);

    // The first run fails, the task waits out its backoff in the delayed set,
    // the mover promotes it, and the second run succeeds.
    await succeeded.future.timeout(const Duration(seconds: 8));
    expect(attempts, 2, reason: 'task should be reprocessed exactly once');

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await client.close();
    await worker.close();
  });

  test('a failing task lands in the dead-letter list after maxRetries',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    // Small base keeps the two backed-off retries fast and deterministic.
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      backoffBase: const Duration(milliseconds: 30),
      backoffJitter: 0,
    );

    var attempts = 0;
    worker.handle('boom', (_, __) {
      attempts++;
      throw StateError('always fails');
    });
    final loop = worker.run();

    await client.enqueue(Task('boom', {}), maxRetries: 2);

    // Poll the dead-letter list until the envelope shows up.
    final inspect = await _connect();
    String? dead;
    await _pollUntil(
      () async {
        dead = await inspect.send_object(['RPOP', _deadKey]) as String?;
        return dead != null;
      },
      timeout: const Duration(seconds: 10),
    );

    expect(dead, isNotNull, reason: 'envelope should reach the dead-letter list');
    expect(attempts, greaterThanOrEqualTo(3)); // initial + 2 retries

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await inspect.get_connection().close();
    await client.close();
    await worker.close();
  });

  test('a scheduled task waits in the delayed set, not pending', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);

    // Far enough out that nothing could promote it during the test. No worker
    // is needed: enqueue alone must place it correctly.
    await client.enqueue(
      Task('later', {}),
      processIn: const Duration(seconds: 60),
    );

    final inspect = await _connect();
    final delayed =
        await inspect.send_object(['ZCARD', _delayedKey('default')]) as int;
    expect(delayed, 1, reason: 'a scheduled task should sit in the delayed set');

    final pending =
        await inspect.send_object(['LLEN', _pendingKey('default')]) as int;
    expect(pending, 0, reason: 'a scheduled task must not be pending yet');

    // ...and its score must be the future due time.
    final withScores = await inspect.send_object(
      ['ZRANGE', _delayedKey('default'), '0', '-1', 'WITHSCORES'],
    ) as List;
    final score = int.parse(withScores[1] as String);
    expect(
      score,
      greaterThan(DateTime.now().millisecondsSinceEpoch),
      reason: 'the scheduled task should be due in the future',
    );

    await inspect.get_connection().close();
    await client.close();
  });

  test('a scheduled task runs once its time comes, not before', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
    );

    final ran = Completer<DateTime>();
    worker.handle('later', (_, __) => ran.complete(DateTime.now()));
    final loop = worker.run();

    // The score is taken inside enqueue, after enqueuedAt, so the handler
    // firing before enqueuedAt + delay would prove a promotion too early.
    // enqueuedAt is floored to millis to match the score's precision; that
    // keeps the bound airtight rather than off by a sub-millisecond sliver.
    const delay = Duration(milliseconds: 300);
    final enqueuedAt = DateTime.fromMillisecondsSinceEpoch(
      DateTime.now().millisecondsSinceEpoch,
    );
    await client.enqueue(Task('later', {}), processIn: delay);

    final ranAt = await ran.future.timeout(const Duration(seconds: 8));
    expect(
      ranAt.isBefore(enqueuedAt.add(delay)),
      isFalse,
      reason: 'the handler must not fire before the scheduled time',
    );

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await client.close();
    await worker.close();
  });

  test('a processAt in the past runs promptly', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
    );

    final ran = Completer<void>();
    worker.handle('overdue', (_, __) => ran.complete());
    final loop = worker.run();

    await client.enqueue(
      Task('overdue', {}),
      processAt: DateTime.now().subtract(const Duration(hours: 1)),
    );

    // An already-due score is promoted on the next mover pass, so this should
    // complete well inside the poll timeout.
    await ran.future.timeout(const Duration(seconds: 8));

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await client.close();
    await worker.close();
  });

  test('onError fires on each failure and onDeadLetter fires terminally',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final errors = <({int attempt, bool willRetry, Object error, String id})>[];
    final deadLettered = Completer<Task>();
    String? deadLetteredId;
    // Small base keeps the single backed-off retry fast and deterministic.
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      backoffBase: const Duration(milliseconds: 30),
      backoffJitter: 0,
      onError: (task, context, error, stackTrace) {
        errors.add((
          attempt: context.attempt,
          willRetry: !context.isLastAttempt,
          error: error,
          id: context.id,
        ));
      },
      onDeadLetter: (task, context, error, stackTrace) {
        deadLetteredId = context.id;
        if (!deadLettered.isCompleted) deadLettered.complete(task);
      },
    );

    worker.handle('boom', (_, __) => throw StateError('always fails'));
    final loop = worker.run();

    // maxRetries: 1 means two executions: attempt 1 (will retry), then
    // attempt 2 (retries exhausted -> dead-letter).
    final id = await client.enqueue(Task('boom', {}), maxRetries: 1);

    final task = await deadLettered.future.timeout(const Duration(seconds: 10));
    expect(task.type, 'boom');
    // onError should have seen both attempts, with willRetry flipping to false
    // on the last one, and the real exception passed through.
    expect(errors.map((e) => e.attempt).toList(), [1, 2]);
    expect(errors.map((e) => e.willRetry).toList(), [true, false]);
    expect(errors.every((e) => e.error is StateError), isTrue);
    // The point of handing the observers a context: a failure log and a
    // dead-letter alert can name the job, not just its type. Without the id
    // there is no way to get from either back to the task that produced it.
    expect(errors.map((e) => e.id).toSet(), {id});
    expect(deadLetteredId, id);

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await client.close();
    await worker.close();
  });

  test('the worker gives each queue first look in proportion to its weight',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    // Weights 3:2:1. Enqueue exactly the weight count to each queue, all up
    // front, so one full weighted cycle drains them and the lead order is
    // deterministic.
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'a': 3, 'b': 2, 'c': 1},
      workerId: 'fairness-worker',
    );

    final processed = <String>[];
    final done = Completer<void>();
    void record(Task task, TaskContext _) {
      processed.add(task.payload['q'] as String);
      if (processed.length == 6 && !done.isCompleted) done.complete();
    }

    worker.handle('a', record);
    worker.handle('b', record);
    worker.handle('c', record);

    // Enqueue to each queue's pending list. QueueClient.enqueue takes the queue
    // via its `queue` argument.
    for (var i = 0; i < 3; i++) {
      await client.enqueue(Task('a', {'q': 'a'}), queue: 'a');
    }
    for (var i = 0; i < 2; i++) {
      await client.enqueue(Task('b', {'q': 'b'}), queue: 'b');
    }
    await client.enqueue(Task('c', {'q': 'c'}), queue: 'c');

    final loop = worker.run();
    await done.future.timeout(const Duration(seconds: 8));

    // A fresh worker's cursor starts at 0, so the weighted order [a,a,a,b,b,c]
    // is followed exactly: the high-weight queue leads first, but the
    // low-weight queue still gets its turn within the cycle.
    expect(processed, ['a', 'a', 'a', 'b', 'b', 'c']);

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await client.close();
    await worker.close();
  });

  test('a throwing observer callback does not break the worker', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    // onError itself throws; the task must still reach the dead-letter list,
    // proving the callback is isolated from the processing path.
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      onError: (_, __, ___, ____) => throw StateError('observer is buggy'),
    );

    worker.handle('boom', (_, __) => throw StateError('always fails'));
    final loop = worker.run();

    // maxRetries: 0 sends the task straight to dead-letter on the first failure.
    await client.enqueue(Task('boom', {}), maxRetries: 0);

    final inspect = await _connect();
    final reached = await _pollUntil(
      () async => (await inspect.send_object(['LLEN', _deadKey]) as int) == 1,
      timeout: const Duration(seconds: 10),
    );
    expect(reached, isTrue,
        reason: 'task must dead-letter even though onError threw');

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await inspect.get_connection().close();
    await client.close();
    await worker.close();
  });
}
