import 'dart:async';
import 'dart:convert';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';
import 'package:test/test.dart';

/// These tests need a Redis instance. Point REDIS_PORT at one (the CI/dev
/// default is 6399 so it won't clobber a local 6379).
const _host = 'localhost';
final _port = int.parse(
  String.fromEnvironment('REDIS_PORT', defaultValue: '6399'),
);
const _prefix = 'rtq_reliability_test';
const _workerId = 'test-worker';

String _inFlightKey() => '$_prefix:inflight:$_workerId';
String _pendingKey(String queue) => '$_prefix:queue:$queue';

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

  test('a running task is held on the in-flight list, not lost, until it finishes',
      () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      workerId: _workerId,
    );

    final started = Completer<void>();
    final release = Completer<void>();
    worker.handle('slow', (_) async {
      started.complete();
      await release.future; // Block, standing in for work in progress.
    });
    final loop = worker.run();

    await client.enqueue(Task('slow', {}));
    await started.future.timeout(const Duration(seconds: 5));

    // While the handler runs, the task sits on the in-flight list, off pending,
    // and nowhere else. A crash at this instant would not lose it: the next run
    // of this worker finds it here.
    final inspect = await _connect();
    final inflight =
        await inspect.send_object(['LLEN', _inFlightKey()]) as int;
    final pending =
        await inspect.send_object(['LLEN', _pendingKey('default')]) as int;
    expect(inflight, 1,
        reason: 'the running task must be held on the in-flight list');
    expect(pending, 0,
        reason: 'it must be off the pending list while it runs');

    // Once it finishes, the in-flight list drains.
    release.complete();
    final drained = await _pollUntil(
      () async => (await inspect.send_object(['LLEN', _inFlightKey()]) as int) == 0,
    );
    expect(drained, isTrue,
        reason: 'a finished task must leave the in-flight list');

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await inspect.get_connection().close();
    await client.close();
    await worker.close();
  });

  test('a worker recovers tasks a previous run left in flight when it died',
      () async {
    // Stand in for a worker that claimed a task and died before finishing it:
    // seed the in-flight list with an envelope, exactly as an interrupted claim
    // would have left it.
    final seed = await _connect();
    final envelope = jsonEncode({
      'id': 'recover-1',
      'task': {
        'type': 'resume',
        'payload': {'n': 7},
      },
      'queue': 'default',
      'max_retries': 3,
      'attempt': 0,
    });
    await seed.send_object(['LPUSH', _inFlightKey(), envelope]);
    await seed.get_connection().close();

    // A fresh worker with the same id starts up; it must requeue and run the
    // orphan rather than leave it stranded.
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      workerId: _workerId,
    );
    final ran = Completer<int>();
    worker.handle('resume', (task) => ran.complete(task.payload['n'] as int));
    final loop = worker.run();

    expect(await ran.future.timeout(const Duration(seconds: 5)), 7,
        reason: 'the orphaned task must be recovered and run');

    // The in-flight list is clean once the recovered task completes.
    final inspect = await _connect();
    final drained = await _pollUntil(
      () async => (await inspect.send_object(['LLEN', _inFlightKey()]) as int) == 0,
    );
    expect(drained, isTrue);

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await inspect.get_connection().close();
    await worker.close();
  });

  test('another worker\'s in-flight list is left untouched during recovery',
      () async {
    // A live peer with a different id is mid-task: its in-flight list must not
    // be reclaimed by this worker's startup recovery.
    final seed = await _connect();
    final peerKey = '$_prefix:inflight:other-worker';
    final peerEnvelope = jsonEncode({
      'id': 'peer-1',
      'task': {'type': 'x', 'payload': <String, dynamic>{}},
      'queue': 'default',
      'max_retries': 0,
      'attempt': 0,
    });
    await seed.send_object(['LPUSH', peerKey, peerEnvelope]);

    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
      workerId: _workerId,
    );
    worker.handle('x', (_) {});
    final loop = worker.run();

    // Give recovery + a poll cycle time to run, then confirm the peer's task is
    // still exactly where it was.
    await Future<void>.delayed(const Duration(milliseconds: 500));
    final stillThere =
        await seed.send_object(['LLEN', peerKey]) as int;
    expect(stillThere, 1,
        reason: "a peer worker's in-flight task must not be recovered");

    worker.stop();
    await loop.timeout(const Duration(seconds: 3));
    await seed.get_connection().close();
    await worker.close();
  });
}
