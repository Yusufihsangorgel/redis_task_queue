import 'dart:async';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';
import 'package:test/test.dart';

const _host = 'localhost';
final _port = int.parse(
  String.fromEnvironment('REDIS_PORT', defaultValue: '6399'),
);
const _prefix = 'rtq_worker_id_test';

Future<void> _flush() async {
  final cmd = await RedisConnection().connect(_host, _port);
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) await cmd.send_object(['DEL', ...keys.cast<String>()]);
  await cmd.get_connection().close();
}

void main() {
  setUp(_flush);
  tearDown(_flush);

  // Before workerId was required, both of these workers defaulted to the host
  // name, so the second one's startup recovery treated the first one's live
  // in-flight task as an orphan and ran it a second time.
  test('a second worker cannot claim a live worker id', () async {
    final a = await Worker.connect(
      workerId: 'shared',
      port: _port,
      prefix: _prefix,
      queues: const {'default': 1},
    );
    addTearDown(a.close);

    expect(
      () => Worker.connect(
        workerId: 'shared',
        port: _port,
        prefix: _prefix,
        queues: const {'default': 1},
      ),
      throwsA(
        isA<StateError>().having(
          (e) => e.message,
          'message',
          contains('already in use'),
        ),
      ),
    );
  });

  test('the same id is free again once the holder closes', () async {
    final a = await Worker.connect(
      workerId: 'reusable',
      port: _port,
      prefix: _prefix,
      queues: const {'default': 1},
    );
    await a.close();

    final b = await Worker.connect(
      workerId: 'reusable',
      port: _port,
      prefix: _prefix,
      queues: const {'default': 1},
    );
    addTearDown(b.close);
    expect(b, isA<Worker>());
  });

  // The claim is per Redis instance and per prefix: two workers with the same
  // id against different prefixes have different in-flight lists, so they are
  // not in conflict.
  test('the same id under a different prefix is allowed', () async {
    final a = await Worker.connect(
      workerId: 'ns',
      port: _port,
      prefix: '${_prefix}_one',
      queues: const {'default': 1},
    );
    addTearDown(a.close);
    final b = await Worker.connect(
      workerId: 'ns',
      port: _port,
      prefix: '${_prefix}_two',
      queues: const {'default': 1},
    );
    addTearDown(b.close);
    expect(b, isA<Worker>());
  });

  test('an empty workerId is rejected', () async {
    expect(
      () => Worker.connect(workerId: '', port: _port, prefix: _prefix),
      throwsA(isA<ArgumentError>()),
    );
  });

  test('run after close throws instead of starting a zombie loop', () async {
    final worker = await Worker.connect(
      workerId: 'closed',
      port: _port,
      prefix: _prefix,
      queues: const {'default': 1},
    );
    await worker.close();

    expect(
      () => worker.run(),
      throwsA(
        isA<StateError>().having(
          (e) => e.message,
          'message',
          contains('has been closed'),
        ),
      ),
    );
  });

  test('close is idempotent and stops a running loop', () async {
    final worker = await Worker.connect(
      workerId: 'twice',
      port: _port,
      prefix: _prefix,
      queues: const {'default': 1},
    );
    final loop = worker.run();
    await worker.close();
    await worker.close(); // no-op, must not throw
    await loop; // the loop really exited rather than polling a closed socket
  });
}
