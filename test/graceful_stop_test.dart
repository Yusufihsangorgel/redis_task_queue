import 'dart:async';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';
import 'package:test/test.dart';

const _host = 'localhost';
final _port = int.parse(
  String.fromEnvironment('REDIS_PORT', defaultValue: '6399'),
);
const _prefix = 'rtq_graceful_test';

Future<void> _flush() async {
  final cmd = await RedisConnection().connect(_host, _port);
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) await cmd.send_object(['DEL', ...keys.cast<String>()]);
  await cmd.get_connection().close();
}

void main() {
  setUp(_flush);

  test('awaiting stop() drains the in-flight task before returning', () async {
    final client =
        await QueueClient.connect(host: _host, port: _port, prefix: _prefix);
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
    );
    final started = Completer<void>();
    var finished = false;
    worker.handle('slow', (task, context) async {
      started.complete();
      await Future<void>.delayed(const Duration(milliseconds: 300));
      finished = true;
    });
    unawaited(worker.run());

    await client.enqueue(Task('slow', {}));
    await started.future; // the task is now in flight

    // The whole point: await stop() returns only after the task finished.
    await worker.stop();
    expect(finished, isTrue);

    await worker.close();
    await client.close();
  });

  test('stop() before run() returns an already-completed future', () async {
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
    );
    await worker.stop().timeout(const Duration(seconds: 1)); // must not hang
    await worker.close();
  });

  test('stop() is safe to call twice', () async {
    final worker = await Worker.connect(
      host: _host,
      port: _port,
      prefix: _prefix,
      queues: {'default': 1},
    );
    unawaited(worker.run());
    await worker.stop();
    await worker.stop().timeout(const Duration(seconds: 1));
    await worker.close();
  });
}
