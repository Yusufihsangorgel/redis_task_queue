import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';
import 'package:test/test.dart';

import '_redis.dart';

const _prefix = 'rtq_exceptions_test';

// A port nothing listens on, so dialling it fails immediately.
const _deadPort = 6301;

Future<void> _flush() async {
  final cmd = await RedisConnection().connect(redisHost, redisPort);
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) await cmd.send_object(['DEL', ...keys.cast<String>()]);
  await cmd.get_connection().close();
}

void main() {
  requireRedis();

  setUp(_flush);
  tearDown(_flush);

  test('connecting to a dead port reports a connection exception', () async {
    await expectLater(
      QueueClient.connect(port: _deadPort, prefix: _prefix),
      throwsA(isA<TaskQueueConnectionException>()),
    );
    await expectLater(
      Worker.connect(workerId: 'w', port: _deadPort, prefix: _prefix),
      throwsA(isA<TaskQueueConnectionException>()),
    );
  });

  // A pending queue is a list. Making the key hold a plain string first means
  // the LPUSH behind enqueue gets WRONGTYPE back from the server.
  test('a server refusal reports a server exception', () async {
    final raw = await RedisConnection().connect(redisHost, redisPort);
    await raw.send_object(['SET', '$_prefix:queue:default', 'not-a-list']);
    await raw.get_connection().close();

    final client = await QueueClient.connect(port: redisPort, prefix: _prefix);
    addTearDown(client.close);

    await expectLater(
      client.enqueue(Task('t', const {})),
      throwsA(
        isA<TaskQueueServerException>().having(
          (e) => e.cause,
          'cause',
          isNotNull,
        ),
      ),
    );
  });

  // The point of the hierarchy: one catch clause covers both, and it works
  // without importing package:redis.
  test('both are catchable as TaskQueueException', () async {
    Object? caught;
    try {
      await QueueClient.connect(port: _deadPort, prefix: _prefix);
    } on TaskQueueException catch (e) {
      caught = e;
    }
    expect(caught, isA<TaskQueueConnectionException>());
    expect(caught.toString(), contains('connect failed'));
  });

  // package:redis raises bare classes that do not implement Exception, so
  // `on Exception` used to miss them. The wrapper makes that work.
  test('the wrapped error is a Dart Exception', () async {
    Object? caught;
    try {
      await QueueClient.connect(port: _deadPort, prefix: _prefix);
    } on Exception catch (e) {
      caught = e;
    }
    expect(caught, isA<TaskQueueConnectionException>());
  });
}
