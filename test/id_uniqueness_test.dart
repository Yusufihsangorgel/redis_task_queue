import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';
import 'package:test/test.dart';

import '_redis.dart';

const _prefix = 'rtq_id_test';

Future<void> _flush() async {
  final conn = RedisConnection();
  final cmd = await conn.connect(redisHost, redisPort);
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) {
    await cmd.send_object(['DEL', ...keys.cast<String>()]);
  }
  await cmd.get_connection().close();
}

void main() {
  requireRedis();

  setUp(_flush);

  test('ids are unique across many separate clients', () async {
    // Each client stands in for a separate producer process. The old id was a
    // per-process identity hash plus a counter that restarted at 1 in every
    // process, so two clients minted the same first id often enough to lose a
    // task to the dedup key. Every client here starts fresh and the first id
    // each produces must still be distinct.
    const clientCount = 200;
    const perClient = 5;

    final ids = <String>[];
    final clients = <QueueClient>[];
    try {
      for (var c = 0; c < clientCount; c++) {
        final client = await QueueClient.connect(
          host: redisHost,
          port: redisPort,
          prefix: _prefix,
        );
        clients.add(client);
        for (var t = 0; t < perClient; t++) {
          ids.add(await client.enqueue(Task('noop', {'c': c, 't': t})));
        }
      }
    } finally {
      for (final client in clients) {
        await client.close();
      }
    }

    expect(ids, hasLength(clientCount * perClient));
    // The whole point: no two tasks share an id, so none is silently taken for
    // a duplicate of another.
    expect(
      ids.toSet(),
      hasLength(ids.length),
      reason: 'every task id must be unique across all clients',
    );
  });

  test('a single client mints distinct, ordered ids', () async {
    final client = await QueueClient.connect(
      host: redisHost,
      port: redisPort,
      prefix: _prefix,
    );
    try {
      final ids = [
        for (var i = 0; i < 100; i++) await client.enqueue(Task('noop', {})),
      ];
      expect(ids.toSet(), hasLength(100));
      // Same client, same prefix, so the counter suffix rises monotonically.
      final suffixes = ids.map((id) => int.parse(id.split('-').last)).toList();
      expect(suffixes, [for (var i = 1; i <= 100; i++) i]);
    } finally {
      await client.close();
    }
  });
}
