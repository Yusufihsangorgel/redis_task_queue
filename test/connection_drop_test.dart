import 'dart:async';
import 'dart:io';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';
import 'package:test/test.dart';

/// These tests need a Redis instance. Point REDIS_PORT at one (the CI/dev
/// default is 6399 so it won't clobber a local 6379).
const _host = 'localhost';
final _port = int.parse(
  String.fromEnvironment('REDIS_PORT', defaultValue: '6399'),
);
const _prefix = 'rtq_connection_drop_test';
const _workerId = 'test-worker';

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
  Duration timeout = const Duration(seconds: 8),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    if (await check()) return true;
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
  return false;
}

/// A minimal TCP proxy standing in for Redis' side of the connection, so this
/// suite can sever a connection on command.
///
/// A real Redis, shared with every other test file running concurrently
/// against the same `REDIS_PORT`, has no way to sever just *this* test's
/// connection without first identifying it among however many others happen
/// to be open at that instant, which is exactly the kind of cross-test race
/// this package's other suites are careful to avoid (see the comment on
/// `_flush` in the sibling test files). Routing through a proxy that this
/// test starts, uses exclusively, and stops sidesteps that: [sever] closes
/// every socket this proxy is holding open, which reaches [QueueClient] or
/// [Worker] exactly the way a real dropped connection would (the socket just
/// closes, mid-conversation, with no cooperation from the client side), while
/// the real Redis underneath it, and every other test file's connection to
/// it, is untouched.
class _Proxy {
  _Proxy._(this._server);

  final ServerSocket _server;
  final _sockets = <Socket>[];

  int get port => _server.port;

  static Future<_Proxy> start() async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final proxy = _Proxy._(server);
    server.listen(proxy._accept);
    return proxy;
  }

  Future<void> _accept(Socket client) async {
    Socket upstream;
    try {
      upstream = await Socket.connect(_host, _port);
    } catch (_) {
      client.destroy();
      return;
    }
    _sockets.addAll([client, upstream]);
    client.listen(
      upstream.add,
      onDone: upstream.destroy,
      onError: (_) => upstream.destroy(),
    );
    upstream.listen(
      client.add,
      onDone: client.destroy,
      onError: (_) => client.destroy(),
    );
  }

  /// Closes every socket currently proxied. A client reconnecting afterwards
  /// still finds this proxy listening and gets a fresh pair piped straight
  /// through to the same, healthy, real Redis.
  void sever() {
    for (final socket in _sockets) {
      socket.destroy();
    }
    _sockets.clear();
  }

  Future<void> stop() async {
    sever();
    await _server.close();
  }
}

void main() {
  setUp(_flush);

  test('QueueClient.enqueue recovers after its connection is severed',
      () async {
    final proxy = await _Proxy.start();
    final client = await QueueClient.connect(
      host: 'localhost',
      port: proxy.port,
      prefix: _prefix,
    );

    // Baseline: the client works before anything is severed.
    final id1 = await client.enqueue(Task('probe', {'n': 1}));
    expect(id1, isNotEmpty);

    // Sever the connection. README.md recommends keeping one client around
    // for the app's lifetime and reusing it; that reused client must recover
    // on its own rather than fail every call from here on.
    proxy.sever();

    final id2 = await client.enqueue(Task('probe', {'n': 2}));
    expect(id2, isNotEmpty);
    expect(id2, isNot(id1));

    await client.close();
    await proxy.stop();
  });

  test('Worker.run() survives its connection being severed mid-loop', () async {
    final proxy = await _Proxy.start();
    final client = await QueueClient.connect(
      host: 'localhost',
      port: proxy.port,
      prefix: _prefix,
    );
    final worker = await Worker.connect(
      host: 'localhost',
      port: proxy.port,
      prefix: _prefix,
      queues: {'default': 1},
      workerId: _workerId,
      // Short, so the reconnect backoff in the poll loop doesn't stretch this
      // test out; correctness doesn't depend on the exact duration.
      backoffBase: const Duration(milliseconds: 100),
    );

    final processed = <int>[];
    worker.handle('probe', (task, _) {
      processed.add(task.payload['n'] as int);
    });
    final loop = worker.run();

    await client.enqueue(Task('probe', {'n': 1}));
    final gotFirst = await _pollUntil(() => processed.contains(1));
    expect(gotFirst, isTrue,
        reason: 'baseline: the worker must process a task before its '
            'connection is severed');

    // Sever both connections at once, the same way a real Redis restart or
    // failover would drop every client simultaneously, not just one. Before
    // the fix this either crashed run() outright with an unhandled "stream is
    // closed" error, or, if a caller wrapped run() in a try/catch of its own,
    // left the worker stuck forever, never processing another task even once
    // Redis was healthy again.
    proxy.sever();

    final id2 = await client.enqueue(Task('probe', {'n': 2}));
    expect(id2, isNotEmpty); // the client survives the same drop too

    final gotSecond = await _pollUntil(
      () => processed.contains(2),
      timeout: const Duration(seconds: 5),
    );
    expect(gotSecond, isTrue,
        reason: 'the worker must keep processing after its connection is '
            'severed, not die or get stuck');

    worker.stop();
    // If run() had died from the severed connection, awaiting it here
    // rethrows that error instead of completing normally.
    await loop.timeout(const Duration(seconds: 3));
    await client.close();
    await worker.close();
    await proxy.stop();
  });
}
