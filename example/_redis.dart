/// Connection settings shared by the example programs.
///
/// One place, because each program opens more than one connection: a client
/// and a worker at least, plus a raw `RedisConnection` in the two that set up
/// their own keys. Sending some of those to `REDIS_PORT` and the rest to the
/// default is how a demo half-works, which is harder to read than one that
/// does not work at all.
library;

import 'dart:io';

/// Where the examples look for Redis.
const redisHost = 'localhost';

/// The port they connect to, `REDIS_PORT` first.
///
/// The default is 6379, the library's own. The test suite deliberately uses
/// 6399 so `dart test` cannot touch a Redis you are already using, so a
/// container started by following "Running the tests" needs `REDIS_PORT=6399`
/// to be reachable from here.
final redisPort = int.parse(Platform.environment['REDIS_PORT'] ?? '6379');

/// Stops with the command that fixes it, rather than an unhandled
/// `SocketException` and a stack trace nobody needs to read.
Future<void> requireRedis() async {
  try {
    final socket = await Socket.connect(
      redisHost,
      redisPort,
      timeout: const Duration(seconds: 2),
    );
    socket.destroy();
  } on SocketException {
    stderr.writeln(
      'This example needs a Redis listening on $redisHost:$redisPort.\n'
      '  docker run --rm -p $redisPort:6379 redis:7\n'
      'Set REDIS_PORT if yours is somewhere else (the tests use 6399).',
    );
    exit(69); // EX_UNAVAILABLE
  }
}
