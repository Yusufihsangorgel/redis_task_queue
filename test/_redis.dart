import 'dart:io';

import 'package:test/test.dart';

/// Shared connection settings for the integration tests.
///
/// The tests talk to a real Redis rather than a fake, so they need one
/// listening. The default port is 6399, not 6379, so a run cannot touch a
/// Redis you are already using.
const redisHost = 'localhost';

/// The port the tests connect to.
///
/// `REDIS_PORT` is read from the environment first, which is what every test
/// file's doc comment has claimed. `String.fromEnvironment` on its own did not
/// do that — it reads a compile-time `--define`, so `REDIS_PORT=7777 dart test`
/// was silently ignored and the run went to 6399 anyway. Both spellings work
/// now, with the environment winning.
final redisPort = int.parse(
  Platform.environment['REDIS_PORT'] ??
      const String.fromEnvironment('REDIS_PORT', defaultValue: '6399'),
);

/// Stops the suite once, with the command that fixes it.
///
/// Without this every test fails on its own `SocketException: Connection
/// refused`, which buries the one thing the reader needs to know.
void requireRedis() {
  setUpAll(() async {
    try {
      final socket = await Socket.connect(
        redisHost,
        redisPort,
        timeout: const Duration(seconds: 2),
      );
      socket.destroy();
    } on SocketException {
      throw StateError(
        'These tests need a Redis listening on $redisHost:$redisPort.\n'
        '  docker run --rm -p $redisPort:6379 redis:7\n'
        'Set REDIS_PORT to point at a different one.',
      );
    }
  });
}
