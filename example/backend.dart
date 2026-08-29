/// How this package sits in a Dart HTTP backend.
///
/// The type signatures do not say it. A reader arriving from dart_frog or
/// shelf has to work out three things: the [QueueClient] lives as long as
/// the process, not the request; the worker is a separate process, not a
/// route; and SIGTERM has to wait for the task in flight rather than
/// abandoning it to recovery.
///
/// This file is that wiring. `serve` and `worker` are the two entry points
/// you copy. `dart:io`'s [HttpServer] stands in for the framework so this
/// package does not grow a dart_frog or shelf dependency; the comments say
/// how the same shape maps onto either.
///
///     dart run example/backend.dart serve     # terminal 1
///     dart run example/backend.dart worker    # terminal 2
///     curl -X POST http://127.0.0.1:8080/jobs
///
/// With no args it starts both, POSTs a job, then SIGTERMs the worker
/// while the handler is still running, so the drain is visible.
///
/// Needs a Redis reachable on localhost:6379.
///
///     docker run --rm -p 6379:6379 redis:7
///     dart run example/backend.dart
///
/// Set `REDIS_PORT` if yours is somewhere else; the test suite uses 6399.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';

import '_redis.dart';

/// Own keyspace so a run does not fold in leftover `rtq` keys from the
/// other examples.
const _prefix = 'rtq-backend';

const _queue = 'default';
const _type = 'email:welcome';

/// Stable across restarts, unique per worker. A pod name in production;
/// a UUID minted at startup would strand the previous in-flight list.
const _workerId = 'backend-example-worker';

Future<void> main(List<String> args) async {
  await requireRedis();
  final mode = args.isEmpty ? 'demo' : args.first;

  if (mode == 'worker') {
    await _worker(hold: args.contains('--hold'));
    return;
  }
  if (mode == 'serve') {
    await _serve();
    return;
  }
  if (mode != 'demo') {
    stderr.writeln(
      'usage: dart run example/backend.dart [serve | worker | demo]',
    );
    exit(64);
  }
  await _demo();
}

/// The HTTP process. One [QueueClient] for the process lifetime; the
/// handler only calls [QueueClient.enqueue].
///
/// dart_frog: construct the client in `init` on a custom entrypoint, put
/// the same instance on `RequestContext` from middleware, enqueue from
/// the route. `QueueClient.connect` inside a route — or inside middleware
/// that runs per request — is the mistake this file exists to prevent.
///
/// shelf: construct it next to `shelf_io.serve` and close it when the
/// process is asked to stop. The `Handler` only calls `enqueue`.
Future<void> _serve() async {
  // connect() is a TCP handshake and a socket that has to be closed.
  // enqueue is a single LPUSH. A client per request pays that handshake
  // on every POST, and if the handler returns without close() the socket
  // leaks and can keep the isolate alive. Reconnect-on-drop also lives
  // on this instance: a fresh client per request cannot reuse it.
  final client = await QueueClient.connect(
    port: redisPort,
    prefix: _prefix,
  );
  final port = int.parse(Platform.environment['PORT'] ?? '8080');
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
  server.listen((request) {
    unawaited(_handle(request, client));
  });
  print('listening on http://${server.address.host}:${server.port}');
  await stdout.flush();

  await _untilStopSignal();
  // In-flight POSTs finish so the caller still gets the 202 and the id.
  await server.close(force: false);
  await client.close();
}

Future<void> _handle(HttpRequest request, QueueClient client) async {
  final response = request.response;
  try {
    if (request.method != 'POST' || request.uri.path != '/jobs') {
      response.statusCode = HttpStatus.notFound;
      return;
    }
    final raw = await utf8.decoder.bind(request).join();
    final Map<String, dynamic> payload;
    try {
      payload = raw.isEmpty
          ? <String, dynamic>{}
          : jsonDecode(raw) as Map<String, dynamic>;
    } on FormatException {
      response.statusCode = HttpStatus.badRequest;
      return;
    } on TypeError {
      response.statusCode = HttpStatus.badRequest;
      return;
    }
    final id = await client.enqueue(
      Task(_type, {'user_id': '${payload['user_id'] ?? '42'}'}),
      queue: _queue,
    );
    response.statusCode = HttpStatus.accepted;
    response.write(id);
    print('enqueued $id');
  } on TaskQueueConnectionException {
    // Redis is down (the call already retried once). Do not claim the
    // job was queued; the request path has to say so.
    response.statusCode = HttpStatus.serviceUnavailable;
    response.write('queue unavailable');
  } finally {
    await response.close();
  }
}

/// The worker process. A second executable (`bin/worker.dart` in a
/// dart_frog or shelf app), not a route and not an isolate started from
/// the server. The two processes agree on the type string and the payload
/// shape, nothing else.
///
/// [hold] is demo-only: a delay so the parent can SIGTERM mid-handler.
Future<void> _worker({required bool hold}) async {
  final worker = await Worker.connect(
    port: redisPort,
    prefix: _prefix,
    workerId: Platform.environment['WORKER_ID'] ?? _workerId,
    // Only queues listed here are polled. A task on any other name sits
    // pending forever. Start with `default` if you only have one.
    queues: const {'critical': 6, 'default': 3, 'low': 1},
  );
  worker.handle(_type, (task, context) async {
    // context.id is the same on every attempt and after crash recovery.
    // Key the effect on it so a redelivery is a no-op; see at_least_once.dart.
    print('holding ${context.id}');
    await stdout.flush();
    if (hold) {
      await Future<void>.delayed(const Duration(milliseconds: 800));
    }
    print(
      'sent welcome email for user ${task.payload['user_id']} '
      '(task ${context.id})',
    );
  });

  // stop() sets a flag; the poll loop will not start another sweep after
  // the current iteration. A handler already running is awaited, then
  // the envelope leaves the in-flight list the normal way (LREM on
  // success, the retry script, or the dead-letter script). If the worker
  // is idle inside the 1s BLMOVE, that wait finishes first; a task that
  // arrives in that window still runs, because it is already on the
  // in-flight list — finishing it is the clean end, not leaving it for
  // recovery. The pending queue is not drained.
  //
  // Operators: SIGTERM and wait. Size the deploy grace period for the
  // slowest handler, plus about a second, plus that handler again if a
  // blocking claim wins the race. SIGKILL, or a grace period that
  // expires, leaves the envelope on inflight:$workerId; the next run
  // under the same id requeues it without incrementing attempt. That is
  // crash_recovery.dart, not this path. Handlers still have to be
  // idempotent.
  //
  // Watching the signal takes over from the VM's default terminate, so
  // something here has to call stop() or the process never leaves.
  // close() awaits stop(), releases the worker id, and closes the Redis
  // socket. Skipping it can keep the isolate alive. close() is terminal:
  // run() afterwards throws StateError.
  var stopping = false;
  void requestStop(ProcessSignal signal) {
    if (stopping) return;
    stopping = true;
    print('stop(): waiting for the task in flight ($signal)');
    unawaited(stdout.flush());
    unawaited(worker.stop());
  }

  final signals = _onStopSignals(requestStop);
  print('worker running (SIGTERM to stop)');
  await stdout.flush();
  await worker.run();
  await worker.close();
  // A live signal watch keeps the isolate alive; cancel it or the
  // process never leaves after run() returns.
  await _cancelSignals(signals);
}

/// Starts both processes, POSTs a job, SIGTERMs the worker while the
/// handler is still in its delay, and prints that the handler finished
/// before the process exited.
Future<void> _demo() async {
  await _flush();

  print('two processes, the way this sits in a Dart HTTP backend\n');

  print('1. the worker is its own process, not a route');
  final worker = await _spawn(['worker', '--hold']);
  final workerRunning = Completer<void>();
  final workerHolding = Completer<void>();
  final workerFinished = Completer<void>();
  final workerStopping = Completer<void>();
  _pipe(worker, 'worker', (line) {
    if (line.startsWith('worker running') && !workerRunning.isCompleted) {
      workerRunning.complete();
    }
    if (line.startsWith('holding ') && !workerHolding.isCompleted) {
      workerHolding.complete();
    }
    if (line.startsWith('sent welcome email') && !workerFinished.isCompleted) {
      workerFinished.complete();
    }
    if (line.startsWith('stop():') && !workerStopping.isCompleted) {
      workerStopping.complete();
    }
  });
  try {
    await workerRunning.future.timeout(_demoTimeout);
  } on TimeoutException {
    stderr.writeln('the worker did not start within 20s.');
    worker.kill(ProcessSignal.sigkill);
    exit(1);
  }

  print('\n2. the HTTP process holds one QueueClient for its lifetime');
  final serve = await _spawn(['serve'], environment: const {'PORT': '0'});
  final listening = Completer<int>();
  _pipe(serve, 'serve', (line) {
    final match = RegExp(r'listening on http://[^:]+:(\d+)').firstMatch(line);
    if (match != null && !listening.isCompleted) {
      listening.complete(int.parse(match.group(1)!));
    }
  });
  final int port;
  try {
    port = await listening.future.timeout(_demoTimeout);
  } on TimeoutException {
    stderr.writeln('the HTTP process did not listen within 20s.');
    worker.kill(ProcessSignal.sigkill);
    serve.kill(ProcessSignal.sigkill);
    exit(1);
  }

  print(
      '\n3. a request enqueues and returns; the handler does not do the work');
  final id = await _postJob(port);
  print('   POST /jobs -> 202 $id');

  try {
    await workerHolding.future.timeout(_demoTimeout);
  } on TimeoutException {
    stderr.writeln('the worker did not claim the task within 20s.');
    worker.kill(ProcessSignal.sigkill);
    serve.kill(ProcessSignal.sigkill);
    exit(1);
  }

  print('\n4. SIGTERM arrives while the handler is still running');
  // Inside the 800ms hold, so stop() has something in flight to wait for.
  await Future<void>.delayed(const Duration(milliseconds: 50));
  worker.kill(ProcessSignal.sigterm);
  print('   SIGTERM sent to worker pid ${worker.pid}');

  try {
    await workerStopping.future.timeout(_demoTimeout);
    await workerFinished.future.timeout(_demoTimeout);
    final code = await worker.exitCode.timeout(_demoTimeout);
    print('   worker exited $code after finishing the task');
    if (code != 0) {
      stderr.writeln('worker was expected to exit 0 after stop().');
      serve.kill(ProcessSignal.sigkill);
      exit(1);
    }
  } on TimeoutException {
    stderr.writeln(
      'the worker did not finish the in-flight task and exit within 20s.',
    );
    worker.kill(ProcessSignal.sigkill);
    serve.kill(ProcessSignal.sigkill);
    exit(1);
  }

  serve.kill(ProcessSignal.sigterm);
  final serveCode = await serve.exitCode.timeout(
    _demoTimeout,
    onTimeout: () {
      serve.kill(ProcessSignal.sigkill);
      return -1;
    },
  );
  if (serveCode != 0) {
    stderr.writeln('the HTTP process exited $serveCode, expected 0.');
    exit(1);
  }

  print('\nthe in-flight task was not abandoned. SIGKILL would have been');
  print('crash recovery; this was a drain.');
}

const _demoTimeout = Duration(seconds: 20);

Future<Process> _spawn(
  List<String> args, {
  Map<String, String>? environment,
}) {
  return Process.start(
    Platform.executable,
    [Platform.script.toFilePath(), ...args],
    environment: environment,
  );
}

void _pipe(
  Process process,
  String name,
  void Function(String line) onLine,
) {
  unawaited(process.stderr.transform(utf8.decoder).forEach(stderr.write));
  unawaited(
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .forEach((line) {
      print('   [$name] $line');
      onLine(line);
    }),
  );
}

Future<String> _postJob(int port) async {
  final http = HttpClient();
  try {
    final request = await http.postUrl(
      Uri.parse('http://127.0.0.1:$port/jobs'),
    );
    request.headers.contentType = ContentType.json;
    request.write('{"user_id":"42"}');
    final response = await request.close();
    final body = await utf8.decoder.bind(response).join();
    if (response.statusCode != HttpStatus.accepted) {
      throw StateError('POST /jobs -> ${response.statusCode} $body');
    }
    return body;
  } finally {
    http.close(force: true);
  }
}

List<StreamSubscription<ProcessSignal>> _onStopSignals(
  void Function(ProcessSignal) callback,
) {
  final subs = <StreamSubscription<ProcessSignal>>[
    ProcessSignal.sigint.watch().listen(callback),
  ];
  if (!Platform.isWindows) {
    subs.add(ProcessSignal.sigterm.watch().listen(callback));
  }
  return subs;
}

Future<void> _untilStopSignal() async {
  final done = Completer<void>();
  final subs = _onStopSignals((_) {
    if (!done.isCompleted) done.complete();
  });
  await done.future;
  await _cancelSignals(subs);
}

Future<void> _cancelSignals(
  List<StreamSubscription<ProcessSignal>> subscriptions,
) async {
  for (final sub in subscriptions) {
    await sub.cancel();
  }
}

Future<void> _flush() async {
  final cmd = await RedisConnection().connect(redisHost, redisPort);
  final keys = await cmd.send_object(['KEYS', '$_prefix:*']) as List;
  if (keys.isNotEmpty) {
    await cmd.send_object(['DEL', ...keys.cast<String>()]);
  }
  await cmd.get_connection().close();
}
