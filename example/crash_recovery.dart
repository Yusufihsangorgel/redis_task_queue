/// Proves the one thing worth knowing before a queue carries real work: when a
/// worker dies holding a task, is the task lost?
///
/// Nothing here is staged. The demo enqueues a task, hands it to a worker
/// running in a child process, and that child really does exit between
/// claiming the task and finishing it, with no chance to unwind, the way a
/// process goes down under SIGKILL or an out-of-memory kill. A worker then
/// starts again under the same id and the task runs to completion.
///
/// Needs a Redis reachable on localhost:6379.
///
///     dart run example/crash_recovery.dart
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:redis_task_queue/redis_task_queue.dart';

const _queue = 'crash-demo';
const _type = 'report:build';

/// The id the dying worker and its replacement share. Recovery is scoped to
/// one worker's own in-flight list, so the replacement only finds the orphan
/// because it starts under the same id. See this folder's README.
const _workerId = 'crash-demo-worker';

Future<void> main(List<String> args) async {
  if (args.contains('--victim')) return _victim();

  final client = await QueueClient.connect();
  final id = await client.enqueue(
    Task(_type, {'report': 'march-invoices'}),
    queue: _queue,
  );
  print('enqueued $id on "$_queue"\n');

  print('1. a worker claims the task, then dies holding it');
  final victim = await Process.start(Platform.executable, [
    Platform.script.toFilePath(),
    '--victim',
  ]);
  unawaited(victim.stdout.transform(utf8.decoder).forEach(stdout.write));
  unawaited(victim.stderr.transform(utf8.decoder).forEach(stderr.write));
  final code = await victim.exitCode;
  print('   the process is gone (exit $code) and the task never finished\n');

  print('2. the task is on no pending list, so nothing will poll it up,');
  print('   and no live worker holds it. It is parked on the in-flight list');
  print('   that belongs to "$_workerId", waiting for that worker to return.\n');

  print('3. a worker starts again under the same id');
  final ran = Completer<void>();
  final worker = await Worker.connect(
    queues: {_queue: 1},
    workerId: _workerId,
  );
  worker.handle(_type, (task, context) async {
    print('   recovered and ran ${task.payload['report']}');
    if (!ran.isCompleted) ran.complete();
  });
  final loop = worker.run();

  try {
    await ran.future.timeout(const Duration(seconds: 20));
    print('\nthe task survived a worker that died mid-flight.');
  } on TimeoutException {
    stderr.writeln('\nthe task was not recovered within 20s.');
    exitCode = 1;
  } finally {
    // stop() ends the poll loop; close() releases the connection, without which
    // the socket keeps the VM alive and the program never exits.
    worker.stop();
    await loop;
    await worker.close();
    await client.close();
  }
}

/// The worker that dies. It claims the task, says so, and then exits without
/// unwinding: no ack, no retry scheduled, no dead-letter entry, nothing that a
/// graceful shutdown would have done.
Future<void> _victim() async {
  final worker = await Worker.connect(
    queues: {_queue: 1},
    workerId: _workerId,
  );
  worker.handle(_type, (task, context) async {
    print('   [victim pid $pid] holding ${task.payload['report']}, dying now');
    await stdout.flush();
    // 137 is what a kernel OOM kill reports: 128 plus SIGKILL's 9.
    exit(137);
  });
  await worker.run();
}
