/// Shows why a handler has to be idempotent, and what that costs to write.
///
/// A worker can finish the work and then die before it records that it
/// finished. The task is not lost (see `crash_recovery.dart`), which means it
/// comes back, which means the work runs twice. That is what at-least-once
/// delivery is, and no queue can avoid it: the acknowledgement is a separate
/// step from the work, so something can always happen in between.
///
/// This runs the same crash twice, once with a handler that does not defend
/// itself and once with a handler that keys off [TaskContext.id]. The charge on
/// the invoice is applied twice in the first run and once in the second.
///
/// Needs a Redis reachable on localhost:6379.
///
///     dart run example/at_least_once.dart
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';

const _queue = 'idempotency-demo';
const _type = 'invoice:charge';
const _doneKey = 'rtq-demo:charged';

/// Where the "effect" lands, so it can be counted after the fact.
File _ledger() => File('${Directory.systemTemp.path}/rtq-ledger.txt');

Future<void> main(List<String> args) async {
  if (args.contains('--victim')) return _victim(args.contains('--dedup'));

  await _round(dedup: false);
  await _round(dedup: true);
}

Future<void> _round({required bool dedup}) async {
  final label =
      dedup ? 'a handler keyed off the task id' : 'a handler with no defence';
  print('== $label ==');

  _ledger().writeAsStringSync('');
  final redis = await RedisConnection().connect('localhost', 6379);
  await redis.send_object(['DEL', _doneKey]);

  final client = await QueueClient.connect();
  await client.enqueue(Task(_type, {'invoice': 'INV-4417'}), queue: _queue);

  // First run: the worker charges the invoice and dies before acknowledging,
  // so the task stays on its in-flight list.
  final victim = await Process.start(Platform.executable, [
    Platform.script.toFilePath(),
    '--victim',
    if (dedup) '--dedup',
  ]);
  unawaited(victim.stdout.transform(utf8.decoder).forEach(stdout.write));
  await victim.exitCode;

  // Second run: recovery hands the same task back, and it runs again.
  final replayed = Completer<void>();
  final worker = await Worker.connect(
    queues: {_queue: 1},
    workerId: 'idempotency-demo-worker',
  );
  worker.handle(_type, (task, context) async {
    await _charge(task, context, redis, dedup: dedup);
    if (!replayed.isCompleted) replayed.complete();
  });
  final loop = worker.run();
  await replayed.future.timeout(const Duration(seconds: 20));
  worker.stop();
  await loop;
  await worker.close();

  final charges = _ledger().readAsLinesSync().where((l) => l.isNotEmpty).length;
  print('   the task ran twice, and the invoice was charged $charges time'
      '${charges == 1 ? '' : 's'}\n');

  await redis.get_connection().close();
  await client.close();
}

/// The work, in both its careless and its careful form.
Future<void> _charge(
  Task task,
  TaskContext context,
  Command redis, {
  required bool dedup,
}) async {
  if (dedup) {
    // SADD reports whether the id was new. Two workers racing on the same task
    // cannot both get a 1, so exactly one of them proceeds.
    //
    // A marker in a separate key is the shape of the idea, not the whole of it:
    // this program can still be killed between the SADD and the charge. In a
    // real system the id goes in with the result, in one transaction, usually
    // as a unique constraint on a column. Then the duplicate does not need to
    // be detected, it simply cannot be written.
    final fresh = await redis.send_object(['SADD', _doneKey, context.id]);
    if (fresh == 0) {
      print('   attempt ${context.attempt}: already charged, skipping');
      return;
    }
  }
  _ledger().writeAsStringSync(
    'charged ${task.payload['invoice']}\n',
    mode: FileMode.append,
  );
  print('   attempt ${context.attempt}: charged ${task.payload['invoice']}');
}

/// Charges the invoice and then dies before the worker can acknowledge it, the
/// gap every at-least-once queue has.
Future<void> _victim(bool dedup) async {
  final redis = await RedisConnection().connect('localhost', 6379);
  final worker = await Worker.connect(
    queues: {_queue: 1},
    workerId: 'idempotency-demo-worker',
  );
  worker.handle(_type, (task, context) async {
    await _charge(task, context, redis, dedup: dedup);
    await stdout.flush();
    exit(137);
  });
  await worker.run();
}
