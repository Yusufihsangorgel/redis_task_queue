// What a handler sees across one task's whole life, with no Redis running.
//
//   dart run example/no_redis_demo.dart
//
// Every other example here needs a server, and a reader with nothing installed
// gets four exit-69 messages and no idea what the package does. The part they
// need to understand before installing anything is not the storage. It is the
// contract the handler is written against, and that is a value type.
//
// The lesson worth having is the one that costs money when it is missed:
// at-least-once means a handler can run twice for the same task, so it has to
// be safe to run twice. The queue cannot make that true for you. What it gives
// you is `TaskContext.id`, which is the same on every attempt, and that is the
// key to write against.
import 'package:redis_task_queue/redis_task_queue.dart';

/// Stands in for the store the real handler would write to.
///
/// Keyed by task id rather than by attempt, which is the whole point: the
/// second run of a task finds the first run's work already there.
final charged = <String, int>{};

/// A handler that is safe to run twice, because it checks first.
///
/// Without the `containsKey` this is the classic double charge: the work
/// succeeded, the worker died before recording it, and the replacement did it
/// again.
Future<void> chargeOnce(Task task, TaskContext context) async {
  if (charged.containsKey(context.id)) {
    print('      already charged on an earlier attempt, nothing to do');
    return;
  }
  charged[context.id] = task.payload['cents']! as int;
  print('      charged ${task.payload['cents']} cents');
}

void describe(TaskContext context) {
  print(
    '  attempt ${context.attempt} of ${context.maxAttempts}'
    '${context.isLastAttempt ? '  <- last one' : ''}',
  );
  print('      id ${context.id} (the same on every attempt)');
}

Future<void> main() async {
  final task = Task('charge-card', {'cents': 2500, 'order': 'A-118'});

  // `maxRetries: 2` means three runs in all: the first plus two retries. The
  // off-by-one is worth seeing rather than reading, because a handler that
  // treats `maxAttempts` as the retry count writes its last-ditch path one
  // attempt too early.
  const maxRetries = 2;
  const id = 'task-7f3a';

  print('');
  print('A task enqueued with maxRetries: $maxRetries');
  print('The worker dies after the charge but before recording it, twice.');
  print('');

  for (var attempt = 1; attempt <= maxRetries + 1; attempt++) {
    final context = TaskContext(
      id: id,
      queue: 'payments',
      attempt: attempt,
      maxAttempts: maxRetries + 1,
    );
    describe(context);
    await chargeOnce(task, context);

    if (attempt <= maxRetries) {
      // Dying here is the case at-least-once exists for: the work happened,
      // and nothing recorded that it did.
      print('      worker dies before acknowledging; the task goes back');
    } else {
      print('      acknowledged');
    }
    print('');
  }

  print('charged once, across three runs: $charged');
  print('');
  print('If chargeOnce had not checked, this would read 7500 cents against an');
  print('order for 2500. The queue redelivers; the handler decides whether');
  print('that is safe. TaskContext.id is what makes it decidable.');
  print('');
  print('The other examples in this directory do all of the above against a');
  print('real Redis. They print the command to start one when it is missing:');
  print('  dart run example/crash_recovery.dart');
}
