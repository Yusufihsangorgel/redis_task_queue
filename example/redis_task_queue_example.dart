import 'package:redis_task_queue/redis_task_queue.dart';

/// Run a Redis instance, then start this in two terminals:
///   dart run example/redis_task_queue_example.dart worker
///   dart run example/redis_task_queue_example.dart enqueue
Future<void> main(List<String> args) async {
  final mode = args.isEmpty ? 'enqueue' : args.first;

  if (mode == 'worker') {
    final worker = await Worker.connect();
    worker.handle('email:welcome', (task, context) async {
      // Real work goes here. Throw to trigger a retry; return to mark it done.
      //
      // `context.id` is the same on every attempt, so it is what to record
      // against the effect to keep a repeat from sending the mail twice. See
      // at_least_once.dart for why a repeat is not hypothetical.
      print('attempt ${context.attempt} of ${context.maxAttempts}: '
          'sending welcome email for user ${task.payload['user_id']} '
          '(task ${context.id})');
    });
    print('worker running (Ctrl-C to stop)');
    await worker.run();
    return;
  }

  final client = await QueueClient.connect();
  final id = await client.enqueue(
    Task('email:welcome', {'user_id': '42'}),
    queue: 'default',
    maxRetries: 5,
  );
  print('enqueued task $id');
  await client.close();
}
