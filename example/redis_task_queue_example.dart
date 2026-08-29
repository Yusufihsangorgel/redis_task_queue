import 'package:redis_task_queue/redis_task_queue.dart';

import '_redis.dart';

/// Run a Redis instance, then start this in two terminals:
///   dart run example/redis_task_queue_example.dart worker
///   dart run example/redis_task_queue_example.dart enqueue
///
/// This connects to 6379, the library default. The test suite uses 6399 so a
/// run cannot touch a Redis you are already using, so if you started a
/// container by following "Running the tests" it is on the wrong port for this.
/// Set `REDIS_PORT=6399` to point the example at that one.
Future<void> main(List<String> args) async {
  final mode = args.isEmpty ? 'enqueue' : args.first;
  await requireRedis();

  if (mode == 'worker') {
    final worker = await Worker.connect(
      port: redisPort,
      workerId: 'example-worker',
    );
    worker.handle('email:welcome', (task, context) async {
      // Real work goes here. Throw to trigger a retry; return to mark it done.
      //
      // `context.id` is the same on every attempt, so it is what to record
      // against the effect to keep a repeat from sending the mail twice. See
      // at_least_once.dart for why a repeat is not hypothetical.
      print(
        'attempt ${context.attempt} of ${context.maxAttempts}: '
        'sending welcome email for user ${task.payload['user_id']} '
        '(task ${context.id})',
      );
    });
    print('worker running (Ctrl-C to stop)');
    await worker.run();
    return;
  }

  final client = await QueueClient.connect(port: redisPort);
  final id = await client.enqueue(
    Task('email:welcome', {'user_id': '42'}),
    queue: 'default',
    maxRetries: 5,
  );
  print('enqueued task $id');
  await client.close();
}
