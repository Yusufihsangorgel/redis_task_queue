import 'package:redis_task_queue/redis_task_queue.dart';

/// Run a Redis instance, then start this in two terminals:
///   dart run example/redis_task_queue_example.dart worker
///   dart run example/redis_task_queue_example.dart enqueue
Future<void> main(List<String> args) async {
  final mode = args.isEmpty ? 'enqueue' : args.first;

  if (mode == 'worker') {
    final worker = await Worker.connect();
    worker.handle('email:welcome', (task) async {
      // Real work goes here. Throw to trigger a retry.
      print('sending welcome email for user ${task.payload['user_id']}');
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
