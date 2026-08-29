/// Answers the third question to put to a queue before it carries work you
/// care about: what happens to the work that never succeeds?
///
/// Retries end somewhere. A task that keeps throwing runs out of attempts and
/// moves to the dead-letter list, where it then waits: nothing on that list
/// expires, retries itself, or pages anyone. This program fails three webhook
/// deliveries for good and then does the triage: read what died and why, put
/// back the one whose cause has been fixed, and clear the two that never can.
/// It prints the queue depth after every step, so the backlog is visible
/// moving rather than described.
///
/// Needs a Redis reachable on localhost:6379.
///
///     docker run --rm -p 6379:6379 redis:7
///     dart run example/triage.dart
///
/// Set `REDIS_PORT` if yours is somewhere else; the test suite uses 6399.
library;

import 'dart:async';
import 'dart:io';

import 'package:redis/redis.dart';
import 'package:redis_task_queue/redis_task_queue.dart';

import '_redis.dart';

const _queue = 'webhooks';
const _type = 'webhook:deliver';

/// This demo keeps to its own key namespace.
///
/// Two of the four numbers printed below are counted across the whole prefix
/// rather than per queue: the dead-letter list is one list shared by every
/// queue, and the in-flight figure sums every worker's list. Running on the
/// default `rtq` prefix would fold the other examples' leftovers into this
/// run's readings. Namespacing is also how a real deployment lets a queue share
/// one Redis with everything else.
const _prefix = 'rtq-triage-demo';

/// Stable across restarts and unique to this worker: the two properties a
/// `workerId` has to have at once. The worker below is closed and started again
/// after the fix, deliberately under the same id — recovery is scoped to a
/// worker's own in-flight list, so an id that changed on restart would strand
/// whatever was left on the old one.
const _workerId = 'triage-demo-worker';

/// The webhook signing key configured per customer, held by key id rather than
/// by secret: a log line should never carry the secret itself.
///
/// `initech` is missing one, and that is the bug this run is about. Its
/// delivery fails, exhausts its retries and dead-letters; adding the key is the
/// fix that makes replaying it worth doing.
final _signingKeys = <String, String>{'acme': 'key-1', 'globex': 'key-2'};

/// Customers who deleted their endpoint. A 410 is not a blip — no number of
/// retries turns it into a 200 — which is what makes these two a purge rather
/// than a replay.
const _deletedEndpoints = {'acme', 'globex'};

Future<void> main() async {
  await requireRedis();
  await _clearDemoKeys();

  final client = await QueueClient.connect(port: redisPort, prefix: _prefix);

  print('three webhook deliveries go on the queue');
  final ids = <String, String>{};
  for (final customer in _deletedEndpoints.followedBy(['initech'])) {
    ids[customer] = await client.enqueue(
      Task(_type, {
        'customer': customer,
        'url': 'https://$customer.example/hooks/orders',
      }),
      queue: _queue,
      // Two attempts each: the first run plus one retry. Deliberately small so
      // the run is short; a real budget is larger, and the arithmetic is the
      // same either way (attempts == maxRetries + 1).
      maxRetries: 1,
    );
    print('   ${customer.padRight(8)} ${ids[customer]}');
  }
  await _depth(client, 'queued');

  print('\n1. a worker runs them. Every attempt throws, so every one gives up');
  final allDead = Completer<void>();
  var gaveUp = 0;
  var worker = await _worker(
    // onDeadLetter is the alerting seam: it fires once per task, on the
    // transition into the dead-letter list, which is the moment a human needs
    // to hear about. Without it the give-up is silent. `context.id` is in scope
    // here too, so a page can name the job somebody has to go and find.
    onDeadLetter: (task, context, error, stackTrace) {
      gaveUp++;
      print(
        '   gave up on ${task.payload['customer']} after '
        '${context.attempt} attempts',
      );
      if (gaveUp == 3 && !allDead.isCompleted) allDead.complete();
    },
  );
  worker.handle(_type, _deliver);
  var loop = worker.run();

  try {
    await allDead.future.timeout(const Duration(seconds: 30));
  } on TimeoutException {
    stderr.writeln('\nthe three tasks did not dead-letter within 30s.');
    exitCode = 1;
    worker.stop();
    await loop;
    await worker.close();
    await client.close();
    return;
  }
  // Stop before measuring: with the worker parked, nothing can be claimed
  // between the four counts below, so they describe one moment rather than
  // four.
  worker.stop();
  await loop;
  await worker.close();
  await _depth(client, 'gave up');

  print('\n2. the dead-letter list is what is left. Read it before deciding');
  for (final dead in await client.deadLetters()) {
    // deadAt is null only for an entry written before 0.8.0, which stored the
    // envelope with no failure attached. Anything this worker writes has both.
    // It comes back as a local DateTime, so it prints without a zone suffix.
    final at = dead.deadAt?.toIso8601String() ?? 'unknown (pre-0.8.0 entry)';
    print(
      '   ${(dead.task.payload['customer'] as String).padRight(8)} '
      '${dead.id}',
    );
    print('      ${dead.attempts} attempts, died $at');
    print('      ${dead.error}');
  }

  print(
    '\n3. two of those are 410 Gone: the endpoint is deleted, so replaying',
  );
  print('   them is a round trip back to this list. The third is our own');
  print('   missing config. Fix that, then put the one task back.');
  _signingKeys['initech'] = 'key-3';
  final replayed = await client.replayDeadLetter(ids['initech']!);
  print('   replayDeadLetter -> $replayed');
  await _depth(client, 'replayed');
  print('   one call moved it: off the dead list and onto the queue inside a');
  print('   single Lua step. Doing it by hand is two commands with a gap in');
  print('   the middle, and a crash in that gap either loses the task or');
  print('   enqueues it twice.');

  print('\n4. a worker starts again, and this time the delivery goes through');
  final delivered = Completer<void>();
  worker = await _worker();
  worker.handle(_type, (task, context) {
    _deliver(task, context);
    if (!delivered.isCompleted) delivered.complete();
  });
  loop = worker.run();
  try {
    await delivered.future.timeout(const Duration(seconds: 30));
  } on TimeoutException {
    stderr.writeln('\nthe replayed task did not run within 30s.');
    exitCode = 1;
  }
  worker.stop();
  await loop;
  await worker.close();
  await _depth(client, 'delivered');

  print('\n5. the two that can never succeed are cleared');
  // purgeDeadLetters empties the list; it does not remove the entries you were
  // just looking at. Anything another queue dead-lettered while you were
  // reading goes with them, and the count it returns is the length it saw, so
  // purge when the list is quiet and read before you purge.
  print('   purgeDeadLetters -> ${await client.purgeDeadLetters()} entries');
  await _depth(client, 'purged');

  print(
    '\nwhat never succeeds is not lost. It is parked where a human can '
    'read it,\nand putting one back is one call.',
  );
  await client.close();
}

/// The delivery, such as it is. Throwing is how a handler reports failure: the
/// worker retries until the attempts run out, then dead-letters the task with
/// the error text this throw produced, which is what step 2 reads back.
void _deliver(Task task, TaskContext context) {
  final customer = task.payload['customer'] as String;
  final url = task.payload['url'] as String;
  if (_deletedEndpoints.contains(customer)) {
    throw StateError('POST $url -> 410 Gone');
  }
  final key = _signingKeys[customer];
  if (key == null) {
    throw StateError(
      'no signing key for "$customer"; refusing to POST $url unsigned',
    );
  }
  print('   delivered to $customer (signed with $key)');
}

Future<Worker> _worker({TaskDeadLetterCallback? onDeadLetter}) =>
    Worker.connect(
      port: redisPort,
      prefix: _prefix,
      workerId: _workerId,
      queues: {_queue: 1},
      // The default backoff starts at a second and doubles, which would make
      // this program sit for longer than it is worth watching. Production wants
      // the default: it is what stops a failing dependency being hammered.
      backoffBase: const Duration(milliseconds: 50),
      backoffCap: const Duration(milliseconds: 200),
      // Jitter off, only so this program prints the same thing twice. The
      // default is 0.1, and you want it: it spreads a burst of simultaneous
      // failures out instead of letting them all re-fire on the same tick. It
      // is also visible here if you leave it on — the three tasks stop giving
      // up in the order they were queued, because their retries come due in
      // whatever order the random offsets landed.
      backoffJitter: 0,
      onDeadLetter: onDeadLetter,
    );

/// Prints how deep the queue is right now, one line per checkpoint.
///
/// `stats(queues: [...])` names the queue instead of discovering it. Discovery
/// (`stats()` with no argument) `SCAN`s for queue keys, and Redis deletes a
/// list once it empties, so a queue whose last task is in flight has no key
/// left to find and drops out of the report. A named queue reports zero rather
/// than disappearing, which is what a fixed dashboard needs. Either way the
/// call reads counters, not tasks, so it is cheap enough to poll.
///
/// See [QueueStats] for what the four numbers mean.
Future<void> _depth(QueueClient client, String label) async {
  final depth = await client.stats(queues: const [_queue]);
  print(
    '   [$label] pending ${depth.totalPending} · '
    'delayed ${depth.totalDelayed} · in flight ${depth.inFlight} · '
    'dead ${depth.deadLetter}',
  );
}

/// Deletes this demo's keys so a re-run starts where a first run does.
///
/// `SCAN` rather than `KEYS`, for the reason `stats()` gives: it walks the
/// keyspace in bounded pages instead of blocking Redis while it builds one big
/// answer. The pattern is scoped to [_prefix], so nothing else in this Redis is
/// touched.
Future<void> _clearDemoKeys() async {
  final redis = await RedisConnection().connect(redisHost, redisPort);
  final keys = <String>[];
  var cursor = '0';
  do {
    final page = await redis.send_object([
      'SCAN',
      cursor,
      'MATCH',
      '$_prefix:*',
      'COUNT',
      '100',
    ]) as List;
    cursor = page[0] as String;
    keys.addAll((page[1] as List).cast<String>());
  } while (cursor != '0');
  if (keys.isNotEmpty) await redis.send_object(['DEL', ...keys]);
  await redis.get_connection().close();
}
