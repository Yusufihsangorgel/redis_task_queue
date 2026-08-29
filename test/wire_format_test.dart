import 'dart:convert';

import 'package:redis_task_queue/src/task.dart';
import 'package:test/test.dart';

/// What goes into Redis and what comes back out.
///
/// Every other test here needs a Redis listening, so the format itself was
/// only ever exercised in passing, as a side effect of a job completing. That
/// hides two things. A round trip that quietly drops a field still looks like
/// a job that ran. And `DeadLetter.decode` promises to read entries written
/// before 0.8.0, a promise to anyone with an old dead-letter list, which
/// nothing checked at all.
///
/// These need no Redis, so they run in any CI.
void main() {
  Envelope anEnvelope({
    String id = 'task-1',
    String type = 'send-email',
    Map<String, dynamic>? payload,
    String queue = 'default',
    int maxRetries = 3,
    int attempt = 0,
  }) => Envelope(
    id: id,
    task: Task(type, payload ?? {'to': 'someone@example.com'}),
    queue: queue,
    maxRetries: maxRetries,
    attempt: attempt,
  );

  group('Envelope round trip', () {
    test('keeps every field', () {
      final before = anEnvelope(attempt: 2, maxRetries: 7, queue: 'mail');
      final after = Envelope.decode(before.encode());

      expect(after.id, before.id);
      expect(after.queue, 'mail');
      expect(after.maxRetries, 7);
      expect(
        after.attempt,
        2,
        reason: 'the retry count decides when to give up',
      );
      expect(after.task.type, before.task.type);
      expect(after.task.payload, before.task.payload);
    });

    test('carries a payload the queue never looks inside', () {
      // A queue has no business knowing the shape of what it moves, so the
      // awkward cases belong here: nesting, lists, nulls, a number that is not
      // an int, and text outside ASCII.
      final payload = <String, dynamic>{
        'user': {
          'name': 'Ada Lovelace',
          'tags': ['first', 'analytical'],
        },
        'amount': 12.5,
        'note': 'yazı — düz metin, emoji dahil 🧮',
        'retryHint': null,
        'quoted': r'a "quoted" \ backslash',
      };

      final after = Envelope.decode(anEnvelope(payload: payload).encode());

      expect(after.task.payload, payload);
    });

    test('an empty payload is not turned into something else', () {
      final after = Envelope.decode(anEnvelope(payload: {}).encode());
      expect(after.task.payload, isEmpty);
    });

    test('encoding twice gives the same bytes', () {
      // The stored string is used as a set member for exact removal, so two
      // encodings of the same envelope have to be equal or a replay cannot
      // find the entry it is replaying.
      final envelope = anEnvelope(attempt: 1);
      expect(envelope.encode(), envelope.encode());
    });

    test('a truncated entry fails rather than decoding to something wrong', () {
      final good = anEnvelope().encode();
      expect(
        () => Envelope.decode(good.substring(0, good.length ~/ 2)),
        throwsA(isA<FormatException>()),
      );
    });
  });

  group('dead letters', () {
    test('round trip keeps the failure alongside the task', () {
      final envelope = anEnvelope(id: 'task-9', attempt: 2);
      final at = DateTime.fromMillisecondsSinceEpoch(1786400000000);

      final dead = DeadLetter.decode(
        envelope.encodeDead(
          'Exception: smtp refused',
          at.millisecondsSinceEpoch,
        ),
      );

      expect(dead.id, 'task-9');
      expect(dead.error, 'Exception: smtp refused');
      expect(dead.deadAt, at);
      expect(dead.task.type, 'send-email');
      expect(dead.queue, 'default');
    });

    test('attempts counts runs, not retries', () {
      // `attempt` is how many retries have happened, so a task that ran three
      // times carries 2. Reporting that as "attempts: 2" to someone reading a
      // triage list would be off by the first run.
      final dead = DeadLetter.decode(
        anEnvelope(attempt: 2).encodeDead('boom', 0),
      );
      expect(dead.attempts, 3);
    });

    test('keeps the exact stored string, so a replay can remove it', () {
      final raw = anEnvelope().encodeDead('boom', 1);
      expect(DeadLetter.decode(raw).raw, raw);
    });

    test('an empty error is not confused with a missing one', () {
      final dead = DeadLetter.decode(anEnvelope().encodeDead('', 0));
      expect(dead.error, '');
      expect(dead.deadAt, DateTime.fromMillisecondsSinceEpoch(0));
    });

    group('written before 0.8.0', () {
      // That version stored the envelope on its own, with no error and no
      // timestamp. Anyone upgrading still has those entries on their list, and
      // the reader is documented as tolerating them.
      String oldFormat() => anEnvelope(id: 'old-1', attempt: 1).encode();

      test('still reads', () {
        final dead = DeadLetter.decode(oldFormat());

        expect(dead.id, 'old-1');
        expect(dead.task.type, 'send-email');
        expect(dead.queue, 'default');
        expect(dead.attempts, 2);
      });

      test('reports the missing fields as missing rather than as values', () {
        final dead = DeadLetter.decode(oldFormat());

        expect(dead.error, '', reason: 'no error was stored back then');
        expect(
          dead.deadAt,
          isNull,
          reason: 'null says "unknown"; an epoch date would say 1970',
        );
      });

      test('is told apart from the new format by the envelope key', () {
        // The two are distinguished by whether `envelope` holds a map. A
        // payload with a key of that name must not flip the reader onto the
        // wrong branch.
        final confusing = anEnvelope(
          payload: {'envelope': 'a string, not a map'},
        );

        final dead = DeadLetter.decode(confusing.encodeDead('boom', 5));

        expect(dead.error, 'boom');
        expect(dead.task.payload['envelope'], 'a string, not a map');
      });
    });
  });

  group('Task', () {
    test('survives a trip through plain JSON', () {
      final task = Task('resize', {'width': 800});
      final json = jsonDecode(jsonEncode(task.toJson()));

      final after = Task.fromJson((json as Map).cast<String, dynamic>());

      expect(after.type, 'resize');
      expect(after.payload, {'width': 800});
    });
  });
}
