import 'dart:convert';

/// A unit of work to run outside the request path.
///
/// A task has a [type] (which decides who handles it) and a JSON-serializable
/// [payload]. The type/payload split is deliberate: the enqueuing side and the
/// worker side only need to agree on a string and a shape, not share code.
final class Task {
  Task(this.type, this.payload);

  /// The task type name. The worker routes to a handler by this string.
  final String type;

  /// Arbitrary JSON-serializable data the handler needs.
  final Map<String, dynamic> payload;

  Map<String, dynamic> toJson() => {'type': type, 'payload': payload};

  static Task fromJson(Map<String, dynamic> json) => Task(
        json['type'] as String,
        (json['payload'] as Map).cast<String, dynamic>(),
      );
}

/// What the worker knows about the run in progress, handed to a handler
/// alongside the task.
///
/// Delivery is at-least-once, so a handler has to cope with running twice for
/// the same task: a worker can die after doing the work but before recording
/// it, and the replacement will hand the task back. [id] is what makes that
/// survivable. It is assigned once, at enqueue, and is the same on every
/// attempt, so it is the key to write against when deciding whether the work
/// has already been done.
final class TaskContext {
  const TaskContext({
    required this.id,
    required this.queue,
    required this.attempt,
    required this.maxAttempts,
  });

  /// The task's id: assigned at enqueue, stable across retries and recoveries.
  ///
  /// This is the value to deduplicate on. Storing it with the result of the
  /// work, in the same transaction as the work where the store allows it, turns
  /// at-least-once delivery into an effect that happens once.
  final String id;

  /// The queue this task was claimed from.
  final String queue;

  /// Which attempt this is, counting from 1: the first run is 1, the run after
  /// one retry is 2.
  final int attempt;

  /// How many attempts the task gets in all: the first run plus its retries,
  /// so a task enqueued with `maxRetries: 5` has six.
  final int maxAttempts;

  /// Whether throwing from this run sends the task to the dead-letter list
  /// rather than scheduling another retry.
  ///
  /// The hook for a last-ditch path: record a partial result, or attach the
  /// context a human will want, while there is still a run to do it in.
  bool get isLastAttempt => attempt >= maxAttempts;
}

/// A task plus the queue metadata Redis needs to run and retry it.
///
/// This is what actually gets stored; [Task] is the user-facing part.
class Envelope {
  Envelope({
    required this.id,
    required this.task,
    required this.queue,
    required this.maxRetries,
    this.attempt = 0,
  });

  final String id;
  final Task task;
  final String queue;
  final int maxRetries;

  /// How many times this envelope has already been tried. Incremented on each
  /// retry so the worker can give up after [maxRetries].
  int attempt;

  String encode() => jsonEncode({
        'id': id,
        'task': task.toJson(),
        'queue': queue,
        'max_retries': maxRetries,
        'attempt': attempt,
      });

  static Envelope decode(String raw) {
    final json = jsonDecode(raw) as Map<String, dynamic>;
    return Envelope.fromJson(json);
  }

  static Envelope fromJson(Map<String, dynamic> json) => Envelope(
        id: json['id'] as String,
        task: Task.fromJson((json['task'] as Map).cast<String, dynamic>()),
        queue: json['queue'] as String,
        maxRetries: json['max_retries'] as int,
        attempt: json['attempt'] as int,
      );

  /// Wraps this envelope for the dead-letter list, keeping the failure that
  /// gave up on it. [error] is the last attempt's error, [deadAt] when it was
  /// dead-lettered.
  String encodeDead(String error, int deadAt) => jsonEncode({
        'envelope': {
          'id': id,
          'task': task.toJson(),
          'queue': queue,
          'max_retries': maxRetries,
          'attempt': attempt,
        },
        'error': error,
        'dead_at': deadAt,
      });
}

/// A task that exhausted its retries and is parked on the dead-letter list,
/// together with why it failed.
///
/// This is what [QueueClient.deadLetters] returns, the raw material for triage:
/// see what died and why, then decide whether to [QueueClient.replayDeadLetter]
/// it after fixing the cause, or drop it.
final class DeadLetter {
  DeadLetter({
    required this.id,
    required this.task,
    required this.queue,
    required this.error,
    required this.attempts,
    required this.deadAt,
    required this.raw,
  });

  /// The task's id, the same one [enqueue] returned and the handler saw.
  final String id;

  /// The task that failed.
  final Task task;

  /// The queue it was on.
  final String queue;

  /// The last attempt's error, as text. Empty when the entry predates 0.8.0,
  /// which did not store it.
  final String error;

  /// How many attempts were made in all before giving up (the first run plus
  /// its retries).
  final int attempts;

  /// When it was dead-lettered, or null for a pre-0.8.0 entry.
  final DateTime? deadAt;

  /// The exact stored string, kept so a replay can remove this precise entry.
  final String raw;

  /// Decodes a dead-letter entry. Tolerates the pre-0.8.0 format, a bare
  /// envelope with no error, so a list written by an older worker still reads.
  static DeadLetter decode(String raw) {
    final json = jsonDecode(raw) as Map<String, dynamic>;
    final envelopeJson = json['envelope'];
    final Envelope envelope;
    final String error;
    final DateTime? deadAt;
    if (envelopeJson is Map) {
      envelope = Envelope.fromJson(envelopeJson.cast<String, dynamic>());
      error = (json['error'] as String?) ?? '';
      final deadMillis = json['dead_at'] as int?;
      deadAt = deadMillis == null
          ? null
          : DateTime.fromMillisecondsSinceEpoch(deadMillis);
    } else {
      // Old format: the stored string is the envelope itself.
      envelope = Envelope.fromJson(json);
      error = '';
      deadAt = null;
    }
    return DeadLetter(
      id: envelope.id,
      task: envelope.task,
      queue: envelope.queue,
      error: error,
      attempts: envelope.attempt + 1,
      deadAt: deadAt,
      raw: raw,
    );
  }
}
