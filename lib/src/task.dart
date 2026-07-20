import 'dart:convert';

/// A unit of work to run outside the request path.
///
/// A task has a [type] (which decides who handles it) and a JSON-serializable
/// [payload]. The type/payload split is deliberate: the enqueuing side and the
/// worker side only need to agree on a string and a shape, not share code.
class Task {
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
class TaskContext {
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
    return Envelope(
      id: json['id'] as String,
      task: Task.fromJson((json['task'] as Map).cast<String, dynamic>()),
      queue: json['queue'] as String,
      maxRetries: json['max_retries'] as int,
      attempt: json['attempt'] as int,
    );
  }
}
