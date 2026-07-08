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
