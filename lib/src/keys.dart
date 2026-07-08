/// Redis key layout, in one place so the client and worker can't disagree.
///
/// Everything is namespaced under a prefix so a queue can share a Redis
/// instance with other data without collisions.
class Keys {
  Keys(this.prefix);

  final String prefix;

  /// The list holding pending envelopes for a queue. Producers LPUSH, the
  /// worker BRPOP.
  String pending(String queue) => '$prefix:queue:$queue';

  /// The sorted set holding envelopes waiting out a retry backoff for a queue.
  /// The score is the unix-millis timestamp at which the task becomes due; the
  /// worker's due-mover promotes members whose score has passed back onto
  /// [pending]. Kept per-queue (rather than one global set) so the mover can
  /// LPUSH straight to the right pending list without decoding the envelope.
  String delayed(String queue) => '$prefix:queue:$queue:delayed';

  /// The list holding envelopes that exhausted their retries.
  String deadLetter() => '$prefix:dead';
}
