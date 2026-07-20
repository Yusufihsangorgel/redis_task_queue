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

  /// The sorted set holding envelopes that aren't due yet for a queue, both
  /// scheduled tasks and retries waiting out a backoff.
  /// The score is the unix-millis timestamp at which the task becomes due; the
  /// worker's due-mover promotes members whose score has passed back onto
  /// [pending]. Kept per-queue (rather than one global set) so the mover can
  /// LPUSH straight to the right pending list without decoding the envelope.
  String delayed(String queue) => '$prefix:queue:$queue:delayed';

  /// The list holding envelopes that exhausted their retries.
  String deadLetter() => '$prefix:dead';

  /// The per-worker in-flight list: envelopes a worker has taken off [pending]
  /// but not yet finished. A task is atomically moved here as it is claimed and
  /// removed once it is done, retried, or dead-lettered, so a worker that dies
  /// mid-task leaves the envelope here instead of losing it. The id is
  /// per-worker so a restarted worker recovers only its own orphans, never a
  /// live peer's in-flight work.
  String inFlight(String workerId) => '$prefix:inflight:$workerId';
}
