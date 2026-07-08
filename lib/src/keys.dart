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

  /// The list holding envelopes that exhausted their retries.
  String deadLetter() => '$prefix:dead';
}
