/// A snapshot of how much work is sitting in the queue right now.
///
/// The answer to "is the system keeping up": [pending] is work waiting to be
/// claimed, [inFlight] is work a worker has taken but not finished, [delayed]
/// is scheduled or backing-off work not yet due, and [deadLetter] is work that
/// gave up. A pending count that only grows means the workers are behind; a
/// dead-letter count that grows means something is failing for good.
final class QueueStats {
  QueueStats({
    required this.pending,
    required this.delayed,
    required this.deadLetter,
    required this.inFlight,
  });

  /// Tasks waiting on each queue's pending list, keyed by queue name.
  final Map<String, int> pending;

  /// Tasks in each queue's delayed set (scheduled or backing off before a
  /// retry), keyed by queue name.
  final Map<String, int> delayed;

  /// Tasks on the dead-letter list, across all queues.
  final int deadLetter;

  /// Tasks currently held in flight by workers, summed across every worker's
  /// in-flight list.
  final int inFlight;

  /// Total pending across all queues.
  int get totalPending => pending.values.fold(0, (a, b) => a + b);

  /// Total delayed across all queues.
  int get totalDelayed => delayed.values.fold(0, (a, b) => a + b);

  @override
  String toString() => 'QueueStats(pending: $totalPending, inFlight: '
      '$inFlight, delayed: $totalDelayed, deadLetter: $deadLetter)';
}
