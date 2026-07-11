import 'package:redis_task_queue/src/schedule.dart';
import 'package:test/test.dart';

/// Pure tests for the scheduling score math. No Redis needed: [dueAtMillis]
/// takes `now` as a parameter, so a fixed clock checks the math exactly.
void main() {
  final now = DateTime.utc(2026, 1, 15, 12);

  test('processAt is used as the score verbatim', () {
    final at = DateTime.utc(2026, 3, 1, 9, 30);
    expect(dueAtMillis(processAt: at, now: now), at.millisecondsSinceEpoch);
  });

  test('processIn scores relative to now', () {
    expect(
      dueAtMillis(processIn: const Duration(minutes: 5), now: now),
      now.millisecondsSinceEpoch + 5 * 60 * 1000,
    );
  });

  test('a zero processIn is already due (the mover bound is inclusive)', () {
    // The mover promotes score <= now, so a score equal to now counts as due.
    expect(
      dueAtMillis(processIn: Duration.zero, now: now),
      now.millisecondsSinceEpoch,
    );
  });

  test('a positive processIn is not yet due at enqueue time', () {
    expect(
      dueAtMillis(processIn: const Duration(seconds: 1), now: now),
      greaterThan(now.millisecondsSinceEpoch),
    );
  });

  test('a processAt in the past scores as already due', () {
    final past = now.subtract(const Duration(hours: 2));
    expect(
      dueAtMillis(processAt: past, now: now),
      lessThanOrEqualTo(now.millisecondsSinceEpoch),
    );
  });

  test('setting both processAt and processIn is an assertion failure', () {
    expect(
      () => dueAtMillis(
        processAt: now.add(const Duration(minutes: 1)),
        processIn: const Duration(minutes: 1),
        now: now,
      ),
      throwsA(isA<AssertionError>()),
    );
  });
}
