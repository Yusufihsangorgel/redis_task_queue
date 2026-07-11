/// The unix-millis sorted-set score at which a scheduled envelope becomes
/// due: [processAt] as-is, or [now] plus [processIn]. Set at most one. [now]
/// is a parameter rather than a wall-clock read so the math is testable.
///
/// If asserts are compiled out and both are set, [processAt] wins.
int dueAtMillis({
  DateTime? processAt,
  Duration? processIn,
  required DateTime now,
}) {
  assert(
    processAt == null || processIn == null,
    'pass processAt or processIn, not both',
  );
  if (processAt != null) return processAt.millisecondsSinceEpoch;
  return now.add(processIn ?? Duration.zero).millisecondsSinceEpoch;
}
