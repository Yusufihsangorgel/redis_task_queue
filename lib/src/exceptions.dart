import 'package:redis/redis.dart';

/// The base type for every failure this package reports.
///
/// Without it, callers would have to catch what `package:redis` happens to
/// raise, which is neither a stable nor a catchable set: `RedisError`,
/// `RedisRuntimeError` and `TransactionError` do not implement [Exception], so
/// `on Exception catch` misses them entirely, and a dropped socket surfaces
/// variously as a `SocketException`, a `StateError`, or a bare `String`.
/// Every public call funnels those into this hierarchy instead, so
/// `on TaskQueueException catch (e)` covers the lot and no caller has to
/// import `package:redis` to name an error type.
///
/// It is `sealed`: the two subtypes below are the whole set, so a `switch` over
/// them is exhaustive and stays that way.
sealed class TaskQueueException implements Exception {
  const TaskQueueException(this.message, {this.cause});

  /// What went wrong, in terms of the operation the caller asked for.
  final String message;

  /// The underlying error, kept for logging. Its type comes from
  /// `package:redis` or `dart:io` and is deliberately not part of this
  /// package's contract, so do not switch on it.
  final Object? cause;

  @override
  String toString() => cause == null
      ? '$runtimeType: $message'
      : '$runtimeType: $message ($cause)';
}

/// Redis could not be reached, or the connection died mid-command.
///
/// The client already retries once on a fresh socket before reporting this, so
/// receiving it means the second attempt failed too: Redis is down or
/// unreachable rather than merely blipping. Whatever was being written may or
/// may not have been applied.
final class TaskQueueConnectionException extends TaskQueueException {
  const TaskQueueConnectionException(super.message, {super.cause});
}

/// Redis received the command and answered with an error.
///
/// This is a server-side refusal — a wrong type for the key, a script failure,
/// an out-of-memory reply — not a connectivity problem, so retrying the same
/// command unchanged will usually fail the same way.
final class TaskQueueServerException extends TaskQueueException {
  const TaskQueueServerException(super.message, {super.cause});
}

/// Maps whatever `package:redis` or `dart:io` raised onto this package's
/// hierarchy. [operation] names the call the user made, so the message says
/// what failed rather than only how.
TaskQueueException wrapRedisError(Object error, String operation) =>
    switch (error) {
      RedisError() => TaskQueueServerException(
        '$operation failed: Redis returned an error',
        cause: error,
      ),
      RedisRuntimeError() => TaskQueueServerException(
        '$operation failed: the Redis reply could not be parsed',
        cause: error,
      ),
      _ => TaskQueueConnectionException(
        '$operation failed: the connection to Redis is unusable',
        cause: error,
      ),
    };
