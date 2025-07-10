//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public readonly struct DurationSpecifier {
  private readonly object _duration;

  public DurationSpecifier() => _duration = (Int64)0;
  public DurationSpecifier(Int64 nanos) => _duration = nanos;
  public DurationSpecifier(string duration) => _duration = duration;

  public static implicit operator DurationSpecifier(Int64 nanos) => new(nanos);
  public static implicit operator DurationSpecifier(string duration) => new(duration);
  public static implicit operator DurationSpecifier(TimeSpan ts) => new((long)(ts.TotalMicroseconds * 1000));

  public void Visit(Action<Int64> onNanos, Action<string> onDuration) {
    if (_duration is Int64 nanos) {
      onNanos(nanos);
    } else {
      onDuration((string)_duration);
    }
  }
}

public readonly struct TimePointSpecifier {
  private readonly object _timePoint;

  public TimePointSpecifier() => _timePoint = (Int64)0;
  public TimePointSpecifier(Int64 nanos) => _timePoint = nanos;
  public TimePointSpecifier(string timePoint) => _timePoint = timePoint;

  public static implicit operator TimePointSpecifier(Int64 nanos) => new(nanos);
  public static implicit operator TimePointSpecifier(string timePoint) => new(timePoint);

  public void Visit(Action<Int64> onNanos, Action<string> onDuration) {
    if (_timePoint is Int64 nanos) {
      onNanos(nanos);
    } else {
      onDuration((string)_timePoint);
    }
  }
}
