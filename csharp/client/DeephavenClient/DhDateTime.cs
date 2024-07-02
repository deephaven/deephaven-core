using System;
using Deephaven.DeephavenClient.Utility;

namespace Deephaven.DeephavenClient;

/// <summary>
/// Deephaven's custom representation of DateTime, with full nanosecond resolution,
/// unlike .NET's own System.DateTime which has 100ns resolution.
/// </summary>
public readonly struct DhDateTime {
  public static DhDateTime Parse(string date) {
    // TODO(kosak): do something about the extra nanosecond resolution
    var dt = DateTime.Parse(date).ToUniversalTime();
    var ts = dt - DateTime.UnixEpoch;

    return new DhDateTime((Int64)ts.TotalNanoseconds);
  }

  public readonly Int64 Nanos;

  public static DhDateTime FromNanos(Int64 nanos) => new (nanos);

  public DhDateTime(int year, int month, int day, int hour, int minute = 0, int second = 0, int nanos = 0) {
    var ts = new DateTime(year, month, day, hour, minute, second) - DateTime.UnixEpoch;
    Nanos = (Int64)ts.TotalNanoseconds + nanos;
  }

  public DhDateTime(Int64 nanos) {
    Nanos = nanos == DeephavenConstants.NullLong ? 0 : nanos;
  }

  public DateTime DateTime => DateTime.UnixEpoch.AddTicks(Nanos / 100);

  public override string ToString() {
    return $"[Nanos={Nanos}]";
  }
}
