using System;
using System.Runtime.InteropServices;
using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DeephavenClient;

public class DurationSpecifier {
  private readonly object _duration;

  public DurationSpecifier(Int64 nanos) => _duration = nanos;
  public DurationSpecifier(string duration) => _duration = duration;

  public static implicit operator DurationSpecifier(Int64 nanos) => new (nanos);
  public static implicit operator DurationSpecifier(string duration) => new (duration);
  public static implicit operator DurationSpecifier(TimeSpan ts) => new((long)(ts.TotalMicroseconds * 1000));

  internal InternalDurationSpecifier Materialize() => new (_duration);
}

internal class InternalDurationSpecifier : IDisposable {
  internal NativePtr<NativeDurationSpecifier> Self;

  public InternalDurationSpecifier(object duration) {
    NativePtr<NativeDurationSpecifier> result;
    ErrorStatus status;
    if (duration is Int64 nanos) {
      NativeDurationSpecifier.deephaven_client_utility_DurationSpecifier_ctor_nanos(nanos,
        out result, out status);
    } else if (duration is string dur) {
      NativeDurationSpecifier.deephaven_client_utility_DurationSpecifier_ctor_durationstr(dur,
        out result, out status);
    } else {
      throw new ArgumentException($"Unexpected type {duration.GetType().Name} for duration");
    }
    status.OkOrThrow();
    Self = result;
  }

  ~InternalDurationSpecifier() {
    ReleaseUnmanagedResources();
  }

  public void Dispose() {
    ReleaseUnmanagedResources();
    GC.SuppressFinalize(this);
  }

  private void ReleaseUnmanagedResources() {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    NativeDurationSpecifier.deephaven_client_utility_DurationSpecifier_dtor(old);
  }
}


internal partial class NativeDurationSpecifier {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_utility_DurationSpecifier_ctor_nanos(Int64 nanos,
    out NativePtr<NativeDurationSpecifier> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_utility_DurationSpecifier_ctor_durationstr(string duration,
    out NativePtr<NativeDurationSpecifier> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_utility_DurationSpecifier_dtor(NativePtr<NativeDurationSpecifier> self);
}
