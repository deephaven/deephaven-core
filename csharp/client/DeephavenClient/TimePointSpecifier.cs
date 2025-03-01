using Deephaven.DeephavenClient.Interop;
using System.Runtime.InteropServices;

namespace Deephaven.DeephavenClient;

public class TimePointSpecifier {
  private readonly object _timePoint;

  public TimePointSpecifier(Int64 nanos) => _timePoint = nanos;
  public TimePointSpecifier(string timePoint) => _timePoint = timePoint;

  public static implicit operator TimePointSpecifier(Int64 nanos) => new(nanos);
  public static implicit operator TimePointSpecifier(string timePoint) => new(timePoint);

  internal InternalTimePointSpecifier Materialize() => new (_timePoint);
}

internal class InternalTimePointSpecifier : IDisposable {
  internal NativePtr<NativeTimePointSpecifier> Self;

  public InternalTimePointSpecifier(object duration) {
    NativePtr<NativeTimePointSpecifier> result;
    ErrorStatus status;
    if (duration is Int64 nanos) {
      NativeTimePointSpecifier.deephaven_client_utility_TimePointSpecifier_ctor_nanos(nanos,
        out result, out status);
    } else if (duration is string dur) {
      NativeTimePointSpecifier.deephaven_client_utility_TimePointSpecifier_ctor_timepointstr(dur,
        out result, out status);
    } else {
      throw new ArgumentException($"Unexpected type {duration.GetType().Name} for duration");
    }
    status.OkOrThrow();
    Self = result;
  }

  ~InternalTimePointSpecifier() {
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
    NativeTimePointSpecifier.deephaven_client_utility_TimePointSpecifier_dtor(old);
  }
}

internal partial class NativeTimePointSpecifier {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_utility_TimePointSpecifier_ctor_nanos(Int64 nanos,
    out NativePtr<NativeTimePointSpecifier> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_utility_TimePointSpecifier_ctor_timepointstr(
    string timePointStr, out NativePtr<NativeTimePointSpecifier> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_utility_TimePointSpecifier_dtor(NativePtr<NativeTimePointSpecifier> self);
}
