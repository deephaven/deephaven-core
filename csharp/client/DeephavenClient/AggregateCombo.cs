using System;
using System.Runtime.InteropServices;
using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DeephavenClient;

public class AggregateCombo {
  private readonly Aggregate[] _aggregates;

  public AggregateCombo(IEnumerable<Aggregate> aggregates) => _aggregates = aggregates.ToArray();

  internal InternalAggregateCombo Invoke() {
    return new InternalAggregateCombo(_aggregates);
  }
}

internal class InternalAggregateCombo : IDisposable {
  internal NativePtr<NativeAggregateCombo> Self;

  internal InternalAggregateCombo(Aggregate[] aggregates) {
    var internalAggregates = new List<InternalAggregate>();
    try {
      // Invoke the lazy method on the aggregate to get its C++ wrapper
      foreach (var agg in aggregates) {
        internalAggregates.Add(agg.Materialize());
      }

      var internalAggPtrs = internalAggregates.Select(ag => ag.Self).ToArray();
      NativeAggregateCombo.deephaven_client_AggregateCombo_Create(
        internalAggPtrs, internalAggPtrs.Length, out var result, out var status);
      status.OkOrThrow();
      Self = result;
    } finally {
      foreach (var agg in internalAggregates) {
        agg.Dispose();
      }
    }
  }

  ~InternalAggregateCombo() {
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
    NativeAggregateCombo.deephaven_client_AggregateCombo_dtor(old);
  }
}

internal partial class NativeAggregateCombo {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_AggregateCombo_Create(
    NativePtr<NativeAggregate>[] aggregates, Int32 numAggregates,
    out NativePtr<NativeAggregateCombo> self, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_AggregateCombo_dtor(NativePtr<NativeAggregateCombo> self);
}
