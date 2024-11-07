using Deephaven.DeephavenClient.Interop;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Deephaven.DeephavenClient;

public class SubscriptionHandle : IDisposable {
  internal NativePtr<NativeSubscriptionHandle> Self;

  internal SubscriptionHandle(NativePtr<NativeSubscriptionHandle> self) {
    Self = self;
  }

  ~SubscriptionHandle() {
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
    NativeSubscriptionHandle.deephaven_client_SubscriptionHandle_dtor(old);
  }
}

public class TickingUpdate : IDisposable {
  internal NativePtr<NativeTickingUpdate> Self;

  internal TickingUpdate(NativePtr<NativeTickingUpdate> self) => this.Self = self;

  public ClientTable Current {
    get {
      NativeTickingUpdate.deephaven_client_TickingUpdate_Current(Self,
        out var ct, out var status);
      status.OkOrThrow();
      return new ClientTable(ct);
    }
  }
  // public ClientTable BeforeRemoves { get;  }
  // public RowSequence RemovedRows { get;  }

  ~TickingUpdate() {
    ReleaseUnmanagedResources();
  }

  public void Dispose() {
    ReleaseUnmanagedResources();
    GC.SuppressFinalize(this);
  }

  public void ReleaseUnmanagedResources() {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    NativeTickingUpdate.deephaven_client_TickingUpdate_dtor(old);
  }
}

public interface ITickingCallback {
  /**
   * Invoked on each update to the subscription.
   */
  void OnTick(TickingUpdate update);

  /**
   * Invoked if there is an error involving the subscription.
   */
  void OnFailure(string errorText);
}

internal partial class NativeSubscriptionHandle {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_SubscriptionHandle_dtor(NativePtr<NativeSubscriptionHandle> self);
}

internal partial class NativeTickingUpdate {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TickingUpdate_dtor(NativePtr<NativeTickingUpdate> self);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TickingUpdate_Current(NativePtr<NativeTickingUpdate> self,
    out NativePtr<NativeClientTable> result, out ErrorStatus status);
}
