using System;
using Deephaven.CppClientInterop.Native;

namespace Deephaven.CppClientInterop;

public class SubscriptionHandle {
  internal NativePtr<Native.SubscriptionHandle> Self;

  internal SubscriptionHandle(NativePtr<Native.SubscriptionHandle> self) {
    Self = self;
  }
}

public class TickingUpdate : IDisposable {
  private NativePtr<Native.TickingUpdate> self;

  internal TickingUpdate(NativePtr<Native.TickingUpdate> self) => this.self = self;

  public ClientTable Current {
    get {
      Native.TickingUpdate.deephaven_client_TickingUpdate_Current(self,
        out var ct, out var status);
      status.OkOrThrow();
      return new ClientTable(ct);
    }
  }
  // public ClientTable BeforeRemoves { get;  }
  // public RowSequence RemovedRows { get;  }

  ~TickingUpdate() {
    Dispose();
  }

  public void Dispose() {
    if (self.ptr == IntPtr.Zero) {
      return;
    }
    Native.TickingUpdate.deephaven_client_TickingUpdate_dtor(self);
    self.ptr = IntPtr.Zero;
    GC.SuppressFinalize(this);
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
  void OnFailure(string errorMessage);
}
