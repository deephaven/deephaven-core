using System.Runtime.InteropServices;
using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DheClient.Session;

public sealed class DndTableHandleManager : TableHandleManager {
  internal new NativePtr<NativeDndTableHandleManager> Self;

  internal DndTableHandleManager(NativePtr<NativeDndTableHandleManager> self) :
    base(self.UnsafeCast<NativeTableHandleManager>()) {
    Self = self;
  }

  protected override void ReleaseUnmanagedResources(bool destructSelf) {
    base.ReleaseUnmanagedResources(false);
    if (!Self.TryRelease(out var old)) {
      return;
    }
    if (!destructSelf) {
      return;
    }
    NativeDndTableHandleManager.deephaven_enterprise_session_DndTableHandleManager_dtor(old);
  }

}

internal partial class NativeDndTableHandleManager {
  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_DndTableHandleManager_dtor(
    NativePtr<NativeDndTableHandleManager> self);
}
