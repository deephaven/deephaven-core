using System.Runtime.InteropServices;
using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DheClient.Session;

public sealed class DndClient : Client {
  internal new NativePtr<NativeDndClient> Self;
  public Int64 PqSerial;

  internal static DndClient OfNativePtr(NativePtr<NativeDndClient> dndClient) {
    NativeDndClient.deephaven_enterprise_session_DndClient_GetManager(dndClient,
      out var dndManagerResult, out var status1);
    status1.OkOrThrow();
    NativeDndClient.deephaven_enterprise_session_DndClient_PqSerial(dndClient,
      out var pqSerial, out var status2);
    status2.OkOrThrow();
    var dndManager = new DndTableHandleManager(dndManagerResult);

    return new DndClient(dndClient, dndManager, pqSerial);
  }

  private DndClient(NativePtr<NativeDndClient> self, DndTableHandleManager manager,
    Int64 pqSerial)
    : base(self.UnsafeCast<NativeClient>(), manager) {
    Self = self;
    PqSerial = pqSerial;
  }

  protected override void ReleaseUnmanagedResources(bool destructSelf) {
    base.ReleaseUnmanagedResources(false);
    if (!Self.TryRelease(out var old)) {
      return;
    }
    if (!destructSelf) {
      return;
    }
    NativeDndClient.deephaven_enterprise_session_DndClient_dtor(old);
  }
}


internal partial class NativeDndClient {
  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_DndClient_dtor(
    NativePtr<NativeDndClient> self);

  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_DndClient_GetManager(
    NativePtr<NativeDndClient> self,
    out NativePtr<NativeDndTableHandleManager> result, out ErrorStatus status);

  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_DndClient_PqSerial(
    NativePtr<NativeDndClient> self,
    out Int64 result, out ErrorStatus status);
}
