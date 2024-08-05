using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DheClient.session;

public class DndClient : Client {
  internal static DndClient OfNativePtr(NativePtr<NativeDndClient> dndClient) {
    NativeDndClient.deephaven_enterprise_session_NativeDndClient_GetManager(dndClient,
      out var dndManagerResult, out var status);
    status.OkOrThrow();
    var dndManager = new DndTableHandleManager(dndManagerResult);

    return new DndClient(dndClient, dndManager);
  }

  private DndClient(NativePtr<NativeDndClient> self, DndTableHandleManager manager)
    : base(self.UnsafeCast<NativeClient>(self), manager) {
  }
}


internal partial class NativeDndClient {
  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_SessionManager_ConnectToPqByName(
    string pqName, InteropBool removeOnClose, out NativePtr<NativeDndClient> result, out ErrorStatus status);
}
