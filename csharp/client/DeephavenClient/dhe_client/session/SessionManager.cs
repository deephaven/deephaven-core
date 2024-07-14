using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DheClient.session;

public class SessionManager : IDisposable {
  internal NativePtr<NativeSessionManager> Self;

  public static SessionManager FromUrl(string descriptiveName, string jsonUrl) {
    NativeSessionManager.deephaven_enterprise_session_SessionManager_FromJson(descriptiveName,
      jsonUrl, out var sessionResult, out var status);
    status.OkOrThrow();
    return new SessionManager(sessionResult);

  }

  private SessionManager(NativePtr<NativeSessionManager> self) {
    Self = self;
  }

  ~SessionManager() {
    ReleaseUnmanagedResources();
  }

  public void Close() {
    Dispose();
  }

  public void Dispose() {
    ReleaseUnmanagedResources();
    GC.SuppressFinalize(this);
  }

  private void ReleaseUnmanagedResources() {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    NativeSessionManager.deephaven_enterprise_session_SessionManager_dtor(old);
  }
}

internal partial class NativeSessionManager {
  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_SessionManager_dtor(
    NativePtr<NativeSessionManager> self);

  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_SessionManager_FromJson(string descriptiveName,
    string json, out NativePtr<NativeSessionManager> result, out ErrorStatus status);
}
