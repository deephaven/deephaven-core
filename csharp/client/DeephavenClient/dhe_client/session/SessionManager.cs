using System.Runtime.InteropServices;
using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DheClient.session;

public class SessionManager : IDisposable {
  internal NativePtr<NativeSessionManager> Self;

  public static SessionManager FromUrl(string descriptiveName, string jsonUrl) {
    NativeSessionManager.deephaven_enterprise_session_SessionManager_FromUrl(descriptiveName,
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

  public bool PasswordAuthentication(string user, string password, string operateAs) {
    NativeSessionManager.deephaven_enterprise_session_SessionManager_PasswordAuthentication(
      user, password, operateAs, out var result, out var status);
    status.OkOrThrow();
    return (bool)result;
  }

  public DndClient ConnectToPqByName(string pqName, bool removeOnClose) {
    NativeSessionManager.deephaven_enterprise_session_SessionManager_ConnectToPqByName(
      pqName, (InteropBool)removeOnClose, out var result, out var status);
    status.OkOrThrow();
    return DndClient.OfNativePtr(result);
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
  public static partial void deephaven_enterprise_session_SessionManager_FromUrl(string descriptiveName,
    string jsonUrl, out NativePtr<NativeSessionManager> result, out ErrorStatus status);

  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_SessionManager_PasswordAuthentication(
    string user, string password, string operateAs, out InteropBool result, out ErrorStatus status);

  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_enterprise_session_SessionManager_ConnectToPqByName(
    string pqName, InteropBool removeOnClose, out NativePtr<NativeDndClient> result, out ErrorStatus status);
}
