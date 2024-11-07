using Deephaven.DeephavenClient.Interop;
using System;
using System.Runtime.InteropServices;

namespace Deephaven.DeephavenClient;

public class Client : IDisposable {
  internal NativePtr<NativeClient> Self;
  public TableHandleManager Manager;

  public static Client Connect(string target, ClientOptions options) {
    NativeClient.deephaven_client_Client_Connect(target, options.Self, out var clientResult, out var status1);
    status1.OkOrThrow();
    NativeClient.deephaven_client_Client_GetManager(clientResult, out var managerResult, out var status2);
    status2.OkOrThrow();
    var manager = new TableHandleManager(managerResult);
    return new Client(clientResult, manager);
  }

  private protected Client(NativePtr<NativeClient> self, TableHandleManager manager) {
    Self = self;
    Manager = manager;
  }

  ~Client() {
    ReleaseUnmanagedResources(true);
  }

  public void Close() {
    Dispose();
  }

  public void Dispose() {
    ReleaseUnmanagedResources(true);
    GC.SuppressFinalize(this);
  }

  protected virtual void ReleaseUnmanagedResources(bool destructSelf) {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    if (!destructSelf) {
      return;
    }
    NativeClient.deephaven_client_Client_dtor(old);
  }
}

internal partial class NativeClient {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_Client_Connect(string target,
    NativePtr<NativeClientOptions> options,
    out NativePtr<NativeClient> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_Client_dtor(NativePtr<NativeClient> self);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_Client_Close(NativePtr<NativeClient> self,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_Client_GetManager(NativePtr<NativeClient> self,
    out NativePtr<NativeTableHandleManager> result,
    out ErrorStatus status);
}
