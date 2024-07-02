using Deephaven.DeephavenClient.Interop;
using System.Runtime.InteropServices;

namespace Deephaven.DeephavenClient;

public class ClientOptions : IDisposable {
  internal NativePtr<NativeClientOptions> Self;

  public ClientOptions() {
    NativeClientOptions.deephaven_client_ClientOptions_ctor(out var result, out var status);
    status.OkOrThrow();
    Self = result;
  }

  ~ClientOptions() {
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
    NativeClientOptions.deephaven_client_ClientOptions_dtor(old);
  }

  public ClientOptions SetDefaultAuthentication() {
    NativeClientOptions.deephaven_client_ClientOptions_SetDefaultAuthentication(Self,
      out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions SetBasicAuthentication(string username, string password) {
    NativeClientOptions.deephaven_client_ClientOptions_SetBasicAuthentication(Self,
      username, password, out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions SetCustomAuthentication(string authenticationKey, string authenticationValue) {
    NativeClientOptions.deephaven_client_ClientOptions_SetCustomAuthentication(Self,
      authenticationKey, authenticationValue, out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions SetSessionType(string sessionType) {
    NativeClientOptions.deephaven_client_ClientOptions_SetSessionType(Self, sessionType,
      out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions SetUseTls(bool useTls) {
    NativeClientOptions.deephaven_client_ClientOptions_SetUseTls(Self, (InteropBool)useTls,
      out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions SetTlsRootCerts(string tlsRootCerts) {
    NativeClientOptions.deephaven_client_ClientOptions_SetTlsRootCerts(Self, tlsRootCerts,
      out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions SetClientCertChain(string clientCertChain) {
    NativeClientOptions.deephaven_client_ClientOptions_SetClientCertChain(Self, clientCertChain,
      out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions SetClientPrivateKey(string clientPrivateKey) {
    NativeClientOptions.deephaven_client_ClientOptions_SetClientPrivateKey(Self, clientPrivateKey,
      out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions AddIntOption(string opt, Int32 val) {
    NativeClientOptions.deephaven_client_ClientOptions_AddIntOption(Self, opt, val,
      out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions AddStringOption(string opt, string val) {
    NativeClientOptions.deephaven_client_ClientOptions_AddStringOption(Self, opt, val,
      out var status);
    status.OkOrThrow();
    return this;
  }

  public ClientOptions AddExtraHeader(string headerName, string headerValue) {
    NativeClientOptions.deephaven_client_ClientOptions_AddExtraHeader(Self, headerName, headerValue,
      out var status);
    status.OkOrThrow();
    return this;
  }
}


internal partial class NativeClientOptions {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_ctor(
    out NativePtr<NativeClientOptions> result, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_dtor(NativePtr<NativeClientOptions> self);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_SetDefaultAuthentication(NativePtr<NativeClientOptions> self,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_SetBasicAuthentication(NativePtr<NativeClientOptions> self,
    string username, string password, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_SetCustomAuthentication(NativePtr<NativeClientOptions> self,
    string authentication_key, string authentication_value, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_SetSessionType(NativePtr<NativeClientOptions> self,
    string session_type, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_SetUseTls(NativePtr<NativeClientOptions> self,
    InteropBool use_tls, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_SetTlsRootCerts(NativePtr<NativeClientOptions> self,
    string tls_root_certs, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_SetClientCertChain(NativePtr<NativeClientOptions> self,
    string client_cert_chain, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_SetClientPrivateKey(NativePtr<NativeClientOptions> self,
    string client_private_key, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_AddIntOption(NativePtr<NativeClientOptions> self,
    string opt, Int32 val, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_AddStringOption(NativePtr<NativeClientOptions> self,
    string opt, string val, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientOptions_AddExtraHeader(NativePtr<NativeClientOptions> self,
    string header_name, string header_value, out ErrorStatus status);
}
