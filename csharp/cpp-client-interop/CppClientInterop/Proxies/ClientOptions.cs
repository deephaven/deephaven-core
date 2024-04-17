using Deephaven.CppClientInterop.Native;

namespace Deephaven.CppClientInterop;

public class ClientOptions : IDisposable {
  internal NativePtr<Native.ClientOptions> self;

  public ClientOptions() {
    Native.ClientOptions.deephaven_client_ClientOptions_ctor(out var result, out var status);
    self = status.Unwrap(result);
  }

  ~ClientOptions() {
    Dispose();
  }

  public void Dispose() {
    if (self.ptr == IntPtr.Zero) {
      return;
    }
    Native.ClientOptions.deephaven_client_ClientOptions_dtor(self);
    self.ptr = IntPtr.Zero;
    GC.SuppressFinalize(this);
  }

  ClientOptions SetDefaultAuthentication() {
    Native.ClientOptions.deephaven_client_ClientOptions_SetDefaultAuthentication(self,
      out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions SetBasicAuthentication(string username, string password) {
    Native.ClientOptions.deephaven_client_ClientOptions_SetBasicAuthentication(self,
      username, password, out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions SetCustomAuthentication(string authenticationKey, string authenticationValue) {
    Native.ClientOptions.deephaven_client_ClientOptions_SetCustomAuthentication(self,
      authenticationKey, authenticationValue, out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions SetSessionType(string sessionType) {
    Native.ClientOptions.deephaven_client_ClientOptions_SetSessionType(self, sessionType,
      out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions SetUseTls(bool useTls) {
    Native.ClientOptions.deephaven_client_ClientOptions_SetUseTls(self, useTls,
      out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions SetTlsRootCerts(string tlsRootCerts) {
    Native.ClientOptions.deephaven_client_ClientOptions_SetTlsRootCerts(self, tlsRootCerts,
      out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions SetClientCertChain(string clientCertChain) {
    Native.ClientOptions.deephaven_client_ClientOptions_SetClientCertChain(self, clientCertChain,
      out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions SetClientPrivateKey(string clientPrivateKey) {
    Native.ClientOptions.deephaven_client_ClientOptions_SetClientPrivateKey(self, clientPrivateKey,
      out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions AddIntOption(string opt, Int32 val) {
    Native.ClientOptions.deephaven_client_ClientOptions_AddIntOption(self, opt, val,
      out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions AddStringOption(string opt, string val) {
    Native.ClientOptions.deephaven_client_ClientOptions_AddStringOption(self, opt, val,
      out var status);
    status.OkOrThrow();
    return this;
  }

  ClientOptions AddExtraHeader(string headerName, string headerValue) {
    Native.ClientOptions.deephaven_client_ClientOptions_AddExtraHeader(self, headerName, headerValue,
      out var status);
    status.OkOrThrow();
    return this;
  }
}
