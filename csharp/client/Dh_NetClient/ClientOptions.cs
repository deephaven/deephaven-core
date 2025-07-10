//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Text;

namespace Deephaven.Dh_NetClient;

public static class SessionTypes {
  public const string Python = "python";
  public const string Groovy = "groovy";
}

public class ClientOptions {
  /// <summary>
  /// Returns the value for the authorization header that will be sent to the server
  /// on the first request; this value is a function of the authentication method selected.
  /// </summary>
  public string AuthorizationValue { get; private set; } = "";

  /// <summary>
  /// The Session Type (python or groovy).
  /// </summary>
  public string SessionType { get; set; } = "";

  /// <summary>
  /// Returns true if server connections should be configured for TLS/SSL, false for insecure.
  /// </summary>
  public bool UseTls { get; set; } = false;

  /// <summary>
  /// The PEM-encoded certificate root for server connections, or the empty string
  /// if using system defaults.
  /// </summary>
  public string TlsRootCerts { get; set; } = "";

  /// <summary>
  /// The PEM-encoded certificate chain to use for the client
  /// when using mutual TLS, or the empty string for no mutual TLS.
  /// </summary>
  public string ClientCertChain { get; set; } = "";

  /// <summary>
  /// The PEM-encoded client private key to use for mutual TLS.
  /// </summary>
  public string ClientPrivateKey { get; set; } = "";

  /// <summary>
  /// Integer-valued channel options set for server connections.
  /// </summary>
  public IReadOnlyList<(string, int)> IntOptions => _intOptions;

  /// <summary>
  /// String-valued channel options set for server connections.
  /// </summary>
  public IReadOnlyList<(string, string)> StringOptions => _stringOptions;

  /// <summary>
  /// Extra headers that should be sent with each outgoing server request.
  /// </summary>
  public IReadOnlyList<(string, string)> ExtraHeaders => _extraHeaders;
  private readonly List<(string, int)> _intOptions = [];
  private readonly List<(string, string)> _stringOptions = [];
  private readonly List<(string, string)> _extraHeaders = [];

  /// <summary>
  /// Creates a default ClientOptions object with default authentication and Python scripting.
  /// </summary>
  public ClientOptions() {
    SetDefaultAuthentication();
    SetSessionType(SessionTypes.Python);
  }

  /// <summary>
  /// Modifies the ClientOptions object to set the default authentication scheme.
  /// </summary>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions SetDefaultAuthentication() {
    AuthorizationValue = "Anonymous";
    return this;
  }

  /// <summary>
  /// Modifies the ClientOptions object to set the basic authentication scheme.
  /// </summary>
  /// <param name="username">The username</param>
  /// <param name="password">The password</param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions SetBasicAuthentication(string username, string password) {
    var token = username + ':' + password;
    var tokenAsBytes = Encoding.ASCII.GetBytes(token);
    AuthorizationValue = "Basic " + Convert.ToBase64String(tokenAsBytes);
    return this;
  }

  /// <summary>
  /// Modifies the ClientOptions object to set a custom authentication scheme.
  /// </summary>
  /// <param name="authenticationKey">The key of the custom authentication scheme</param>
  /// <param name="authenticationValue">The value of the custom authentication scheme</param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions SetCustomAuthentication(string authenticationKey, string authenticationValue) {
    AuthorizationValue = authenticationKey + " " + authenticationValue;
    return this;
  }

  /// <summary>
  /// Modifies the ClientOptions object to set the scripting language for the session.
  /// </summary>
  /// <param name="sessionType">The scripting language for the session, such as "groovy" or "python"</param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions SetSessionType(string sessionType) {
    SessionType = sessionType;
    return this;
  }

  /// <summary>
  /// Configure whether to set server connections as TLS
  /// </summary>
  /// <param name="useTls"></param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions SetUseTls(bool useTls) {
    UseTls = useTls;
    return this;
  }

  /// <summary>
  /// Sets a PEM-encoded certificate root for server connections.  The empty string
  /// means use system defaults.
  /// </summary>
  /// <param name="tlsRootCerts">a PEM encoded certificate chain</param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions SetTlsRootCerts(string tlsRootCerts) {
    TlsRootCerts = tlsRootCerts;
    return this;
  }

  /// <summary>
  /// Sets a PEM-encoded certificate for the client and use mutual TLS.
  /// The empty string means don't use mutual TLS.
  /// </summary>
  /// <param name="clientCertChain">a PEM encoded certificate chain, or empty for no mutual TLS</param>
  /// <returns></returns>
  public ClientOptions SetClientCertChain(string clientCertChain) {
    ClientCertChain = clientCertChain;
    return this;
  }

  /// <summary>
  /// Sets a PEM-encoded private key for the client certificate chain when using
  /// mutual TLS.
  /// </summary>
  /// <param name="clientPrivateKey">a PEM encoded private key</param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions SetClientPrivateKey(string clientPrivateKey) {
    ClientPrivateKey = clientPrivateKey;
    return this;
  }

  /// <summary>
  /// Adds an int-valued option for the configuration of the underlying gRPC channels.
  /// See https://grpc.github.io/grpc/cpp/group__grpc__arg__keys.html for a list of available options.
  /// </summary>
  /// <example>copt.setIntOption("grpc.min_reconnect_backoff_ms", 2000)</example>
  /// <param name="opt">The option key</param>
  /// <param name="val">The option value</param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions AddIntOption(string opt, int val) {
    _intOptions.Add((opt, val));
    return this;
  }

  /// <summary>
  /// Adds a string-valued option for the configuration of the underlying gRPC channels.
  /// See https://grpc.github.io/grpc/cpp/group__grpc__arg__keys.html for a list of available options.
  /// </summary>
  /// <param name="opt">The option key</param>
  /// <param name="val">The option value</param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions AddStringOption(string opt, string val) {
    _stringOptions.Add((opt, val));
    return this;
  }

  /// <summary>
  /// Adds an extra header with a constant name and value to be sent with every outgoing server request.
  /// </summary>
  /// <param name="headerName"></param>
  /// <param name="headerValue"></param>
  /// <returns>this, so that methods can be chained.</returns>
  public ClientOptions AddExtraHeader(string headerName, string headerValue) {
    _extraHeaders.Add((headerName, headerValue));
    return this;
  }
}
