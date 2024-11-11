/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <utility>
#include <vector>

namespace deephaven::client {

class Client;

/**
 * The ClientOptions object is intended to be passed to Client::Connect(). For convenience, the mutating methods can be
 * chained.
 * @example auto client = Client::Connect("localhost:10000", ClientOptions().SetBasicAuthentication("foo", "bar").SetSessionType("groovy")
 */
class ClientOptions {
public:
  using int_options_t = std::vector<std::pair<std::string, int>>;
  using string_options_t = std::vector<std::pair<std::string, std::string>>;
  using extra_headers_t = std::vector<std::pair<std::string, std::string>>;

  /*
   * Default constructor. Creates a default ClientOptions object with default authentication and Python scripting.
   */
  ClientOptions();
  /**
   * Copy constructor
   */
  ClientOptions(const ClientOptions &other);
  /**
   * Move constructor
   */
  ClientOptions(ClientOptions &&other) noexcept;
  /**
   * Copy assigment operator.
   */
  ClientOptions &operator=(const ClientOptions &other);
  /**
   * Move assigment operator.
   */
  ClientOptions &operator=(ClientOptions &&other) noexcept;
  /**
   * Destructor
   */
  ~ClientOptions();

  /**
   * Modifies the ClientOptions object to set the default authentication scheme.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &SetDefaultAuthentication();
  /**
   * Modifies the ClientOptions object to set the basic authentication scheme.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &SetBasicAuthentication(const std::string &username, const std::string &password);
  /**
   * Modifies the ClientOptions object to set a custom authentication scheme.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &SetCustomAuthentication(const std::string &authentication_key,
      const std::string &authentication_value);
  /**
   * Modifies the ClientOptions object to set the scripting language for the session.
   * @param session_type The scripting language for the session, such as "groovy" or "python".
   * @return *this, so that methods can be chained.
   */
  ClientOptions &SetSessionType(std::string session_type);
  /**
   * Configure whether to set server connections as TLS
   *
   * @param use_tls true if server connections should be TLS/SSL, false for insecure.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &SetUseTls(bool use_tls);
  /**
   *
   * Sets a PEM-encoded certificate root for server connections.  The empty string
   * means use system defaults.
   *
   * @param tls_root_certs a PEM encoded certificate chain.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &SetTlsRootCerts(std::string tls_root_certs);
  /**
   * Sets a PEM-encoded certificate for the client and use mutual TLS.
   * The empty string means don't use mutual TLS.
   *
   * @param client_cert_chain a PEM encoded certificate chain, or empty for no mutual TLS.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &SetClientCertChain(std::string client_cert_chain);
  /**
   * Sets a PEM-encoded private key for the client certificate chain when using
   * mutual TLS.
   *
   * @param client_private_key a PEM encoded private key.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &SetClientPrivateKey(std::string client_private_key);
  /**
   * Adds an int-valued option for the configuration of the underlying gRPC channels.
   * See https://grpc.github.io/grpc/cpp/group__grpc__arg__keys.html for a list of available options.
   *
   * @example copt.setIntOption("grpc.min_reconnect_backoff_ms", 2000)
   * @param opt The option key.
   * @param val The option value.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &AddIntOption(std::string opt, int val);
  /**
   * Adds a string-valued option for the configuration of the underlying gRPC channels.
   * See https://grpc.github.io/grpc/cpp/group__grpc__arg__keys.html for a list of available options.
   *
   * @example copt.setStringOption("grpc.target_name_override", "idonthaveadnsforthishost")
   * @param opt The option key.
   * @param val The option value.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &AddStringOption(std::string opt, std::string val);
  /**
   * Adds an extra header with a constant name and value to be sent with every outgoing server request.
   *
   * @param header_name The header name
   * @param header_value The header value
   * @return *this, so that methods can be chained.
   */
  ClientOptions &AddExtraHeader(std::string header_name, std::string header_value);
  /**
   * Returns the value for the authorization header that will be sent to the server
   * on the first request; this value is a function of the
   * authentication method selected.
   *
   * @return A string value for the authorization header
   */
  [[nodiscard]]
  const std::string &AuthorizationValue() const {
    return authorizationValue_;
  }
    
  /**
   * Returns true if server connections should be configured for TLS/SSL.
   *
   * @return true if this connection should be TLS/SSL, false for insecure.
   */
  [[nodiscard]]
  bool UseTls() const { return useTls_; }
  /**
   * The PEM-encoded certificate root for server connections, or the empty string
   * if using system defaults.
   *
   * @return A PEM-encoded certificate chain, or empty.
   */
  [[nodiscard]]
  const std::string &TlsRootCerts() const { return tlsRootCerts_; }
  /**
   * The PEM-encoded certificate chain to use for the client
   * when using mutual TLS, or the empty string for no mutual TLS.
   *
   * @return A PEM-encoded certificate chain, or empty.
   */
  [[nodiscard]]
  const std::string &ClientCertChain() const { return clientCertChain_; }
  /**
   * The PEM-encoded client private key to use for mutual TLS.
   *
   * @return A PEM-encoded private key, or empty.
   */
  [[nodiscard]]
  const std::string &ClientPrivateKey() const { return clientPrivateKey_; }
  /**
   * Integer-valued channel options set for server connections.
   *
   * @return A vector of pairs of string option name and integer option value
   */
  [[nodiscard]]
  const int_options_t &IntOptions() const { return intOptions_; }
  /**
   * String-valued channel options set for server connections.
   *
   * @return A vector of pairs of string option name and string option value
   */
  [[nodiscard]]
  const string_options_t &StringOptions() const { return stringOptions_; }
  /**
   * Extra headers that should be sent with each outgoing server request.
   *
   * @return A vector of pairs of string header name and string header value
   */
  [[nodiscard]]
  const extra_headers_t &ExtraHeaders() const { return extraHeaders_; }

private:
  std::string authorizationValue_;
  std::string sessionType_;
  bool useTls_ = false;
  std::string tlsRootCerts_;
  std::string clientCertChain_;
  std::string clientPrivateKey_;
  int_options_t intOptions_;
  string_options_t stringOptions_;
  extra_headers_t extraHeaders_;

  friend class Client;
};

}  // namespace deephaven::client
