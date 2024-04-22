/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/client_options.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::Base64Encode;

namespace deephaven::client {

ClientOptions::ClientOptions() {
  SetDefaultAuthentication();
  SetSessionType("python");
}

ClientOptions::ClientOptions(const ClientOptions &other) = default;
ClientOptions::ClientOptions(ClientOptions &&other) noexcept = default;
ClientOptions &ClientOptions::operator=(const ClientOptions &other) = default;
ClientOptions &ClientOptions::operator=(ClientOptions &&other) noexcept = default;

ClientOptions::~ClientOptions() = default;

ClientOptions &ClientOptions::SetDefaultAuthentication() {
  authorizationValue_ = "Anonymous";
  return *this;
}

ClientOptions &ClientOptions::SetBasicAuthentication(const std::string &username, const std::string &password) {
  auto token = username + ':' + password;
  authorizationValue_ = "Basic " + Base64Encode(token);
  return *this;
}

ClientOptions &ClientOptions::SetCustomAuthentication(const std::string &authenticationKey,
    const std::string &authenticationValue) {
  authorizationValue_ = authenticationKey + " " + authenticationValue;
  return *this;
}

ClientOptions &ClientOptions::SetSessionType(std::string sessionType) {
  this->sessionType_ = std::move(sessionType);
  return *this;
}

ClientOptions &ClientOptions::SetUseTls(const bool use_tls) {
  useTls_ = use_tls;
  return *this;
}

ClientOptions &ClientOptions::SetTlsRootCerts(std::string tlsRootCerts) {
  tlsRootCerts_ = std::move(tlsRootCerts);
  return *this;
}

ClientOptions &ClientOptions::SetClientCertChain(std::string client_cert_chain) {
  clientCertChain_ = std::move(client_cert_chain);
  return *this;
}

ClientOptions &ClientOptions::SetClientPrivateKey(std::string client_private_key) {
  clientPrivateKey_ = std::move(client_private_key);
  return *this;
}

ClientOptions &ClientOptions::AddIntOption(std::string opt, int val) {
  intOptions_.emplace_back(std::move(opt), val);
  return *this;
}

ClientOptions &ClientOptions::AddStringOption(std::string opt, std::string val) {
  stringOptions_.emplace_back(std::move(opt), std::move(val));
  return *this;
}

ClientOptions &ClientOptions::AddExtraHeader(std::string header_name, std::string header_value) {
  extraHeaders_.emplace_back(std::move(header_name), std::move(header_value));
  return *this;
}

}  // namespace deephaven::client
