/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/client_options.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::base64Encode;

namespace deephaven::client {

ClientOptions::ClientOptions() {
  setDefaultAuthentication();
  setSessionType("python");
}

ClientOptions::ClientOptions(ClientOptions &&other) noexcept = default;
ClientOptions &ClientOptions::operator=(ClientOptions &&other) noexcept = default;
ClientOptions::~ClientOptions() = default;

ClientOptions &ClientOptions::setDefaultAuthentication() {
  authorizationValue_ = "Anonymous";
  return *this;
}

ClientOptions &ClientOptions::setBasicAuthentication(const std::string &username, const std::string &password) {
  auto token = username + ':' + password;
  authorizationValue_ = "Basic " + base64Encode(token);
  return *this;
}

ClientOptions &ClientOptions::setCustomAuthentication(const std::string &authenticationType,
    const std::string &authenticationToken) {
  authorizationValue_ = authenticationType + " " + authenticationToken;
  return *this;
}

ClientOptions &ClientOptions::setSessionType(const std::string &sessionType) {
  this->sessionType_ = sessionType;
  return *this;
}

ClientOptions &ClientOptions::setUseTls(const bool useTls) {
  useTls_ = useTls;
  return *this;
}

ClientOptions &ClientOptions::setPem(const std::string pem) {
  pem_ = std::move(pem);
  return *this;
}

ClientOptions &ClientOptions::addIntOption(const std::string opt, const int val) {
  intOptions_.emplace_back(std::move(opt), val);
  return *this;
}

ClientOptions &ClientOptions::addStringOption(const std::string opt, const std::string val) {
  stringOptions_.emplace_back(std::move(opt), std::move(val));
  return *this;
}

ClientOptions &ClientOptions::addExtraHeader(std::string header_name, std::string header_value) {
  extraHeaders_.emplace_back(std::move(header_name), std::move(header_value));
  return *this;
}

}  // namespace deephaven::client
