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

}  // namespace deephaven::client
