/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>

namespace deephaven::client {

class Client;
 
/**
 * The ClientOptions object is intended to be passed to Client::connect(). For convenience, the mutating methods can be
 * chained. For example:
 * auto client = Client::connect("localhost:10000", ClientOptions().setBasicAuthentication("foo", "bar").setSessionType("groovy")
 */
class ClientOptions {
public:
  /*
   * Default constructor. Creates a default ClientOptions object with default authentication and Python scripting.
   */
  ClientOptions();
  /**
   * Move constructor
   */
  ClientOptions(ClientOptions &&other) noexcept;
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
  ClientOptions &setDefaultAuthentication();
  /**
   * Modifies the ClientOptions object to set the basic authentication scheme.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &setBasicAuthentication(const std::string &username, const std::string &password);
  /**
   * Modifies the ClientOptions object to set a custom authentication scheme.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &setCustomAuthentication(const std::string &authenticationKey, const std::string &authenticationValue);
  /**
   * Modifies the ClientOptions object to set the scripting language for the session.
   * @param sessionType The scripting language for the session, such as "groovy" or "python".
   * @return *this, so that methods can be chained.
   */
  ClientOptions &setSessionType(const std::string &sessionType);

private:
  std::string authorizationValue_;
  std::string sessionType_;

  friend class Client;
};

}  // namespace deephaven::client
