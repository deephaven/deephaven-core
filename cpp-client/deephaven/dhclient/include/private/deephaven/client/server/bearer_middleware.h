/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <arrow/flight/client_middleware.h>
#include "deephaven/client/server/server_shared_state.h"

namespace deephaven::client::server {

/**
 * Middleware for managing Bearer token authentication in Arrow Flight calls.
 * This middleware:
 * - Adds the Bearer token to outgoing request headers
 * - Adds the envoy-prefix header (if present in extra headers) to outgoing request headers
 * - Extracts updated Bearer tokens from incoming response headers
 * - Uses Server's sessionToken_, extraHeaders_, and mutex_ for thread-safe access
 */
class BearerMiddleware : public arrow::flight::ClientMiddleware {
public:
  /**
   * Construct a BearerMiddleware with a reference to the ServerSharedState.
   * @param shared_state The state object, shared with Server.
   */
  BearerMiddleware(std::shared_ptr<ServerSharedState> shared_state);
  ~BearerMiddleware();

  /**
   * Called before sending headers. Adds the Bearer token to the authorization header.
   */
  void SendingHeaders(arrow::flight::AddCallHeaders *outgoing_headers) override;

  /**
   * Called when headers are received. Extracts and updates the Bearer token if present.
   * Note: Arrow Flight CallHeaders maps header names to single string values.
   */
  void ReceivedHeaders(const arrow::flight::CallHeaders &incoming_headers) override;

  /**
   * Called when the call is completed.
   */
  void CallCompleted(const arrow::Status& status) override;

private:
  std::shared_ptr<ServerSharedState> shared_state_;
};

/**
 * Factory for creating BearerMiddleware instances.
 */
class BearerMiddlewareFactory : public arrow::flight::ClientMiddlewareFactory {
public:
  /**
   * Construct a BearerMiddlewareFactory with a reference to the ServerSharedState.
   * @param shared_state The state object, shared with Server.
   */
  explicit BearerMiddlewareFactory(std::shared_ptr<ServerSharedState> shared_state);
  ~BearerMiddlewareFactory();

  /**
   * Called when a new call starts. Creates a BearerMiddleware instance.
   */
  void StartCall(const arrow::flight::CallInfo& info,
                 std::unique_ptr<arrow::flight::ClientMiddleware> *middleware) override;

private:
  std::shared_ptr<ServerSharedState> shared_state_;
};
}  // namespace deephaven::client::server
