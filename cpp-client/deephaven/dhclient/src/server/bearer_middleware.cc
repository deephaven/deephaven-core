/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/server/bearer_middleware.h"
#include "deephaven/client/utility/logging.h"
#include <absl/log/log.h>
#include <cstring>

using deephaven::client::kAuthorizationHeader;
using deephaven::client::kBearerPrefix;
using deephaven::client::kEnvoyPrefixHeader;

namespace deephaven::client::server {

// BearerMiddleware implementation

BearerMiddleware::BearerMiddleware(std::shared_ptr<ServerSharedState> shared_state) :
  shared_state_(std::move(shared_state)) {}

BearerMiddleware::~BearerMiddleware() = default;

void BearerMiddleware::SendingHeaders(arrow::flight::AddCallHeaders *outgoing_headers) {
  std::unique_lock<std::mutex> lock(shared_state_->mutex_);
  VLOG(2) << "BearerMiddleware::SendingHeaders called";

  // Add authorization header
  if (!shared_state_->sessionToken_.empty()) {
    // session_token_ already contains the full authorization value (e.g., "Bearer abc123")
    // Don't add the prefix again!
    outgoing_headers->AddHeader(kAuthorizationHeader, shared_state_->sessionToken_);
    VLOG(2) << "BearerMiddleware: Added authorization header, value length: " <<
      shared_state_->sessionToken_.size();
  } else {
    LOG(ERROR) << "BearerMiddleware: Session token is EMPTY! Cannot add authorization header";
  }

  // Add envoy-prefix header if present in extra headers
  for (const auto &header : shared_state_->extraHeaders_) {
    if (header.first == kEnvoyPrefixHeader) {
      outgoing_headers->AddHeader(kEnvoyPrefixHeader, header.second);
      VLOG(2) << "BearerMiddleware: Added envoy-prefix header, value: " << header.second;
      break;  // Only one envoy-prefix expected
    }
  }
}

void BearerMiddleware::ReceivedHeaders(const arrow::flight::CallHeaders &incoming_headers) {
  // Look for authorization header in incoming headers
  auto auth_headers = incoming_headers.find(kAuthorizationHeader);

  if (auth_headers == incoming_headers.end()) {
    return;
  }

  // Convert the header value to string
  // Arrow Flight may return string_view, so we explicitly convert
  std::string auth_value = std::string(auth_headers->second);

  // Check if this value starts with "Bearer " - only update if it's a Bearer token
  size_t prefix_len = strlen(kBearerPrefix);
  if (auth_value.size() > prefix_len &&
      auth_value.compare(0, prefix_len, kBearerPrefix) == 0) {
    // Store the FULL authorization value (including "Bearer " prefix)
    // This matches what SendingHeaders expects
    std::unique_lock lock(shared_state_->mutex_);
    shared_state_->sessionToken_ = std::move(auth_value);
    VLOG(2) << "BearerMiddleware: Updated session token from response headers";
  }
}

void BearerMiddleware::CallCompleted(const arrow::Status &status) {
  // Nothing to do on call completion
}

// BearerMiddlewareFactory implementation

BearerMiddlewareFactory::BearerMiddlewareFactory(std::shared_ptr<ServerSharedState> shared_state) :
  shared_state_(std::move(shared_state)) {}

BearerMiddlewareFactory::~BearerMiddlewareFactory() = default;

void BearerMiddlewareFactory::StartCall(
    const arrow::flight::CallInfo &info,
    std::unique_ptr<arrow::flight::ClientMiddleware> *middleware) {
  *middleware = std::make_unique<BearerMiddleware>(shared_state_);
}
}  // namespace deephaven::client::server
