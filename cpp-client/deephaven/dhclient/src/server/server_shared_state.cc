/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/server/server_shared_state.h"

namespace deephaven::client::server {
ServerSharedState::ServerSharedState(ClientOptions::extra_headers_t extra_headers,
  std::string session_token,
  std::chrono::milliseconds expiration_interval,
  std::chrono::system_clock::time_point next_handshake_time
) : extraHeaders_(std::move(extra_headers)),
   nextFreeTicketId_(1),
   sessionToken_(std::move(session_token)),
   expirationInterval_(expiration_interval),
   nextHandshakeTime_(next_handshake_time) {
}

ServerSharedState::~ServerSharedState() = default;
}  // namespace deephaven::client::server
