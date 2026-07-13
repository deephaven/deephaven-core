/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "deephaven/client/client_options.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven_core/proto/ticket.pb.h"
#include "deephaven_core/proto/application.pb.h"
#include "deephaven_core/proto/config.pb.h"

namespace deephaven::client::server {

namespace internal {
struct TicketLess {
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;

  bool operator()(const Ticket &lhs, const Ticket &rhs) const {
    return lhs.ticket() < rhs.ticket();
  }
};
}  // namespace internal

/**
 * This struct is shared between Server, BearerMiddleware, and BearerMiddlewareFactory.
 */
struct ServerSharedState {
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;

  ServerSharedState(ClientOptions::extra_headers_t extra_headers,
      std::string session_token,
      std::chrono::milliseconds expiration_interval,
      std::chrono::system_clock::time_point next_handshake_time);

  ~ServerSharedState();

  const ClientOptions::extra_headers_t extraHeaders_;
  std::mutex mutex_;
  std::condition_variable condVar_;
  int32_t nextFreeTicketId_ = 1;
  bool cancelled_ = false;
  std::set<Ticket, internal::TicketLess> outstanding_tickets_;
  std::string sessionToken_;
  std::chrono::milliseconds expirationInterval_;
  std::chrono::system_clock::time_point nextHandshakeTime_;
  std::thread keepAliveThread_;
};
}  // namespace deephaven::client::server
