/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven/client/server/server.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven_core/proto/ticket.pb.h"

namespace deephaven::client::subscription {
class SubscriptionThread {
  using Server = deephaven::client::server::Server;
  using Executor = deephaven::client::utility::Executor;
  using Schema = deephaven::dhcore::clienttable::Schema;
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;
  using TickingCallback = deephaven::dhcore::ticking::TickingCallback;

public:
  [[nodiscard]]
  static std::shared_ptr<SubscriptionHandle> Start(std::shared_ptr<Server> server,
      Executor *flight_executor, std::shared_ptr<Schema> schema,
      const Ticket &ticket, std::shared_ptr<TickingCallback> callback);
};
}  // namespace deephaven::client::subscription
