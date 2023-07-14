/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven/client/server/server.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/proto/ticket.pb.h"

namespace deephaven::client::subscription {
class SubscriptionThread {
  typedef deephaven::client::server::Server Server;
  typedef deephaven::client::utility::Executor Executor;
  typedef deephaven::dhcore::clienttable::Schema Schema;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef deephaven::dhcore::ticking::TickingCallback TickingCallback;

public:
  static std::shared_ptr<SubscriptionHandle> start(std::shared_ptr<Server> server,
      Executor *flightExecutor, std::shared_ptr<Schema> schema,
      const Ticket &ticket, std::shared_ptr<TickingCallback> callback);
};
}  // namespace deephaven::client::subscription
