/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven/client/ticking.h"
#include "deephaven/client/server/server.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/misc.h"
#include "deephaven/proto/ticket.pb.h"

namespace deephaven::client::subscription {
std::shared_ptr<SubscriptionHandle> startSubscribeThread(
    std::shared_ptr<deephaven::client::server::Server> server,
    deephaven::client::utility::Executor *flightExecutor,
    std::shared_ptr<deephaven::client::utility::ColumnDefinitions> columnDefinitions,
    const io::deephaven::proto::backplane::grpc::Ticket &ticket,
    std::shared_ptr<TickingCallback> callback);
}  // namespace deephaven::client::subscription
