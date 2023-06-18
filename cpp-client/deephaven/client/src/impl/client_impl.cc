/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/client_impl.h"

#include <memory>
#include <optional>
#include <stdexcept>
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/dhcore/utility/callbacks.h"

using io::deephaven::proto::backplane::grpc::HandshakeResponse;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::StartConsoleResponse;

using deephaven::client::impl::TableHandleManagerImpl;
using deephaven::client::server::Server;
using deephaven::client::utility::Executor;
using deephaven::dhcore::utility::SFCallback;

namespace deephaven::client {
namespace impl {
std::shared_ptr<ClientImpl> ClientImpl::create(std::shared_ptr<Server> server,
    std::shared_ptr<Executor> executor, std::shared_ptr<Executor> flightExecutor, const std::string &sessionType) {
  std::optional<Ticket> consoleTicket;
  if (!sessionType.empty()) {
    auto cb = SFCallback<StartConsoleResponse>::createForFuture();
    server->startConsoleAsync(sessionType, std::move(cb.first));
    StartConsoleResponse scr = std::move(std::get<0>(cb.second.get()));
    consoleTicket = std::move(*scr.mutable_result_id());
  }

  auto thmi = TableHandleManagerImpl::create(std::move(consoleTicket), std::move(server), std::move(executor),
      std::move(flightExecutor));
  return std::make_shared<ClientImpl>(Private(), std::move(thmi));
}

ClientImpl::ClientImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&managerImpl) :
    managerImpl_(std::move(managerImpl)) {}

ClientImpl::~ClientImpl() = default;
}  // namespace impl
}  // namespace deephaven::client
