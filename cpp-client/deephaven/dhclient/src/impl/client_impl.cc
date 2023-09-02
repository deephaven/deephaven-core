/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/client_impl.h"

#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>

#include <grpcpp/grpcpp.h>

#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/dhcore/utility/callbacks.h"

using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::StartConsoleResponse;

using deephaven::client::impl::TableHandleManagerImpl;
using deephaven::client::server::Server;
using deephaven::client::utility::Executor;

namespace deephaven::client {
namespace impl {
std::shared_ptr<ClientImpl> ClientImpl::Create(
    std::shared_ptr<Server> server,
    std::shared_ptr<Executor> executor,
    std::shared_ptr<Executor> flight_executor,
    const std::string &session_type) {
  std::optional<Ticket> console_ticket;
  if (!session_type.empty()) {
    auto cb = SFCallback<StartConsoleResponse>::CreateForFuture();
    server->StartConsoleAsync(session_type, std::move(cb.first));
    StartConsoleResponse scr = std::move(std::get<0>(cb.second.get()));
    console_ticket = std::move(*scr.mutable_result_id());
  }

  auto thmi = TableHandleManagerImpl::Create(
          std::move(console_ticket),
          std::move(server),
          std::move(executor),
          std::move(flight_executor));
  return std::make_shared<ClientImpl>(Private(), std::move(thmi));
}

ClientImpl::ClientImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&manager_impl) :
    manager_impl_(std::move(manager_impl)) {}

// There is only one Client associated with the server connection. ClientImpls cannot be
// copied. When the Client owning the state is destructed, we tear down the state via close().
ClientImpl::~ClientImpl() {
  try {
    Shutdown();
  } catch (std::exception &ex) {
    gpr_log(GPR_ERROR,
        "ClientImpl(%p): exception during destructor's call to shutdown: %s.",
        static_cast<void*>(this), ex.what());
  } catch (...) {
    gpr_log(GPR_ERROR,
        "ClientImpl(%p): unknown exception during destructor's call to shutdown.",
        static_cast<void*>(this));
  }
}

ClientImpl::OnCloseCbId ClientImpl::AddOnCloseCallback(OnCloseCb cb) {
  std::unique_lock lock(on_close_.mux);
  OnCloseCbId id({on_close_.next_id++});
  on_close_.map[id] = std::move(cb);
  return id;
}

bool ClientImpl::RemoveOnCloseCallback(OnCloseCbId cb_id) {
  std::unique_lock lock(on_close_.mux);
  return on_close_.map.erase(std::move(cb_id)) > 0;
}

void ClientImpl::Shutdown() {
  // Move to local variable to be defensive.
  auto temp = std::move(manager_impl_);
  if (temp == nullptr) {
    return;
  }
  const void *const v_this = static_cast<void*>(this);
  gpr_log(GPR_DEBUG, "ClientImpl(%p): Shutdown starting...", v_this);
  temp->Shutdown();
  std::unique_lock lock(on_close_.mux);
  auto map = std::move(on_close_.map);
  gpr_log(GPR_DEBUG, "ClientImpl(%p): Executing on close callbacks...", v_this);
  lock.unlock();
  std::size_t cb_num = 0;
  for (const auto &entry : map) {
    ++cb_num;
    gpr_log(GPR_DEBUG, "ClientImpl(%p): Executing on close call %lu.", v_this, cb_num);
    entry.second();
  }
  gpr_log(GPR_DEBUG, "ClientImpl(%p): Shutdown complete.", v_this);
}
}  // namespace impl
}  // namespace deephaven::client
