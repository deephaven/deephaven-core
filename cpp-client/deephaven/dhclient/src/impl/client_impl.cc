/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/client_impl.h"

#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
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

ClientImpl::~ClientImpl() = default;

ClientImpl::OnCloseCbId ClientImpl::AddOnCloseCallback(OnCloseCb cb) {
  std::unique_lock lock(on_close_mux_);
  if (on_close_cb_ctx_ == nullptr) {
    on_close_cb_ctx_ = std::make_unique<OnCloseCbContext>();
  }
  OnCloseCbId id({on_close_cb_ctx_->next_id_++});
  on_close_cb_ctx_->map_[id] = std::move(cb);
  return id;
}

bool ClientImpl::RemoveOnCloseCallback(OnCloseCbId cb_id) {
  std::unique_lock lock(on_close_mux_);
  if (on_close_cb_ctx_ == nullptr) {
    return false;
  }
  return on_close_cb_ctx_->map_.erase(std::move(cb_id)) > 0;
}

void ClientImpl::Shutdown() {
  std::unique_lock lock(on_close_mux_);
  manager_impl_->Shutdown();
  if (on_close_cb_ctx_ == nullptr) {
    return;
  }
  for (const auto &entry : on_close_cb_ctx_->map_) {
    entry.second();
  }
  on_close_cb_ctx_.reset();
}
}  // namespace impl
}  // namespace deephaven::client
