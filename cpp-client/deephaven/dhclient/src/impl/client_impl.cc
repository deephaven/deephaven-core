/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/client_impl.h"

#include <memory>
#include <mutex>
#include <stdexcept>
#include "deephaven/client/impl/table_handle_manager_impl.h"

using deephaven::client::impl::TableHandleManagerImpl;
using deephaven::client::server::Server;
using deephaven::client::utility::Executor;

namespace deephaven::client {
namespace impl {
std::shared_ptr<ClientImpl> ClientImpl::Create(
    std::shared_ptr<Server> server,
    std::shared_ptr<Executor> executor,
    std::shared_ptr<Executor> flight_executor,
    std::string session_type) {
  // No console here: TableHandleManagerImpl starts one on first use, keeping
  // Client::Connect free of the ConsoleService.StartConsole RPC.
  auto thmi = TableHandleManagerImpl::Create(
          std::move(session_type),
          std::move(server),
          std::move(executor),
          std::move(flight_executor));
  return std::make_shared<ClientImpl>(Private(), std::move(thmi));
}

ClientImpl::ClientImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&manager_impl) :
    manager_impl_(std::move(manager_impl)) {}

ClientImpl::~ClientImpl() = default;

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
  // We run shutdown hooks before actually shutting down the table
  // manager, which allows users to try server cleanup operations
  // through the client before closing.
  std::unique_lock lock(on_close_.mux);
  auto map = std::move(on_close_.map);
  lock.unlock();
  for (const auto &entry : map) {
    entry.second();
  }
  manager_impl_->Shutdown();
}
}  // namespace impl
}  // namespace deephaven::client
