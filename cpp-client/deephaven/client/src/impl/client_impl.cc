/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/client_impl.h"

#include <stdexcept>
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/utility/utility.h"
#include "deephaven/client/utility/callbacks.h"

using io::deephaven::proto::backplane::grpc::HandshakeResponse;
using io::deephaven::proto::backplane::script::grpc::StartConsoleResponse;
using deephaven::client::impl::TableHandleManagerImpl;
using deephaven::client::server::Server;
using deephaven::client::utility::Executor;
using deephaven::client::utility::SFCallback;
using deephaven::client::utility::stringf;
using deephaven::client::utility::streamf;

namespace deephaven::client {
namespace impl {
std::shared_ptr<ClientImpl> ClientImpl::create(std::shared_ptr<Server> server,
    std::shared_ptr<Executor> executor, std::shared_ptr<Executor> flightExecutor) {
  auto cb1 = SFCallback<HandshakeResponse>::createForFuture();
  server->newSessionAsync(std::move(cb1.first));
  HandshakeResponse hr = std::move(std::get<0>(cb1.second.get()));

  std::string mh = std::move(*hr.mutable_metadata_header());
  std::string st = std::move(*hr.mutable_session_token());
  std::transform(mh.begin(), mh.end(), mh.begin(), ::tolower);
  server->setAuthentication(std::move(mh), std::move(st));

  auto cb2 = SFCallback<StartConsoleResponse>::createForFuture();
  server->startConsoleAsync(std::move(cb2.first));
  StartConsoleResponse scr = std::move(std::get<0>(cb2.second.get()));

  auto thmi = TableHandleManagerImpl::create(std::move(*scr.mutable_result_id()),
      std::move(server), std::move(executor), std::move(flightExecutor));
  auto result = std::make_shared<ClientImpl>(Private(), std::move(thmi));
  return result;
}

ClientImpl::ClientImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&managerImpl) :
    managerImpl_(std::move(managerImpl)) {}

ClientImpl::~ClientImpl() = default;
}  // namespace impl
}  // namespace deephaven::client
