/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/table_handle_manager_impl.h"

#include <map>
#include "deephaven/client/utility/executor.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/dhcore/utility/callbacks.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::Callback;
using deephaven::dhcore::utility::CBPromise;
using deephaven::client::utility::Executor;
using deephaven::dhcore::utility::SFCallback;
using deephaven::dhcore::utility::streamf;
using deephaven::dhcore::utility::stringf;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse;


namespace deephaven::client::impl {
std::shared_ptr<TableHandleManagerImpl> TableHandleManagerImpl::create(std::optional<Ticket> consoleId,
    std::shared_ptr<Server> server, std::shared_ptr<Executor> executor,
    std::shared_ptr<Executor> flightExecutor) {
  return std::make_shared<TableHandleManagerImpl>(Private(), std::move(consoleId),
      std::move(server), std::move(executor), std::move(flightExecutor));
}

TableHandleManagerImpl::TableHandleManagerImpl(Private, std::optional<Ticket> &&consoleId,
    std::shared_ptr<Server> &&server, std::shared_ptr<Executor> &&executor,
    std::shared_ptr<Executor> &&flightExecutor) : consoleId_(std::move(consoleId)),
    server_(std::move(server)), executor_(std::move(executor)),
    flightExecutor_(std::move(flightExecutor)) {
}

TableHandleManagerImpl::~TableHandleManagerImpl() = default;

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::emptyTable(int64_t size) {
  auto resultTicket = server_->newTicket();
  auto [cb, ls] = TableHandleImpl::createEtcCallback(nullptr, this, resultTicket);
  server_->emptyTableAsync(size, cb, resultTicket);
  return TableHandleImpl::create(shared_from_this(), std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::fetchTable(std::string tableName) {
  auto resultTicket = server_->newTicket();
  auto [cb, ls] = TableHandleImpl::createEtcCallback(nullptr, this, resultTicket);
  server_->fetchTableAsync(std::move(tableName), cb, resultTicket);
  return TableHandleImpl::create(shared_from_this(), std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::timeTable(int64_t startTimeNanos,
    int64_t periodNanos) {
  auto resultTicket = server_->newTicket();
  auto [cb, ls] = TableHandleImpl::createEtcCallback(nullptr, this, resultTicket);
  server_->timeTableAsync(startTimeNanos, periodNanos, std::move(cb), resultTicket);
  return TableHandleImpl::create(shared_from_this(), std::move(resultTicket), std::move(ls));
}

void TableHandleManagerImpl::runScriptAsync(std::string code, std::shared_ptr<SFCallback<>> callback) {
  struct cb_t final : public SFCallback<ExecuteCommandResponse> {
    explicit cb_t(std::shared_ptr<SFCallback<>> outerCb) : outerCb_(std::move(outerCb)) {}

    void onSuccess(ExecuteCommandResponse /*item*/) final {
      outerCb_->onSuccess();
    }

    void onFailure(std::exception_ptr ep) final {
      outerCb_->onFailure(std::move(ep));
    }

    std::shared_ptr<SFCallback<>> outerCb_;
  };
  if (!consoleId_.has_value()) {
    auto eptr = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(
        "Client was created without specifying a script language")));
    callback->onFailure(std::move(eptr));
    return;
  }
  auto cb = std::make_shared<cb_t>(std::move(callback));
  server_->executeCommandAsync(*consoleId_, std::move(code), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::makeTableHandleFromTicket(std::string ticket) {
  Ticket resultTicket;
  *resultTicket.mutable_ticket() = std::move(ticket);
  auto [cb, ls] = TableHandleImpl::createEtcCallback(nullptr, this, resultTicket);
  server_->getExportedTableCreationResponseAsync(resultTicket, std::move(cb));
  return TableHandleImpl::create(shared_from_this(), std::move(resultTicket), std::move(ls));
}
}  // namespace deephaven::client::impl
