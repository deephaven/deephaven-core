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
  auto result = std::make_shared<TableHandleManagerImpl>(Private(), std::move(consoleId),
      std::move(server), std::move(executor), std::move(flightExecutor));
  result->self_ = result;
  return result;
}

TableHandleManagerImpl::TableHandleManagerImpl(Private, std::optional<Ticket> &&consoleId,
    std::shared_ptr<Server> &&server, std::shared_ptr<Executor> &&executor,
    std::shared_ptr<Executor> &&flightExecutor) : consoleId_(std::move(consoleId)),
    server_(std::move(server)), executor_(std::move(executor)),
    flightExecutor_(std::move(flightExecutor)) {
}

TableHandleManagerImpl::~TableHandleManagerImpl() = default;

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::emptyTable(int64_t size) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(this);
  auto resultTicket = server_->emptyTableAsync(size, cb);
  return TableHandleImpl::create(self_.lock(), std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::fetchTable(std::string tableName) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(this);
  auto resultTicket = server_->fetchTableAsync(std::move(tableName), cb);
  return TableHandleImpl::create(self_.lock(), std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::timeTable(int64_t startTimeNanos,
    int64_t periodNanos) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(this);
  auto resultTicket = server_->timeTableAsync(startTimeNanos, periodNanos, std::move(cb));
  return TableHandleImpl::create(self_.lock(), std::move(resultTicket), std::move(ls));
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

std::tuple<std::shared_ptr<TableHandleImpl>, arrow::flight::FlightDescriptor>
TableHandleManagerImpl::newTicket() const {
  auto[ticket, fd] = server_->newTicketAndFlightDescriptor();

  CBPromise<Ticket> ticketPromise;
  ticketPromise.setValue(ticket);
  auto ls = std::make_shared<internal::LazyState>(server_, flightExecutor_,
      ticketPromise.makeFuture());
  auto th = TableHandleImpl::create(self_.lock(), std::move(ticket), std::move(ls));
  return std::make_tuple(std::move(th), std::move(fd));
}
}  // namespace deephaven::client::impl
