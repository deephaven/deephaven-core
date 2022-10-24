/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/table_handle_manager_impl.h"

#include <map>
#include "deephaven/client/utility/utility.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/client/impl/table_handle_impl.h"

using deephaven::client::utility::Callback;
using deephaven::client::utility::Executor;
using deephaven::client::utility::SFCallback;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven::client::impl {
std::shared_ptr<TableHandleManagerImpl> TableHandleManagerImpl::create(Ticket consoleId,
    std::shared_ptr<Server> server, std::shared_ptr<Executor> executor,
    std::shared_ptr<Executor> flightExecutor) {
  auto result = std::make_shared<TableHandleManagerImpl>(Private(), std::move(consoleId),
      std::move(server), std::move(executor), std::move(flightExecutor));
  result->self_ = result;
  return result;
}

TableHandleManagerImpl::TableHandleManagerImpl(Private, Ticket &&consoleId,
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

std::tuple<std::shared_ptr<TableHandleImpl>, arrow::flight::FlightDescriptor>
TableHandleManagerImpl::newTicket() const {
  auto[ticket, fd] = server_->newTicketAndFlightDescriptor();

  utility::CBPromise<Ticket> ticketPromise;
  ticketPromise.setValue(ticket);
  auto ls = std::make_shared<internal::LazyState>(server_, flightExecutor_,
      ticketPromise.makeFuture());
  auto th = TableHandleImpl::create(self_.lock(), std::move(ticket), std::move(ls));
  return std::make_tuple(std::move(th), std::move(fd));
}
}  // namespace deephaven::client::impl
