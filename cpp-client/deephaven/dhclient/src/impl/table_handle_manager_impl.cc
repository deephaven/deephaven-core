/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/table_handle_manager_impl.h"

#include <map>
#include <grpc/support/log.h>
#include "deephaven/client/utility/executor.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/dhcore/utility/callbacks.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::Callback;
using deephaven::dhcore::utility::CBPromise;
using deephaven::client::utility::Executor;
using deephaven::dhcore::utility::SFCallback;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;
using deephaven::dhcore::utility::ObjectId;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse;


namespace deephaven::client::impl {
std::shared_ptr<TableHandleManagerImpl> TableHandleManagerImpl::Create(std::optional<Ticket> console_id,
    std::shared_ptr<ServerType> server, std::shared_ptr<ExecutorType> executor,
    std::shared_ptr<ExecutorType> flight_executor) {
  return std::make_shared<TableHandleManagerImpl>(Private(), std::move(console_id),
      std::move(server), std::move(executor), std::move(flight_executor));
}

TableHandleManagerImpl::TableHandleManagerImpl(Private, std::optional<Ticket> &&console_id,
    std::shared_ptr<ServerType> &&server, std::shared_ptr<ExecutorType> &&executor,
    std::shared_ptr<ExecutorType> &&flight_executor) :
    me_(deephaven::dhcore::utility::ObjectId("TableHandleManagerImpl", this)),
    consoleId_(std::move(console_id)),
    server_(std::move(server)),
    executor_(std::move(executor)),
    flightExecutor_(std::move(flight_executor)) {
  gpr_log(GPR_DEBUG, "%s: Created.", me_.c_str());
}

TableHandleManagerImpl::~TableHandleManagerImpl() {
  gpr_log(GPR_DEBUG,"%s: Destroyed.", me_.c_str());
}

void TableHandleManagerImpl::Shutdown() {
  for (const auto &sub : subscriptions_) {
    sub->Cancel();
  }
  executor_->Shutdown();
  flightExecutor_->Shutdown();
  server_->Shutdown();
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::EmptyTable(int64_t size) {
  auto result_ticket = server_->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(nullptr, this, result_ticket);
  server_->EmptyTableAsync(size, cb, result_ticket);
  return TableHandleImpl::Create(shared_from_this(), std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::FetchTable(std::string table_name) {
  auto result_ticket = server_->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(nullptr, this, result_ticket);
  server_->FetchTableAsync(std::move(table_name), cb, result_ticket);
  return TableHandleImpl::Create(shared_from_this(), std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::TimeTable(DurationSpecifier period,
    TimePointSpecifier start_time, bool blink_table) {
  auto result_ticket = server_->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(nullptr, this, result_ticket);
  server_->TimeTableAsync(std::move(period), std::move(start_time), blink_table, std::move(cb),
      result_ticket);
  return TableHandleImpl::Create(shared_from_this(), std::move(result_ticket), std::move(ls));
}

void TableHandleManagerImpl::RunScriptAsync(std::string code, std::shared_ptr<SFCallback<>> callback) {
  struct cb_t final : public SFCallback<ExecuteCommandResponse> {
    explicit cb_t(std::shared_ptr<SFCallback<>> outer_cb) : outerCb_(std::move(outer_cb)) {}

    void OnSuccess(ExecuteCommandResponse /*item*/) final {
      outerCb_->OnSuccess();
    }

    void OnFailure(std::exception_ptr ep) final {
      outerCb_->OnFailure(std::move(ep));
    }

    std::shared_ptr<SFCallback<>> outerCb_;
  };
  if (!consoleId_.has_value()) {
    auto eptr = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(
        "Client was created without specifying a script language")));
    callback->OnFailure(std::move(eptr));
    return;
  }
  auto cb = std::make_shared<cb_t>(std::move(callback));
  server_->ExecuteCommandAsync(*consoleId_, std::move(code), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::MakeTableHandleFromTicket(std::string ticket) {
  Ticket result_ticket;
  *result_ticket.mutable_ticket() = std::move(ticket);
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(nullptr, this, result_ticket);
  server_->GetExportedTableCreationResponseAsync(result_ticket, std::move(cb));
  return TableHandleImpl::Create(shared_from_this(), std::move(result_ticket), std::move(ls));
}

void TableHandleManagerImpl::AddSubscriptionHandle(std::shared_ptr<SubscriptionHandle> handle) {
  std::unique_lock guard(mutex_);
  subscriptions_.insert(std::move(handle));
}

void TableHandleManagerImpl::RemoveSubscriptionHandle(const std::shared_ptr<SubscriptionHandle> &handle) {
  std::unique_lock guard(mutex_);
  subscriptions_.erase(handle);
}
}  // namespace deephaven::client::impl
