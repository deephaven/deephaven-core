/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/table_handle_manager_impl.h"

#include <map>
#include <grpc/support/log.h>
#include "deephaven/client/utility/executor.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/client/impl/util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::impl::MoveVectorData;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;
using deephaven::dhcore::utility::ObjectId;
using io::deephaven::proto::backplane::grpc::CreateInputTableRequest;
using io::deephaven::proto::backplane::grpc::EmptyTableRequest;
using io::deephaven::proto::backplane::grpc::FetchTableRequest;
using io::deephaven::proto::backplane::grpc::TimeTableRequest;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandRequest;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse;

namespace deephaven::client::impl {
namespace {
Ticket MakeScopeReference(std::string_view table_name);
}  // namespace

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
  EmptyTableRequest req;
  *req.mutable_result_id() = server_->NewTicket();
  req.set_size(size);
  ExportedTableCreationResponse resp;
  server_->SendRpc([&](grpc::ClientContext *ctx) {
    return server_->TableStub()->EmptyTable(ctx, req, &resp);
  });
  return TableHandleImpl::Create(shared_from_this(), std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::FetchTable(std::string table_name) {
  FetchTableRequest req;
  *req.mutable_result_id() = server_->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = MakeScopeReference(table_name);
  ExportedTableCreationResponse resp;
  server_->SendRpc([&](grpc::ClientContext *ctx) {
    return server_->TableStub()->FetchTable(ctx, req, &resp);
  });
  return TableHandleImpl::Create(shared_from_this(), std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::TimeTable(DurationSpecifier period,
    TimePointSpecifier start_time, bool blink_table) {
  struct DurationVisitor {
    void operator()(std::chrono::nanoseconds nsecs) const {
      req->set_period_nanos(nsecs.count());
    }
    void operator()(int64_t nsecs) const {
      req->set_period_nanos(nsecs);
    }
    void operator()(std::string duration_text) const {
      *req->mutable_period_string() = std::move(duration_text);
    }

    TimeTableRequest *req = nullptr;
  };

  struct TimePointVisitor {
    void operator()(std::chrono::system_clock::time_point start) const {
      auto as_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(start.time_since_epoch());
      req->set_start_time_nanos(as_duration.count());
    }
    void operator()(int64_t nsecs) const {
      req->set_start_time_nanos(nsecs);
    }
    void operator()(std::string start_time_text) const {
      *req->mutable_start_time_string() = std::move(start_time_text);
    }

    TimeTableRequest *req = nullptr;
  };

  TimeTableRequest req;
  *req.mutable_result_id() = server_->NewTicket();
  std::visit(DurationVisitor{&req}, std::move(period));
  std::visit(TimePointVisitor{&req}, std::move(start_time));
  req.set_blink_table(blink_table);
  ExportedTableCreationResponse resp;
  server_->SendRpc([&](grpc::ClientContext *ctx) {
    return server_->TableStub()->TimeTable(ctx, req, &resp);
  });
  return TableHandleImpl::Create(shared_from_this(), std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::InputTable(
    const TableHandleImpl &initial_table, std::vector<std::string> columns) {
  auto *server = server_.get();
  CreateInputTableRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_table_id()->mutable_ticket() = initial_table.Ticket();
  if (columns.empty()) {
    (void)req.mutable_kind()->mutable_in_memory_append_only();
  } else {
    MoveVectorData(std::move(columns),
        req.mutable_kind()->mutable_in_memory_key_backed()->mutable_key_columns());
  }
  ExportedTableCreationResponse resp;
  server_->SendRpc([&](grpc::ClientContext *ctx) {
    return server_->TableStub()->CreateInputTable(ctx, req, &resp);
  });
  return TableHandleImpl::Create(shared_from_this(), std::move(resp));
}

void TableHandleManagerImpl::RunScript(std::string code) {
  if (!consoleId_.has_value()) {
    auto message = DEEPHAVEN_LOCATION_STR("Client was created without specifying a script language");
    throw std::runtime_error(message);
  }
  ExecuteCommandRequest req;
  *req.mutable_console_id() = *consoleId_;
  *req.mutable_code() = std::move(code);
  ExecuteCommandResponse resp;
  server_->SendRpc([&](grpc::ClientContext *ctx) {
    return server_->ConsoleStub()->ExecuteCommand(ctx, req, &resp);
  });
}

std::shared_ptr<TableHandleImpl> TableHandleManagerImpl::MakeTableHandleFromTicket(std::string ticket) {
  Ticket req;
  *req.mutable_ticket() = std::move(ticket);
  ExportedTableCreationResponse resp;
  server_->SendRpc([&](grpc::ClientContext *ctx) {
    return server_->TableStub()->GetExportedTableCreationResponse(ctx, req, &resp);
  });
  return TableHandleImpl::Create(shared_from_this(), std::move(resp));
}

void TableHandleManagerImpl::AddSubscriptionHandle(std::shared_ptr<SubscriptionHandle> handle) {
  std::unique_lock guard(mutex_);
  subscriptions_.insert(std::move(handle));
}

void TableHandleManagerImpl::RemoveSubscriptionHandle(const std::shared_ptr<SubscriptionHandle> &handle) {
  std::unique_lock guard(mutex_);
  subscriptions_.erase(handle);
}
namespace {
Ticket MakeScopeReference(std::string_view table_name) {
  if (table_name.empty()) {
    auto message = DEEPHAVEN_LOCATION_STR("table_name is empty");
    throw std::runtime_error(message);
  }

  Ticket result;
  result.mutable_ticket()->reserve(2 + table_name.size());
  result.mutable_ticket()->append("s/");
  result.mutable_ticket()->append(table_name);
  return result;
}
}  // namespace
}  // namespace deephaven::client::impl
