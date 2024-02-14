/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <optional>
#include "deephaven/client/server/server.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/executor.h"

namespace deephaven::client::impl {
class TableHandleImpl;

class TableHandleManagerImpl final : public std::enable_shared_from_this<TableHandleManagerImpl> {
  struct Private {};
  using ServerType = deephaven::client::server::Server;
  using SubscriptionHandle = deephaven::client::subscription::SubscriptionHandle;
  using ExecutorType = deephaven::client::utility::Executor;
  using AsOfJoinTablesRequest = io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest;
  using ComboAggregateRequest = io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
  using ExportedTableCreationResponse = io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse;
  using SortDescriptor = io::deephaven::proto::backplane::grpc::SortDescriptor;
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;
  using BindTableToVariableResponse = io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse;
  using DurationSpecifier = deephaven::client::utility::DurationSpecifier;
  using TimePointSpecifier = deephaven::client::utility::TimePointSpecifier;

public:
  [[nodiscard]]
  static std::shared_ptr<TableHandleManagerImpl> Create(std::optional<Ticket> console_id,
      std::shared_ptr<ServerType> server, std::shared_ptr<ExecutorType> executor,
      std::shared_ptr<ExecutorType> flight_executor);

  TableHandleManagerImpl(Private, std::optional<Ticket> &&console_id,
      std::shared_ptr<ServerType> &&server, std::shared_ptr<ExecutorType> &&executor,
      std::shared_ptr<ExecutorType> &&flight_executor);
  TableHandleManagerImpl(const TableHandleManagerImpl &other) = delete;
  TableHandleManagerImpl &operator=(const TableHandleManagerImpl &other) = delete;
  ~TableHandleManagerImpl();

  void Shutdown();

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> EmptyTable(int64_t size);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> FetchTable(std::string table_name);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> TimeTable(DurationSpecifier period, TimePointSpecifier start_time,
      bool blink_table);
  void RunScript(std::string code);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> InputTable(const TableHandleImpl &initial_table,
      std::vector<std::string> columns);
  /**
   * See the documentation for Server::NewTicket().
   */
  [[nodiscard]]
  std::string NewTicket() {
    auto ticket = server_->NewTicket();
    // our API only wants the internal string part.
    return std::move(*ticket.mutable_ticket());
  }

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> MakeTableHandleFromTicket(std::string ticket);

  void AddSubscriptionHandle(std::shared_ptr<SubscriptionHandle> handle);
  void RemoveSubscriptionHandle(const std::shared_ptr<SubscriptionHandle> &handle);

  [[nodiscard]]
  const std::optional<Ticket> &ConsoleId() const { return consoleId_; }
  [[nodiscard]]
  const std::shared_ptr<ServerType> &Server() const { return server_; }
  [[nodiscard]]
  const std::shared_ptr<ExecutorType> &Executor() const { return executor_; }
  [[nodiscard]]
  const std::shared_ptr<ExecutorType> &FlightExecutor() const { return flightExecutor_; }

private:
  const std::string me_;  // useful printable object name for logging
  std::optional<Ticket> consoleId_;
  std::shared_ptr<ServerType> server_;
  std::shared_ptr<ExecutorType> executor_;
  std::shared_ptr<ExecutorType> flightExecutor_;
  // Protects the below for concurrent access.
  std::mutex mutex_;
  // The SubscriptionHandles for the tables we have subscribed to. We keep these at the TableHandleManagerImpl level
  // so we can cleanly shut them all down when the TableHandleManagerImpl::shutdown() is called.
  std::set<std::shared_ptr<SubscriptionHandle>> subscriptions_;
};
}  // namespace deephaven::client::impl
