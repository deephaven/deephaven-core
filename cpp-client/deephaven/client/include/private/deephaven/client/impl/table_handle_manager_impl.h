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
  typedef deephaven::client::server::Server Server;
  typedef deephaven::client::subscription::SubscriptionHandle SubscriptionHandle;
  typedef deephaven::client::utility::Executor Executor;
  typedef io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest AsOfJoinTablesRequest;
  typedef io::deephaven::proto::backplane::grpc::ComboAggregateRequest ComboAggregateRequest;
  typedef io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse ExportedTableCreationResponse;
  typedef io::deephaven::proto::backplane::grpc::SortDescriptor SortDescriptor;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse BindTableToVariableResponse;

  template<typename ...Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;

public:
  static std::shared_ptr<TableHandleManagerImpl> create(std::optional<Ticket> consoleId,
      std::shared_ptr<Server> server, std::shared_ptr<Executor> executor,
      std::shared_ptr<Executor> flightExecutor);

  TableHandleManagerImpl(Private, std::optional<Ticket> &&consoleId,
      std::shared_ptr<Server> &&server, std::shared_ptr<Executor> &&executor,
      std::shared_ptr<Executor> &&flightExecutor);
  TableHandleManagerImpl(const TableHandleManagerImpl &other) = delete;
  TableHandleManagerImpl &operator=(const TableHandleManagerImpl &other) = delete;
  ~TableHandleManagerImpl();

  void shutdown();

  std::shared_ptr<TableHandleImpl> emptyTable(int64_t size);
  std::shared_ptr<TableHandleImpl> fetchTable(std::string tableName);
  std::shared_ptr<TableHandleImpl> timeTable(int64_t startTimeNanos, int64_t periodNanos);
  void runScriptAsync(std::string code, std::shared_ptr<SFCallback<>> callback);

  /**
   * See the documentation for Server::newTicket().
   */
  std::string newTicket() {
    auto ticket = server_->newTicket();
    // our API only wants the internal string part.
    return std::move(*ticket.mutable_ticket());
  }

  std::shared_ptr<TableHandleImpl> makeTableHandleFromTicket(std::string ticket);

  void addSubscriptionHandle(std::shared_ptr<SubscriptionHandle> handle);
  void removeSubscriptionHandle(const std::shared_ptr<SubscriptionHandle> &handle);

  const std::optional<Ticket> &consoleId() const { return consoleId_; }
  const std::shared_ptr<Server> &server() const { return server_; }
  const std::shared_ptr<Executor> &executor() const { return executor_; }
  const std::shared_ptr<Executor> &flightExecutor() const { return flightExecutor_; }

private:
  const std::string me_;  // useful printable object name for logging
  std::optional<Ticket> consoleId_;
  std::shared_ptr<Server> server_;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<Executor> flightExecutor_;
  // Protects the below for concurrent access.
  std::mutex mutex_;
  // The SubscriptionHandles for the tables we have subscribed to. We keep these at the TableHandleManagerImpl level
  // so we can cleanly shut them all down when the TableHandleManagerImpl::shutdown() is called.
  std::set<std::shared_ptr<SubscriptionHandle>> subscriptions_;
};
}  // namespace deephaven::client::impl
