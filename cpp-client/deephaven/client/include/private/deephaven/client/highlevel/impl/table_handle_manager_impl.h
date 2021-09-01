#pragma once

#include <memory>
#include "deephaven/client/lowlevel/server.h"
#include "deephaven/client/utility/executor.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
class TableHandleImpl;

class TableHandleManagerImpl {
  struct Private {};
  typedef deephaven::client::lowlevel::Server Server;
  typedef deephaven::client::utility::Executor Executor;
  typedef io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest AsOfJoinTablesRequest;
  typedef io::deephaven::proto::backplane::grpc::ComboAggregateRequest ComboAggregateRequest;
  typedef io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse ExportedTableCreationResponse;
  typedef io::deephaven::proto::backplane::grpc::SortDescriptor SortDescriptor;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse BindTableToVariableResponse;

  template<typename... Args>
  using Callback = deephaven::client::utility::Callback<Args...>;
  template<typename T>
  using SFCallback = deephaven::client::utility::SFCallback<T>;
  typedef SFCallback<ExportedTableCreationResponse> EtcCallback;

public:
  static std::shared_ptr<TableHandleManagerImpl> create(Ticket consoleId,
      std::shared_ptr<Server> server, std::shared_ptr<Executor> executor,
      std::shared_ptr<Executor> flightExecutor);

  TableHandleManagerImpl(Private, Ticket &&consoleId,
      std::shared_ptr<Server> &&server, std::shared_ptr<Executor> &&executor,
      std::shared_ptr<Executor> &&flightExecutor);
  TableHandleManagerImpl(const TableHandleManagerImpl &other) = delete;
  TableHandleManagerImpl &operator=(const TableHandleManagerImpl &other) = delete;
  ~TableHandleManagerImpl();

  std::shared_ptr<TableHandleImpl> emptyTable(int64_t size);
  std::shared_ptr<TableHandleImpl> fetchTable(std::string tableName);
  //  std::shared_ptr<QueryTableImpl> tempTable(const std::vector<ColumnDataHolder> &columnDataHolders);
  std::shared_ptr<TableHandleImpl> timeTable(int64_t startTimeNanos, int64_t periodNanos);

  const Ticket &consoleId() const { return consoleId_; }
  const std::shared_ptr<Server> &server() const { return server_; }
  const std::shared_ptr<Executor> &executor() const { return executor_; }
  const std::shared_ptr<Executor> &flightExecutor() const { return flightExecutor_; }

private:
  Ticket consoleId_;
  std::shared_ptr<Server> server_;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<Executor> flightExecutor_;
  std::weak_ptr<TableHandleManagerImpl> self_;
};
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
