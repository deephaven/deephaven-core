/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <set>
#include <string>
#include "deephaven/client/client.h"
#include "deephaven/client/server/server.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/update_by.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/types.h"
#include "deephaven_core/proto/session.pb.h"
#include "deephaven_core/proto/session.grpc.pb.h"
#include "deephaven_core/proto/table.pb.h"
#include "deephaven_core/proto/table.grpc.pb.h"

namespace deephaven::client {
class SortPair;
namespace impl {
class TableHandleManagerImpl;

class TableHandleImpl : public std::enable_shared_from_this<TableHandleImpl> {
  struct Private {
  };
  using SortPair = deephaven::client::SortPair;
  using SubscriptionHandle = deephaven::client::subscription::SubscriptionHandle;
  using Executor = deephaven::client::utility::Executor;
  using SchemaType = deephaven::dhcore::clienttable::Schema;
  using TickingCallback = deephaven::dhcore::ticking::TickingCallback;
  using ElementTypeId = deephaven::dhcore::ElementTypeId;
  using AsOfJoinTablesRequest = io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest;
  using ComboAggregateRequest = io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
  using TicketType = io::deephaven::proto::backplane::grpc::Ticket;
  using ExportedTableCreationResponse = io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse;
  using SelectOrUpdateRequest = io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest;
  using TableService = io::deephaven::proto::backplane::grpc::TableService;

public:
  static std::shared_ptr<TableHandleImpl> Create(std::shared_ptr<TableHandleManagerImpl> thm,
      ExportedTableCreationResponse response);
  TableHandleImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&thm, TicketType &&ticket,
      int64_t num_rows, bool is_static);
  ~TableHandleImpl();

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Select(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Update(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> LazyUpdate(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> View(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> DropColumns(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> UpdateView(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Where(std::string condition);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Sort(std::vector<SortPair> sort_pairs);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> By(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> By(std::vector<ComboAggregateRequest::Aggregate> descriptors,
      std::vector<std::string> group_by_columns);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> MinBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> MaxBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> SumBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> AbsSumBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> VarBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> StdBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> AvgBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> LastBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> FirstBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> MedianBy(std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> PercentileBy(double percentile, bool avg_median,
      std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl>
  CountBy(std::string count_by_column, std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl>
  WavgBy(std::string weight_column, std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> TailBy(int64_t n, std::vector<std::string> column_specs);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> HeadBy(int64_t n, std::vector<std::string> column_specs);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Tail(int64_t n);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Head(int64_t n);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Ungroup(bool null_fill, std::vector<std::string> group_by_columns);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Merge(std::string key_column, std::vector<TicketType> source_tickets);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> CrossJoin(const TableHandleImpl &right_side,
      std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> NaturalJoin(const TableHandleImpl &right_side,
      std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> ExactJoin(const TableHandleImpl &right_side,
      std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Aj(const TableHandleImpl &right_side,
      std::vector<std::string> on, std::vector<std::string> joins);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> Raj(const TableHandleImpl &right_side,
      std::vector<std::string> on, std::vector<std::string> joins);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> LeftOuterJoin(const TableHandleImpl &right_side,
      std::vector<std::string> on, std::vector<std::string> joins);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> UpdateBy(std::vector<std::shared_ptr<UpdateByOperationImpl>> ops,
      std::vector<std::string> by);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> SelectDistinct(std::vector<std::string> columns);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> WhereIn(const TableHandleImpl &filter_table,
      std::vector<std::string> columns);

  void AddTable(const TableHandleImpl &table_to_add);
  void RemoveTable(const TableHandleImpl &table_to_remove);

  void BindToVariable(std::string variable);

  [[nodiscard]]
  std::shared_ptr<SubscriptionHandle> Subscribe(std::shared_ptr<TickingCallback> callback);
  [[nodiscard]]
  std::shared_ptr<SubscriptionHandle> Subscribe(TableHandle::onTickCallback_t on_tick,
      void *on_tick_user_data, TableHandle::onErrorCallback_t on_error, void *on_error_user_data);
  void Unsubscribe(const std::shared_ptr<SubscriptionHandle> &handle);

  [[nodiscard]]
  int64_t NumRows() const { return num_rows_; }
  [[nodiscard]]
  bool IsStatic() const { return is_static_; }
  [[nodiscard]]
  std::shared_ptr<SchemaType> Schema();

  const std::shared_ptr<TableHandleManagerImpl> &ManagerImpl() const { return managerImpl_; }

  const TicketType &Ticket() const { return ticket_; }

private:
  // A pointer to member of TableService::Stub, taking (context, request, resp) and returning a
  // ::grpc::Status
  using selectOrUpdateMethod_t = grpc::Status(TableService::Stub::*)(grpc::ClientContext *context,
          const SelectOrUpdateRequest &request, ExportedTableCreationResponse *resp);

  std::shared_ptr<TableHandleImpl>
  SelectOrUpdateHelper(std::vector<std::string> column_specs, selectOrUpdateMethod_t which_method);

  void LookupHelper(const std::string &column_name, std::initializer_list<ElementTypeId::Enum> valid_types);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> DefaultAggregateByDescriptor(
      ComboAggregateRequest::Aggregate descriptor, std::vector<std::string> group_by_columns);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl>
  DefaultAggregateByType(ComboAggregateRequest::AggType aggregate_type,
      std::vector<std::string> group_by_columns);

  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> HeadOrTailHelper(bool head, int64_t n);
  [[nodiscard]]
  std::shared_ptr<TableHandleImpl> HeadOrTailByHelper(int64_t n, bool head,
      std::vector<std::string> column_specs);

  std::shared_ptr<TableHandleManagerImpl> managerImpl_;
  TicketType ticket_;
  int64_t num_rows_ = 0;
  bool is_static_ = false;
  std::mutex mutex_;
  bool schema_request_sent_ = false;
  std::shared_future<std::shared_ptr<SchemaType>> schema_future_;
};
}  // namespace impl
}  // namespace deephaven::client
