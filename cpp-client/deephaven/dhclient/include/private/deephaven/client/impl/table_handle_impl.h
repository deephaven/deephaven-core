/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
#include "deephaven/dhcore/utility/callbacks.h"
#include "deephaven/dhcore/utility/cbfuture.h"
#include "deephaven/proto/session.pb.h"
#include "deephaven/proto/session.grpc.pb.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

namespace deephaven::client {
class SortPair;
namespace impl {
class BooleanExpressionImpl;

class ColumnImpl;

class DateTimeColImpl;

class NumColImpl;

class StrColImpl;

class TableHandleManagerImpl;

namespace internal {
class LazyStateInfo final {
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;

public:
  LazyStateInfo(int64_t num_rows, bool is_static);
  LazyStateInfo(const LazyStateInfo &other);
  LazyStateInfo &operator=(const LazyStateInfo &other);
  LazyStateInfo(LazyStateInfo &&other) noexcept;
  LazyStateInfo &operator=(LazyStateInfo &&other) noexcept;
  ~LazyStateInfo();

  [[nodiscard]]
  int64_t NumRows() const { return numRows_; }
  [[nodiscard]]
  bool IsStatic() const { return isStatic_; }

private:
  int64_t numRows_ = 0;
  bool isStatic_ = false;
};

class ExportedTableCreationCallback final
    : public deephaven::dhcore::utility::SFCallback<io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse> {
  using ExportedTableCreationResponse = io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse;
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;
  using Server = deephaven::client::server::Server;
  using Executor = deephaven::client::utility::Executor;

  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;
  template<typename T>
  using CBPromise = deephaven::dhcore::utility::CBPromise<T>;
  template<typename T>
  using CBFuture = deephaven::dhcore::utility::CBFuture<T>;

public:
  ExportedTableCreationCallback(std::shared_ptr<TableHandleImpl> dependency, Ticket expected_ticket,
      CBPromise<LazyStateInfo> info_promise);
  ~ExportedTableCreationCallback() final;

  void OnSuccess(ExportedTableCreationResponse item) final;
  void OnFailure(std::exception_ptr ep) final;

private:
  // Hold a dependency on the parent until this callback is done.
  std::shared_ptr<TableHandleImpl> dependency_;
  Ticket expectedTicket_;
  CBPromise<LazyStateInfo> infoPromise_;
};

class LazyState final {
  using Schema = deephaven::dhcore::clienttable::Schema;
  using ExportedTableCreationResponse = io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse;
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;
  using Server = deephaven::client::server::Server;
  using Executor = deephaven::client::utility::Executor;

  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;
  template<typename T>
  using CBPromise = deephaven::dhcore::utility::CBPromise<T>;
  template<typename T>
  using CBFuture = deephaven::dhcore::utility::CBFuture<T>;

public:
  LazyState(std::shared_ptr<Server> server, std::shared_ptr<Executor> flight_executor,
      CBFuture<LazyStateInfo> info_future, Ticket ticket);
  ~LazyState();

  [[nodiscard]]
  std::shared_ptr<Schema> GetSchema();
  void GetSchemaAsync(std::shared_ptr<SFCallback<std::shared_ptr<Schema>>> cb);

  void ReleaseAsync();

  /**
   * Used in tests.
   */
  void WaitUntilReady();

  [[nodiscard]]
  const LazyStateInfo &Info() const;

private:
  std::shared_ptr<Server> server_;
  std::shared_ptr<Executor> flightExecutor_;
  CBFuture<LazyStateInfo> infoFuture_;
  Ticket ticket_;

  std::atomic_flag schemaRequestSent_ = {};
  CBPromise<std::shared_ptr<Schema>> schemaPromise_;
  CBFuture<std::shared_ptr<Schema>> schemaFuture_;
};
}  // namespace internal

class TableHandleImpl : public std::enable_shared_from_this<TableHandleImpl> {
  struct Private {
  };
  using SortPair = deephaven::client::SortPair;
  using ColumnImpl = deephaven::client::impl::ColumnImpl;
  using DateTimeColImpl = deephaven::client::impl::DateTimeColImpl;
  using NumColImpl = deephaven::client::impl::NumColImpl;
  using StrColImpl = deephaven::client::impl::StrColImpl;
  using BooleanExpressionImpl = deephaven::client::impl::BooleanExpressionImpl;
  using SubscriptionHandle = deephaven::client::subscription::SubscriptionHandle;
  using Executor = deephaven::client::utility::Executor;
  using SchemaType = deephaven::dhcore::clienttable::Schema;
  using TickingCallback = deephaven::dhcore::ticking::TickingCallback;
  using ElementTypeId = deephaven::dhcore::ElementTypeId;
  using AsOfJoinTablesRequest = io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest;
  using ComboAggregateRequest = io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
  using TicketType = io::deephaven::proto::backplane::grpc::Ticket;

  template<typename ...Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;
public:
  [[nodiscard]]
  static std::pair<std::shared_ptr<internal::ExportedTableCreationCallback>, std::shared_ptr<internal::LazyState>>
  CreateEtcCallback(std::shared_ptr<TableHandleImpl> dependency, const TableHandleManagerImpl *thm, TicketType result_ticket);

  [[nodiscard]]
  static std::shared_ptr<TableHandleImpl> Create(std::shared_ptr<TableHandleManagerImpl> thm, TicketType ticket,
      std::shared_ptr<internal::LazyState> lazy_state);
  TableHandleImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&thm, TicketType &&ticket,
      std::shared_ptr<internal::LazyState> &&lazy_state);
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
  std::shared_ptr<TableHandleImpl> Preemptive(int32_t sample_interval_ms);

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
  PercentileBy(double percentile, std::vector<std::string> column_specs);
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

  [[nodiscard]]
  std::vector<std::shared_ptr<ColumnImpl>> GetColumnImpls();
  [[nodiscard]]
  std::shared_ptr<StrColImpl> GetStrColImpl(std::string column_name);
  [[nodiscard]]
  std::shared_ptr<NumColImpl> GetNumColImpl(std::string column_name);
  [[nodiscard]]
  std::shared_ptr<DateTimeColImpl> GetDateTimeColImpl(std::string column_name);

  void BindToVariableAsync(std::string variable, std::shared_ptr<SFCallback<>> callback);

  [[nodiscard]]
  std::shared_ptr<SubscriptionHandle> Subscribe(std::shared_ptr<TickingCallback> callback);
  [[nodiscard]]
  std::shared_ptr<SubscriptionHandle> Subscribe(TableHandle::onTickCallback_t on_tick,
      void *on_tick_user_data, TableHandle::onErrorCallback_t on_error, void *on_error_user_data);
  void Unsubscribe(const std::shared_ptr<SubscriptionHandle> &handle);

  /**
   * Used in tests.
   */
  void Observe();

  [[nodiscard]]
  int64_t NumRows() const;
  [[nodiscard]]
  bool IsStatic() const;
  [[nodiscard]]
  std::shared_ptr<SchemaType> Schema() const;

  const std::shared_ptr<TableHandleManagerImpl> &ManagerImpl() const { return managerImpl_; }

  const TicketType &Ticket() const { return ticket_; }

private:
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
  std::shared_ptr<internal::LazyState> lazyState_;
};
}  // namespace impl
}  // namespace deephaven::client
