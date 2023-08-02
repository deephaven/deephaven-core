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
class GetColumnDefsCallback;

class LazyStateInfo final {
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;

public:
  LazyStateInfo(int64_t numRows, bool isStatic);
  LazyStateInfo(const LazyStateInfo &other);
  LazyStateInfo &operator=(const LazyStateInfo &other);
  LazyStateInfo(LazyStateInfo &&other) noexcept;
  LazyStateInfo &operator=(LazyStateInfo &&other) noexcept;
  ~LazyStateInfo();

  int64_t numRows() const { return numRows_; }
  bool isStatic() const { return isStatic_; }

private:
  int64_t numRows_ = 0;
  bool isStatic_ = false;
};

class ExportedTableCreationCallback final
    : public deephaven::dhcore::utility::SFCallback<io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse> {
  typedef io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse ExportedTableCreationResponse;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef deephaven::client::server::Server Server;
  typedef deephaven::client::utility::Executor Executor;

  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;
  template<typename T>
  using CBPromise = deephaven::dhcore::utility::CBPromise<T>;
  template<typename T>
  using CBFuture = deephaven::dhcore::utility::CBFuture<T>;

public:
  ExportedTableCreationCallback(std::shared_ptr<TableHandleImpl> dependency, Ticket expectedTicket,
      CBPromise<LazyStateInfo> infoPromise);
  ~ExportedTableCreationCallback() final;

  void onSuccess(ExportedTableCreationResponse item) final;
  void onFailure(std::exception_ptr ep) final;

private:
  // Hold a dependency on the parent until this callback is done.
  std::shared_ptr<TableHandleImpl> dependency_;
  Ticket expectedTicket_;
  CBPromise<LazyStateInfo> infoPromise_;
};

class LazyState final {
  typedef deephaven::dhcore::clienttable::Schema Schema;
  typedef io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse ExportedTableCreationResponse;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef deephaven::client::server::Server Server;
  typedef deephaven::client::utility::Executor Executor;

  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;
  template<typename T>
  using CBPromise = deephaven::dhcore::utility::CBPromise<T>;
  template<typename T>
  using CBFuture = deephaven::dhcore::utility::CBFuture<T>;

public:
  LazyState(std::shared_ptr<Server> server, std::shared_ptr<Executor> flightExecutor,
      CBFuture<LazyStateInfo> infoFuture, Ticket ticket);
  ~LazyState();

  std::shared_ptr<Schema> getSchema();
  void getSchemaAsync(std::shared_ptr<SFCallback<std::shared_ptr<Schema>>> cb);

  void releaseAsync();

  /**
   * Used in tests.
   */
  void waitUntilReady();

  const LazyStateInfo &info() const;

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
  typedef deephaven::client::SortPair SortPair;
  typedef deephaven::client::impl::ColumnImpl ColumnImpl;
  typedef deephaven::client::impl::DateTimeColImpl DateTimeColImpl;
  typedef deephaven::client::impl::NumColImpl NumColImpl;
  typedef deephaven::client::impl::StrColImpl StrColImpl;
  typedef deephaven::client::impl::BooleanExpressionImpl BooleanExpressionImpl;
  typedef deephaven::client::subscription::SubscriptionHandle SubscriptionHandle;
  typedef deephaven::client::utility::Executor Executor;
  typedef deephaven::dhcore::clienttable::Schema Schema;
  typedef deephaven::dhcore::ticking::TickingCallback TickingCallback;
  typedef deephaven::dhcore::ElementTypeId ElementTypeId;
  typedef io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest AsOfJoinTablesRequest;
  typedef io::deephaven::proto::backplane::grpc::ComboAggregateRequest ComboAggregateRequest;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;

  template<typename ...Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;
public:
  static std::pair<std::shared_ptr<internal::ExportedTableCreationCallback>, std::shared_ptr<internal::LazyState>>
  createEtcCallback(std::shared_ptr<TableHandleImpl> dependency, const TableHandleManagerImpl *thm, Ticket resultTicket);

  static std::shared_ptr<TableHandleImpl> create(std::shared_ptr<TableHandleManagerImpl> thm, Ticket ticket,
      std::shared_ptr<internal::LazyState> lazyState);
  TableHandleImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&thm, Ticket &&ticket,
      std::shared_ptr<internal::LazyState> &&lazyState);
  ~TableHandleImpl();

  std::shared_ptr<TableHandleImpl> select(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> update(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> view(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> dropColumns(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> updateView(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> where(std::string condition);
  std::shared_ptr<TableHandleImpl> sort(std::vector<SortPair> sortPairs);
  std::shared_ptr<TableHandleImpl> preemptive(int32_t sampleIntervalMs);

  std::shared_ptr<TableHandleImpl> by(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> by(std::vector<ComboAggregateRequest::Aggregate> descriptors,
      std::vector<std::string> groupByColumns);
  std::shared_ptr<TableHandleImpl> minBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> maxBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> sumBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> absSumBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> varBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> stdBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> avgBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> lastBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> firstBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> medianBy(std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> percentileBy(double percentile, bool avgMedian,
      std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl>
  percentileBy(double percentile, std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl>
  countBy(std::string countByColumn, std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl>
  wAvgBy(std::string weightColumn, std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> tailBy(int64_t n, std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> headBy(int64_t n, std::vector<std::string> columnSpecs);

  std::shared_ptr<TableHandleImpl> tail(int64_t n);
  std::shared_ptr<TableHandleImpl> head(int64_t n);
  std::shared_ptr<TableHandleImpl> ungroup(bool nullFill, std::vector<std::string> groupByColumns);
  std::shared_ptr<TableHandleImpl> merge(std::string keyColumn, std::vector<Ticket> sourceTickets);

  std::shared_ptr<TableHandleImpl> crossJoin(const TableHandleImpl &rightSide,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd);

  std::shared_ptr<TableHandleImpl> naturalJoin(const TableHandleImpl &rightSide,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd);

  std::shared_ptr<TableHandleImpl> exactJoin(const TableHandleImpl &rightSide,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd);

  std::shared_ptr<TableHandleImpl> asOfJoin(AsOfJoinTablesRequest::MatchRule matchRule,
      const TableHandleImpl &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd);

  std::shared_ptr<TableHandleImpl> updateBy(std::vector<std::shared_ptr<UpdateByOperationImpl>> ops,
      std::vector<std::string> by);

  std::vector<std::shared_ptr<ColumnImpl>> getColumnImpls();
  std::shared_ptr<StrColImpl> getStrColImpl(std::string columnName);
  std::shared_ptr<NumColImpl> getNumColImpl(std::string columnName);
  std::shared_ptr<DateTimeColImpl> getDateTimeColImpl(std::string columnName);

  void bindToVariableAsync(std::string variable, std::shared_ptr<SFCallback<>> callback);

  std::shared_ptr<SubscriptionHandle> subscribe(std::shared_ptr<TickingCallback> callback);
  std::shared_ptr<SubscriptionHandle> subscribe(TableHandle::onTickCallback_t onTick,
      void *onTickUserData, TableHandle::onErrorCallback_t onError, void *onErrorUserData);
  void unsubscribe(const std::shared_ptr<SubscriptionHandle> &handle);

  /**
   * Used in tests.
   */
  void observe();

  int64_t numRows() const;
  bool isStatic() const;
  std::shared_ptr<Schema> schema() const;

  const std::shared_ptr<TableHandleManagerImpl> &managerImpl() const { return managerImpl_; }

  const Ticket &ticket() const { return ticket_; }

private:
  void lookupHelper(const std::string &columnName, std::initializer_list<ElementTypeId::Enum> validTypes);

  std::shared_ptr<TableHandleImpl> defaultAggregateByDescriptor(
      ComboAggregateRequest::Aggregate descriptor, std::vector<std::string> groupByColumns);
  std::shared_ptr<TableHandleImpl>
  defaultAggregateByType(ComboAggregateRequest::AggType aggregateType,
      std::vector<std::string> groupByColumns);

  std::shared_ptr<TableHandleImpl> headOrTailHelper(bool head, int64_t n);
  std::shared_ptr<TableHandleImpl> headOrTailByHelper(int64_t n, bool head,
      std::vector<std::string> columnSpecs);

  std::shared_ptr<TableHandleManagerImpl> managerImpl_;
  Ticket ticket_;
  std::shared_ptr<internal::LazyState> lazyState_;
};
}  // namespace impl
}  // namespace deephaven::client
