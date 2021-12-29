#pragma once

#include <memory>
#include <map>
#include <string>
#include "deephaven/client/lowlevel/server.h"
#include "deephaven/client/highlevel/client.h"
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/utility/cbfuture.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/proto/session.pb.h"
#include "deephaven/proto/session.grpc.pb.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

namespace deephaven {
namespace client {
namespace highlevel {
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

class ColumnDefinitions {
public:
  typedef std::map<std::string, std::shared_ptr<arrow::DataType>> map_t;
  typedef std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> vec_t;

  ColumnDefinitions(vec_t vec, map_t map);
  ~ColumnDefinitions();

  const vec_t &vec() const { return vec_; }
  const map_t &map() const { return map_; }

private:
  vec_t vec_;
  map_t map_;
};

class LazyState final : public deephaven::client::utility::SFCallback<io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse> {
  struct Private {};

  typedef io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse ExportedTableCreationResponse;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef deephaven::client::lowlevel::Server Server;
  typedef deephaven::client::utility::Executor Executor;

  template<typename T>
  using SFCallback = deephaven::client::utility::SFCallback<T>;
  template<typename T>
  using CBPromise = deephaven::client::utility::CBPromise<T>;
  template<typename T>
  using CBFuture = deephaven::client::utility::CBFuture<T>;

public:
  static std::shared_ptr<LazyState> create(std::shared_ptr<Server> server,
      std::shared_ptr<Executor> flightExecutor);
  static std::shared_ptr<LazyState> createSatisfied(std::shared_ptr<Server> server,
      std::shared_ptr<Executor> flightExecutor, Ticket ticket);

  LazyState(Private, std::shared_ptr<Server> &&server, std::shared_ptr<Executor> &&flightExecutor);
  ~LazyState() final;

  void waitUntilReady();

  void onSuccess(ExportedTableCreationResponse item) final;
  void onFailure(std::exception_ptr ep) final;

  std::shared_ptr<ColumnDefinitions> getColumnDefinitions();
  void getColumnDefinitionsAsync(std::shared_ptr<SFCallback<std::shared_ptr<ColumnDefinitions>>> cb);

private:
  std::shared_ptr<Server> server_;
  std::shared_ptr<Executor> flightExecutor_;

  CBPromise<Ticket> ticketPromise_;
  CBFuture<Ticket> ticketFuture_;
  CBPromise<std::shared_ptr<ColumnDefinitions>> colDefsPromise_;
  CBFuture<std::shared_ptr<ColumnDefinitions>> colDefsFuture_;

  std::weak_ptr<LazyState> weakSelf_;

  friend class GetColumnDefsCallback;
};
}  // namespace internal

class TableHandleImpl {
  struct Private {};
  typedef deephaven::client::highlevel::SortPair SortPair;
  typedef deephaven::client::highlevel::impl::ColumnImpl ColumnImpl;
  typedef deephaven::client::highlevel::impl::DateTimeColImpl DateTimeColImpl;
  typedef deephaven::client::highlevel::impl::NumColImpl NumColImpl;
  typedef deephaven::client::highlevel::impl::StrColImpl StrColImpl;
  typedef deephaven::client::highlevel::impl::BooleanExpressionImpl BooleanExpressionImpl;
  typedef deephaven::client::utility::Executor Executor;
  typedef io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest AsOfJoinTablesRequest;
  typedef io::deephaven::proto::backplane::grpc::ComboAggregateRequest ComboAggregateRequest;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;

  template<typename ...Args>
  using SFCallback = deephaven::client::utility::SFCallback<Args...>;
public:
  static std::shared_ptr<internal::LazyState> createEtcCallback(const TableHandleManagerImpl *thm);
  // Create a callback that is already satisfied by "ticket".
  static std::shared_ptr<internal::LazyState> createSatisfiedCallback(const TableHandleManagerImpl *thm,
      Ticket ticket);

  static std::shared_ptr<TableHandleImpl> create(std::shared_ptr<TableHandleManagerImpl> thm,
      Ticket ticket, std::shared_ptr<internal::LazyState> etcCallback);
  TableHandleImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&thm,
      Ticket &&ticket, std::shared_ptr<internal::LazyState> &&lazyState);
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
  std::shared_ptr<TableHandleImpl> percentileBy(double percentile, std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> countBy(std::string countByColumn, std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> wAvgBy(std::string weightColumn, std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> tailBy(int64_t n, std::vector<std::string> columnSpecs);
  std::shared_ptr<TableHandleImpl> headBy(int64_t n, std::vector<std::string> columnSpecs);

  std::shared_ptr<TableHandleImpl> tail(int64_t n);
  std::shared_ptr<TableHandleImpl> head(int64_t n);
  std::shared_ptr<TableHandleImpl> ungroup(bool nullFill, std::vector<std::string> groupByColumns);
  std::shared_ptr<TableHandleImpl> merge(std::string keyColumn, std::vector<Ticket> sourceTickets);

  std::shared_ptr<TableHandleImpl> crossJoin(const TableHandleImpl &rightSide,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const;

  std::shared_ptr<TableHandleImpl> naturalJoin(const TableHandleImpl &rightSide,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const;

  std::shared_ptr<TableHandleImpl> exactJoin(const TableHandleImpl &rightSide,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const;

  std::shared_ptr<TableHandleImpl> asOfJoin(AsOfJoinTablesRequest::MatchRule matchRule,
      const TableHandleImpl &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd);

  std::vector<std::shared_ptr<ColumnImpl>> getColumnImpls();
  std::shared_ptr<StrColImpl> getStrColImpl(std::string columnName);
  std::shared_ptr<NumColImpl> getNumColImpl(std::string columnName);
  std::shared_ptr<DateTimeColImpl> getDateTimeColImpl(std::string columnName);

  void bindToVariableAsync(std::string variable, std::shared_ptr<SFCallback<>> callback);

  // For debugging
  void observe();

  const std::shared_ptr<TableHandleManagerImpl> &managerImpl() const { return managerImpl_; }
  const Ticket &ticket() const { return ticket_; }

private:
  void lookupHelper(const std::string &columnName,
      std::initializer_list<arrow::Type::type> validTypes);

  std::shared_ptr<TableHandleImpl> defaultAggregateByDescriptor(
      ComboAggregateRequest::Aggregate descriptor, std::vector<std::string> groupByColumns);
  std::shared_ptr<TableHandleImpl> defaultAggregateByType(ComboAggregateRequest::AggType aggregateType,
      std::vector<std::string> groupByColumns);

  std::shared_ptr<TableHandleImpl> headOrTailHelper(bool head, int64_t n);
  std::shared_ptr<TableHandleImpl> headOrTailByHelper(int64_t n, bool head,
      std::vector<std::string> columnSpecs);

  std::shared_ptr<TableHandleManagerImpl> managerImpl_;
  Ticket ticket_;
  std::shared_ptr<internal::LazyState> lazyState_;
  std::weak_ptr<TableHandleImpl> weakSelf_;
};
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
