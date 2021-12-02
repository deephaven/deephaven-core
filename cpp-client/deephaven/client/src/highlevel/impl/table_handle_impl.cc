/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/highlevel/impl/table_handle_impl.h"

#include <map>
#include <set>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include "deephaven/client/highlevel/impl/boolean_expression_impl.h"
#include "deephaven/client/highlevel/impl/columns_impl.h"
#include "deephaven/client/highlevel/impl/table_handle_manager_impl.h"
#include "deephaven/client/highlevel/client.h"
#include "deephaven/client/highlevel/columns.h"
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/utility/utility.h"

using io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
using io::deephaven::proto::backplane::grpc::SortDescriptor;
using io::deephaven::proto::backplane::grpc::TableReference;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse;
using deephaven::client::highlevel::SortDirection;
using deephaven::client::highlevel::impl::ColumnImpl;
using deephaven::client::highlevel::impl::DateTimeColImpl;
using deephaven::client::highlevel::impl::NumColImpl;
using deephaven::client::highlevel::impl::StrColImpl;
using deephaven::client::utility::Callback;
using deephaven::client::utility::separatedList;
using deephaven::client::utility::SFCallback;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {

std::shared_ptr<internal::LazyState> TableHandleImpl::createEtcCallback(const TableHandleManagerImpl *thm) {
  return internal::LazyState::create(thm->server(), thm->flightExecutor());
}

std::shared_ptr<internal::LazyState> TableHandleImpl::createSatisfiedCallback(
    const TableHandleManagerImpl *thm, Ticket ticket) {
  return internal::LazyState::createSatisfied(thm->server(), thm->flightExecutor(),
      std::move(ticket));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::create(std::shared_ptr<TableHandleManagerImpl> thm,
    Ticket ticket, std::shared_ptr<internal::LazyState> etcCallback) {
  auto result = std::make_shared<TableHandleImpl>(Private(), std::move(thm), std::move(ticket),
      std::move(etcCallback));
  result->weakSelf_ = result;
  return result;
}

TableHandleImpl::TableHandleImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&thm,
    Ticket &&ticket, std::shared_ptr<internal::LazyState> &&lazyState) :
    managerImpl_(std::move(thm)), ticket_(std::move(ticket)), lazyState_(std::move(lazyState)) {
}

// TODO(kosak): Need to actually send Release to the server
TableHandleImpl::~TableHandleImpl() = default;

std::shared_ptr<TableHandleImpl> TableHandleImpl::select(std::vector<std::string> columnSpecs) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->selectAsync(ticket_, std::move(columnSpecs), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::update(std::vector<std::string> columnSpecs) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->updateAsync(ticket_, std::move(columnSpecs), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::view(std::vector<std::string> columnSpecs) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->viewAsync(ticket_, std::move(columnSpecs), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::dropColumns(std::vector<std::string> columnSpecs) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->dropColumnsAsync(ticket_, std::move(columnSpecs), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::updateView(std::vector<std::string> columnSpecs) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->updateViewAsync(ticket_, std::move(columnSpecs), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::where(std::string condition) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->whereAsync(ticket_, std::move(condition), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::sort(std::vector<SortPair> sortPairs) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  std::vector<SortDescriptor> sortDescriptors;
  sortDescriptors.reserve(sortPairs.size());

  for (auto &sp : sortPairs) {
    auto which = sp.direction() == SortDirection::Ascending ?
        SortDescriptor::ASCENDING : SortDescriptor::DESCENDING;
    SortDescriptor sd;
    sd.set_column_name(std::move(sp.column()));
    sd.set_is_absolute(sp.abs());
    sd.set_direction(which);
    sortDescriptors.push_back(std::move(sd));
  }
  auto resultTicket = managerImpl_->server()->sortAsync(ticket_, std::move(sortDescriptors), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::preemptive(int32_t sampleIntervalMs) {
  throw std::runtime_error("SAD203");
//  auto itdCallback = TableHandleImpl::createItdCallback(scope_->lowLevelSession()->executor());
//  auto resultHandle = scope_->lowLevelSession()->preemptiveAsync(tableHandle_, sampleIntervalMs,
//      itdCallback);
//  return TableHandleImpl::create(scope_, std::move(resultHandle), std::move(itdCallback));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::defaultAggregateByDescriptor(
    ComboAggregateRequest::Aggregate descriptor, std::vector<std::string> columnSpecs) {
  std::vector<ComboAggregateRequest::Aggregate> descriptors;
  descriptors.reserve(1);
  descriptors.push_back(std::move(descriptor));

  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->comboAggregateDescriptorAsync(ticket_,
      std::move(descriptors), std::move(columnSpecs), false, cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::defaultAggregateByType(
    ComboAggregateRequest::AggType aggregateType, std::vector<std::string> columnSpecs) {
  ComboAggregateRequest::Aggregate descriptor;
  descriptor.set_type(aggregateType);
  return defaultAggregateByDescriptor(std::move(descriptor), std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::by(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::GROUP, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::by(
    std::vector<ComboAggregateRequest::Aggregate> descriptors,
    std::vector<std::string> groupByColumns) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->comboAggregateDescriptorAsync(ticket_,
      std::move(descriptors), std::move(groupByColumns), false, cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::minBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::MIN, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::maxBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::MAX, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::sumBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::SUM, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::absSumBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::ABS_SUM, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::varBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::VAR, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::stdBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::STD, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::avgBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::AVG, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::lastBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::LAST, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::firstBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::FIRST, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::medianBy(std::vector<std::string> columnSpecs) {
  return defaultAggregateByType(ComboAggregateRequest::MEDIAN, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::percentileBy(double percentile, bool avgMedian,
    std::vector<std::string> columnSpecs) {
  ComboAggregateRequest::Aggregate descriptor;
  descriptor.set_type(ComboAggregateRequest::PERCENTILE);
  descriptor.set_percentile(percentile);
  descriptor.set_avg_median(avgMedian);
  return defaultAggregateByDescriptor(std::move(descriptor), std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::percentileBy(double percentile,
    std::vector<std::string> columnSpecs) {
  return percentileBy(percentile, false, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::countBy(std::string countByColumn,
    std::vector<std::string> columnSpecs) {
  ComboAggregateRequest::Aggregate descriptor;
  descriptor.set_type(ComboAggregateRequest::COUNT);
  descriptor.set_column_name(std::move(countByColumn));
  return defaultAggregateByDescriptor(std::move(descriptor), std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::wAvgBy(std::string weightColumn,
    std::vector<std::string> columnSpecs) {
  ComboAggregateRequest::Aggregate descriptor;
  descriptor.set_type(ComboAggregateRequest::WEIGHTED_AVG);
  descriptor.set_column_name(std::move(weightColumn));
  return defaultAggregateByDescriptor(std::move(descriptor), std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::tailBy(int64_t n,
    std::vector<std::string> columnSpecs) {
  return headOrTailByHelper(n, false, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::headBy(int64_t n,
    std::vector<std::string> columnSpecs) {
  return headOrTailByHelper(n, true, std::move(columnSpecs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::headOrTailByHelper(int64_t n, bool head,
    std::vector<std::string> columnSpecs) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->headOrTailByAsync(ticket_, head, n,
      std::move(columnSpecs), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::tail(int64_t n) {
  return headOrTailHelper(false, n);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::head(int64_t n) {
  return headOrTailHelper(true, n);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::headOrTailHelper(bool head, int64_t n) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->headOrTailAsync(ticket_, head, n, cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::ungroup(bool nullFill,
    std::vector<std::string> groupByColumns) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->ungroupAsync(ticket_, nullFill, std::move(groupByColumns),
      cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::merge(std::string keyColumn,
    std::vector<Ticket> sourceTickets) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->mergeAsync(std::move(sourceTickets),
      std::move(keyColumn), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::crossJoin(const TableHandleImpl &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->crossJoinAsync(ticket_, rightSide.ticket_,
      std::move(columnsToMatch), std::move(columnsToAdd), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::naturalJoin(const TableHandleImpl &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->naturalJoinAsync(ticket_, rightSide.ticket_,
      std::move(columnsToMatch), std::move(columnsToAdd), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::exactJoin(const TableHandleImpl &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->exactJoinAsync(ticket_, rightSide.ticket_,
      std::move(columnsToMatch), std::move(columnsToAdd), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::asOfJoin(AsOfJoinTablesRequest::MatchRule matchRule,
    const TableHandleImpl &rightSide, std::vector<std::string> columnsToMatch,
    std::vector<std::string> columnsToAdd) {
  auto cb = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->asOfJoinAsync(matchRule, ticket_,
      rightSide.ticket(), std::move(columnsToMatch), std::move(columnsToAdd), cb);
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(cb));
}

std::vector<std::shared_ptr<ColumnImpl>> TableHandleImpl::getColumnImpls() {
  auto colDefs = lazyState_->getColumnDefinitions();
  std::vector<std::shared_ptr<ColumnImpl>> result;
  result.reserve(colDefs->vec().size());
  for (const auto &cd : colDefs->vec()) {
    result.push_back(ColumnImpl::create(cd.first));
  }
  return result;
}

std::shared_ptr<NumColImpl> TableHandleImpl::getNumColImpl(std::string columnName) {
  lookupHelper(columnName,
      {
          arrow::Type::INT8,
          arrow::Type::INT16,
          arrow::Type::INT32,
          arrow::Type::INT64,
          arrow::Type::UINT8,
          arrow::Type::UINT16,
          arrow::Type::UINT32,
          arrow::Type::UINT64,
          arrow::Type::FLOAT,
          arrow::Type::DOUBLE,
      });
  return NumColImpl::create(std::move(columnName));
}

std::shared_ptr<StrColImpl> TableHandleImpl::getStrColImpl(std::string columnName) {
  lookupHelper(columnName, {arrow::Type::STRING});
  return StrColImpl::create(std::move(columnName));
}

std::shared_ptr<DateTimeColImpl> TableHandleImpl::getDateTimeColImpl(std::string columnName) {
  lookupHelper(columnName, {arrow::Type::DATE64});
  return DateTimeColImpl::create(std::move(columnName));
}

void TableHandleImpl::lookupHelper(const std::string &columnName,
    std::initializer_list<arrow::Type::type> validTypes) {
  auto colDefs = lazyState_->getColumnDefinitions();
  auto ip = colDefs->map().find(columnName);
  if (ip == colDefs->map().end()) {
    throw std::runtime_error(stringf(R"(Column name "%o" is not in the table)", columnName));
  }
  auto actualType = ip->second->id();
  for (auto type : validTypes) {
    if (actualType == type) {
      return;
    }
  }

  auto render = [](std::ostream &s, const arrow::Type::type item) {
    // TODO(kosak): render this as a human-readable string.
    s << item;
  };
  auto message = stringf("Column lookup for %o: Expected Arrow type: one of {%o}. Actual type %o",
    columnName, separatedList(validTypes.begin(), validTypes.end(), ", ", render), actualType);
  throw std::runtime_error(message);
}

void TableHandleImpl::bindToVariableAsync(std::string variable,
    std::shared_ptr<SFCallback<>> callback) {
  struct cb_t final : public SFCallback<BindTableToVariableResponse> {
    explicit cb_t(std::shared_ptr<SFCallback<>> outerCb) : outerCb_(std::move(outerCb)) {}

    void onSuccess(BindTableToVariableResponse /*item*/) final {
      outerCb_->onSuccess();
    }

    void onFailure(std::exception_ptr ep) override {
      outerCb_->onFailure(std::move(ep));
    }

    std::shared_ptr<SFCallback<>> outerCb_;
  };
  auto cb = std::make_shared<cb_t>(std::move(callback));
  managerImpl_->server()->bindToVariableAsync(managerImpl_->consoleId(), ticket_,
      std::move(variable), std::move(cb));
}

void TableHandleImpl::observe() {
  lazyState_->waitUntilReady();
}

namespace internal {
std::shared_ptr<LazyState> LazyState::create(std::shared_ptr<Server> server,
    std::shared_ptr<Executor> flightExecutor) {
  auto result = std::make_shared<LazyState>(Private(), std::move(server), std::move(flightExecutor));
  result->weakSelf_ = result;
  return result;
}

std::shared_ptr<LazyState>
LazyState::createSatisfied(std::shared_ptr<Server> server, std::shared_ptr<Executor> flightExecutor,
    Ticket ticket) {
  auto result = create(std::move(server), std::move(flightExecutor));
  result->ticketPromise_.setValue(std::move(ticket));
  return result;
}

LazyState::LazyState(Private, std::shared_ptr<Server> &&server,
    std::shared_ptr<Executor> &&flightExecutor) : server_(std::move(server)),
    flightExecutor_(std::move(flightExecutor)), ticketFuture_(ticketPromise_.makeFuture()),
    colDefsFuture_(colDefsPromise_.makeFuture()) {}
LazyState::~LazyState() = default;

void LazyState::onSuccess(ExportedTableCreationResponse item) {
  if (!item.result_id().has_ticket()) {
    auto ep = std::make_exception_ptr(std::runtime_error(
        "ExportedTableCreationResponse did not contain a ticket"));
    onFailure(std::move(ep));
    return;
  }
  ticketPromise_.setValue(item.result_id().ticket());
}

void LazyState::onFailure(std::exception_ptr error) {
  ticketPromise_.setError(std::move(error));
}

void LazyState::waitUntilReady() {
  (void)ticketFuture_.value();
}

std::shared_ptr<ColumnDefinitions> LazyState::getColumnDefinitions() {
  // Shortcut if we have column definitions
  if (colDefsFuture_.valid()) {
    // value or exception
    return colDefsFuture_.value();
  }

  auto res = SFCallback<std::shared_ptr<ColumnDefinitions>>::createForFuture();
  getColumnDefinitionsAsync(std::move(res.first));
  return std::get<0>(res.second.get());
}

class GetColumnDefsCallback final :
    public SFCallback<const Ticket &>,
    public SFCallback<const std::shared_ptr<ColumnDefinitions> &>,
    public Callback<> {
  struct Private {};
public:
  static std::shared_ptr<GetColumnDefsCallback> create(std::shared_ptr<LazyState> owner,
      std::shared_ptr<SFCallback<std::shared_ptr<ColumnDefinitions>>> cb) {
    auto result = std::make_shared<GetColumnDefsCallback>(Private(), std::move(owner), std::move(cb));
    result->weakSelf_ = result;
    return result;
  }

  GetColumnDefsCallback(Private, std::shared_ptr<LazyState> &&owner,
      std::shared_ptr<SFCallback<std::shared_ptr<ColumnDefinitions>>> &&cb) :
      owner_(std::move(owner)), outer_(std::move(cb)) {}
  ~GetColumnDefsCallback() final = default;

  void onFailure(std::exception_ptr ep) final {
    outer_->onFailure(std::move(ep));
  }

  void onSuccess(const Ticket &ticket) final {
    auto needsTrigger = owner_->colDefsFuture_.invoke(weakSelf_.lock());
    if (!needsTrigger) {
      return;
    }
    ticket_ = ticket;
    owner_->flightExecutor_->invoke(weakSelf_.lock());
  }

  void onSuccess(const std::shared_ptr<ColumnDefinitions> &colDefs) final {
    outer_->onSuccess(colDefs);
  }

  void invoke() final {
    arrow::flight::FlightCallOptions options;
    options.headers.push_back(owner_->server_->makeBlessing());
    std::unique_ptr<arrow::flight::FlightStreamReader> fsr;
    arrow::flight::Ticket tkt;
    tkt.ticket = ticket_.ticket();

    auto doGetRes = owner_->server_->flightClient()->DoGet(options, tkt, &fsr);
    if (!doGetRes.ok()) {
      auto message = stringf("Doget failed with status %o", doGetRes.ToString());
      auto ep = std::make_exception_ptr(std::runtime_error(message));
      owner_->colDefsPromise_.setError(std::move(ep));
      return;
    }

    auto schemaHolder = fsr->GetSchema();
    if (!schemaHolder.ok()) {
      auto message = stringf("GetSchema failed with status %o", schemaHolder.status().ToString());
      auto ep = std::make_exception_ptr(std::runtime_error(message));
      owner_->colDefsPromise_.setError(std::move(ep));
      return;
    }

    auto &schema = schemaHolder.ValueOrDie();

    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> colVec;
    std::map<std::string, std::shared_ptr<arrow::DataType>> colMap;

    for (const auto &f : schema->fields()) {
      colVec.emplace_back(f->name(), f->type());
      colMap[f->name()] = f->type();
    }
    std::shared_ptr<ColumnDefinitions> colDefs(new ColumnDefinitions(std::move(colVec), std::move(colMap)));
    owner_->colDefsPromise_.setValue(std::move(colDefs));
  }

  std::shared_ptr<LazyState> owner_;
  std::shared_ptr<SFCallback<std::shared_ptr<ColumnDefinitions>>> outer_;
  std::weak_ptr<GetColumnDefsCallback> weakSelf_;
  Ticket ticket_;
};

void LazyState::getColumnDefinitionsAsync(
    std::shared_ptr<SFCallback<std::shared_ptr<ColumnDefinitions>>> cb) {
  auto innerCb = GetColumnDefsCallback::create(weakSelf_.lock(), std::move(cb));
  ticketFuture_.invoke(std::move(innerCb));
}

ColumnDefinitions::ColumnDefinitions(vec_t vec, map_t map) : vec_(std::move(vec)),
    map_(std::move(map)) {}
ColumnDefinitions::~ColumnDefinitions() = default;
}  // namespace internal
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
