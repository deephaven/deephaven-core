/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/client.h"

#include <arrow/array.h>
#include <arrow/scalar.h>
#include "deephaven/client/columns.h"
#include "deephaven/client/impl/columns_impl.h"
#include "deephaven/client/impl/boolean_expression_impl.h"
#include "deephaven/client/impl/aggregate_impl.h"
#include "deephaven/client/impl/client_impl.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/utility.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
using io::deephaven::proto::backplane::grpc::HandshakeRequest;
using io::deephaven::proto::backplane::grpc::HandshakeResponse;
using io::deephaven::proto::backplane::grpc::Ticket;
using deephaven::client::server::Server;
using deephaven::client::Column;
using deephaven::client::DateTimeCol;
using deephaven::client::NumCol;
using deephaven::client::StrCol;
using deephaven::client::impl::StrColImpl;
using deephaven::client::impl::AggregateComboImpl;
using deephaven::client::impl::AggregateImpl;
using deephaven::client::impl::ClientImpl;
using deephaven::client::subscription::SubscriptionHandle;
using deephaven::client::utility::Executor;
using deephaven::client::utility::SimpleOstringstream;
using deephaven::client::utility::SFCallback;
using deephaven::client::utility::separatedList;
using deephaven::client::utility::stringf;
using deephaven::client::utility::okOrThrow;

namespace deephaven::client {
namespace {
void printTableData(std::ostream &s, const TableHandle &tableHandle, bool wantHeaders);
}  // namespace

Client Client::connect(const std::string &target) {
  auto executor = Executor::create();
  auto flightExecutor = Executor::create();
  auto server = Server::createFromTarget(target);
  auto impl = ClientImpl::create(std::move(server), executor, flightExecutor);
  return Client(std::move(impl));
}

Client::Client(std::shared_ptr<impl::ClientImpl> impl) : impl_(std::move(impl)) {}
Client::Client(Client &&other) noexcept = default;
Client &Client::operator=(Client &&other) noexcept = default;
Client::~Client() = default;

TableHandleManager Client::getManager() const {
  return TableHandleManager(impl_->managerImpl());
}

TableHandleManager::TableHandleManager(std::shared_ptr<impl::TableHandleManagerImpl> impl) : impl_(std::move(impl)) {}
TableHandleManager::TableHandleManager(TableHandleManager &&other) noexcept = default;
TableHandleManager &TableHandleManager::operator=(TableHandleManager &&other) noexcept = default;
TableHandleManager::~TableHandleManager() = default;

TableHandle TableHandleManager::emptyTable(int64_t size) const {
  auto qsImpl = impl_->emptyTable(size);
  return TableHandle(std::move(qsImpl));
}

TableHandle TableHandleManager::fetchTable(std::string tableName) const {
  auto qsImpl = impl_->fetchTable(std::move(tableName));
  return TableHandle(std::move(qsImpl));
}

TableHandle TableHandleManager::timeTable(int64_t startTimeNanos, int64_t periodNanos) const {
  auto qsImpl = impl_->timeTable(startTimeNanos, periodNanos);
  return TableHandle(std::move(qsImpl));
}

std::tuple<TableHandle, arrow::flight::FlightDescriptor> TableHandleManager::newTableHandleAndFlightDescriptor() const {
  auto [thImpl, fd] = impl_->newTicket();
  TableHandle th(std::move(thImpl));
  return std::make_tuple(std::move(th), std::move(fd));
}

TableHandle TableHandleManager::timeTable(std::chrono::system_clock::time_point startTime,
    std::chrono::system_clock::duration period) const {
  auto stNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(startTime.time_since_epoch()).count();
  auto dNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(period).count();
  return timeTable(stNanos, dNanos);
}

FlightWrapper TableHandleManager::createFlightWrapper() const {
  return FlightWrapper(impl_);
}

namespace {
ComboAggregateRequest::Aggregate
createDescForMatchPairs(ComboAggregateRequest::AggType aggregateType,
    std::vector<std::string> columnSpecs) {
  ComboAggregateRequest::Aggregate result;
  result.set_type(aggregateType);
  for (auto &cs : columnSpecs) {
    result.mutable_match_pairs()->Add(std::move(cs));
  }
  return result;
}

ComboAggregateRequest::Aggregate createDescForColumn(ComboAggregateRequest::AggType aggregateType,
    std::string columnSpec) {
  ComboAggregateRequest::Aggregate result;
  result.set_type(aggregateType);
  result.set_column_name(std::move(columnSpec));
  return result;
}

Aggregate createAggForMatchPairs(ComboAggregateRequest::AggType aggregateType, std::vector<std::string> columnSpecs) {
  auto ad = createDescForMatchPairs(aggregateType, std::move(columnSpecs));
  auto impl = AggregateImpl::create(std::move(ad));
  return Aggregate(std::move(impl));
}
}  // namespace

Aggregate::Aggregate(std::shared_ptr<impl::AggregateImpl> impl) : impl_(std::move(impl)) {
}

Aggregate Aggregate::absSum(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::ABS_SUM, std::move(columnSpecs));
}

Aggregate Aggregate::avg(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::AVG, std::move(columnSpecs));
}

Aggregate Aggregate::count(std::string columnSpec) {
  auto ad = createDescForColumn(ComboAggregateRequest::COUNT, std::move(columnSpec));
  auto impl = AggregateImpl::create(std::move(ad));
  return Aggregate(std::move(impl));
}

Aggregate Aggregate::first(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::FIRST, std::move(columnSpecs));
}

Aggregate Aggregate::last(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::LAST, std::move(columnSpecs));
}

Aggregate Aggregate::max(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::MAX, std::move(columnSpecs));
}

Aggregate Aggregate::med(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::MEDIAN, std::move(columnSpecs));
}

Aggregate Aggregate::min(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::MIN, std::move(columnSpecs));
}

Aggregate Aggregate::pct(double percentile, bool avgMedian, std::vector<std::string> columnSpecs) {
  ComboAggregateRequest::Aggregate pd;
  pd.set_type(ComboAggregateRequest::PERCENTILE);
  pd.set_percentile(percentile);
  pd.set_avg_median(avgMedian);
  for (auto &cs : columnSpecs) {
    pd.mutable_match_pairs()->Add(std::move(cs));
  }
  auto impl = AggregateImpl::create(std::move(pd));
  return Aggregate(std::move(impl));
}

Aggregate Aggregate::std(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::STD, std::move(columnSpecs));
}

Aggregate Aggregate::sum(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::SUM, std::move(columnSpecs));
}

Aggregate Aggregate::var(std::vector<std::string> columnSpecs) {
  return createAggForMatchPairs(ComboAggregateRequest::VAR, std::move(columnSpecs));
}

Aggregate Aggregate::wavg(std::string weightColumn, std::vector<std::string> columnSpecs) {
  ComboAggregateRequest::Aggregate pd;
  pd.set_type(ComboAggregateRequest::WEIGHTED_AVG);
  for (auto &cs : columnSpecs) {
    pd.mutable_match_pairs()->Add(std::move(cs));
  }
  pd.set_column_name(std::move(weightColumn));
  auto impl = AggregateImpl::create(std::move(pd));
  return Aggregate(std::move(impl));
}

AggregateCombo AggregateCombo::create(std::initializer_list<Aggregate> list) {
  std::vector<ComboAggregateRequest::Aggregate> aggregates;
  aggregates.reserve(list.size());
  for (const auto &item : list) {
    aggregates.push_back(item.impl()->descriptor());
  }
  auto impl = AggregateComboImpl::create(std::move(aggregates));
  return AggregateCombo(std::move(impl));
}

AggregateCombo AggregateCombo::create(std::vector<Aggregate> vec) {
  std::vector<ComboAggregateRequest::Aggregate> aggregates;
  aggregates.reserve(vec.size());
  for (const auto &item : vec) {
    aggregates.push_back(std::move(item.impl()->descriptor()));
  }
  auto impl = AggregateComboImpl::create(std::move(aggregates));
  return AggregateCombo(std::move(impl));
}

AggregateCombo::AggregateCombo(std::shared_ptr<impl::AggregateComboImpl> impl) : impl_(std::move(impl)) {}
AggregateCombo::AggregateCombo(AggregateCombo &&other) noexcept = default;
AggregateCombo &AggregateCombo::operator=(AggregateCombo &&other) noexcept = default;
AggregateCombo::~AggregateCombo() = default;

TableHandle::TableHandle() = default;
TableHandle::TableHandle(std::shared_ptr<impl::TableHandleImpl> impl) : impl_(std::move(impl)) {}
TableHandle::TableHandle(const TableHandle &other) = default;
TableHandle &TableHandle::operator=(const TableHandle &other) = default;
TableHandle::TableHandle(TableHandle &&other) noexcept = default;
TableHandle &TableHandle::operator=(TableHandle &&other) noexcept = default;
TableHandle::~TableHandle() = default;

TableHandleManager TableHandle::getManager() const {
  return TableHandleManager(impl_->managerImpl());
}

TableHandle TableHandle::where(const BooleanExpression &condition) const {
  SimpleOstringstream oss;
  condition.implAsBooleanExpressionImpl()->streamIrisRepresentation(oss);
  return where(std::move(oss.str()));
}

TableHandle TableHandle::where(std::string condition) const {
  auto qtImpl = impl_->where(std::move(condition));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::sort(std::vector<SortPair> sortPairs) const {
  auto qtImpl = impl_->sort(std::move(sortPairs));
  return TableHandle(std::move(qtImpl));
}

std::vector<Column> TableHandle::getAllCols() const {
  auto columnImpls = impl_->getColumnImpls();
  std::vector<Column> result;
  result.reserve(columnImpls.size());
  for (const auto &ci : columnImpls) {
    result.emplace_back(ci);
  }
  return result;
}

StrCol TableHandle::getStrCol(std::string columnName) const {
  auto scImpl = impl_->getStrColImpl(std::move(columnName));
  return StrCol(std::move(scImpl));
}

NumCol TableHandle::getNumCol(std::string columnName) const {
  auto ncImpl = impl_->getNumColImpl(std::move(columnName));
  return NumCol(std::move(ncImpl));
}

DateTimeCol TableHandle::getDateTimeCol(std::string columnName) const {
  auto dtImpl = impl_->getDateTimeColImpl(std::move(columnName));
  return DateTimeCol(std::move(dtImpl));
}

TableHandle TableHandle::select(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->select(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::update(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->update(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::view(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->view(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::dropColumns(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->dropColumns(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::updateView(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->updateView(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::by(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->by(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::by(AggregateCombo combo, std::vector<std::string> groupByColumns) const {
  auto qtImpl = impl_->by(combo.impl()->aggregates(), std::move(groupByColumns));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::minBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->minBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::maxBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->maxBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::sumBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->sumBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::absSumBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->absSumBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::varBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->varBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::stdBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->stdBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::avgBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->avgBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::lastBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->lastBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::firstBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->firstBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::medianBy(std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->medianBy(std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::percentileBy(double percentile, bool avgMedian,
    std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->percentileBy(percentile, avgMedian, std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::percentileBy(double percentile, std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->percentileBy(percentile, std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::countBy(std::string countByColumn, std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->countBy(std::move(countByColumn), std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::wAvgBy(std::string weightColumn, std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->wAvgBy(std::move(weightColumn), std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::tailBy(int64_t n, std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->tailBy(n, std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::headBy(int64_t n, std::vector<std::string> columnSpecs) const {
  auto qtImpl = impl_->headBy(n, std::move(columnSpecs));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::head(int64_t n) const {
  auto qtImpl = impl_->head(n);
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::tail(int64_t n) const {
  auto qtImpl = impl_->tail(n);
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::ungroup(bool nullFill, std::vector<std::string> groupByColumns) const {
  auto qtImpl = impl_->ungroup(nullFill, std::move(groupByColumns));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::merge(std::string keyColumn, std::vector<TableHandle> sources) const {
  std::vector<Ticket> sourceHandles;
  sourceHandles.reserve(sources.size() + 1);
  sourceHandles.push_back(impl_->ticket());
  for (const auto &s : sources) {
    sourceHandles.push_back(s.impl()->ticket());
  }
  auto qtImpl = impl_->merge(std::move(keyColumn), std::move(sourceHandles));
  return TableHandle(std::move(qtImpl));
}

FlightWrapper::FlightWrapper(std::shared_ptr<impl::TableHandleManagerImpl> impl) : impl_(std::move(impl)) {}
FlightWrapper::~FlightWrapper() = default;

std::shared_ptr<arrow::flight::FlightStreamReader> FlightWrapper::getFlightStreamReader(
    const TableHandle &table) const {
  const auto *server = impl_->server().get();

  arrow::flight::FlightCallOptions options;
  options.headers.push_back(server->makeBlessing());
  std::unique_ptr<arrow::flight::FlightStreamReader> fsr;
  arrow::flight::Ticket tkt;
  tkt.ticket = table.impl()->ticket().ticket();

  okOrThrow(DEEPHAVEN_EXPR_MSG(server->flightClient()->DoGet(options, tkt, &fsr)));
  return fsr;
}

void FlightWrapper::addAuthHeaders(arrow::flight::FlightCallOptions *options) {
  const auto *server = impl_->server().get();
  options->headers.push_back(server->makeBlessing());
}

arrow::flight::FlightClient *FlightWrapper::flightClient() const {
  const auto *server = impl_->server().get();
  return server->flightClient();
}

namespace {
template<typename T>
std::vector<std::string> toIrisRepresentation(const std::vector<T> &items) {
  std::vector<std::string> result;
  result.reserve(items.size());
  for (const auto &ctm : items) {
    SimpleOstringstream oss;
    ctm.getIrisRepresentableImpl()->streamIrisRepresentation(oss);
    result.push_back(std::move(oss.str()));
  }
  return result;
}
}  // namespace

TableHandle TableHandle::crossJoin(const TableHandle &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto qtImpl = impl_->crossJoin(*rightSide.impl_, std::move(columnsToMatch),
      std::move(columnsToAdd));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::crossJoin(const TableHandle &rightSide,
    std::vector<MatchWithColumn> columnsToMatch, std::vector<SelectColumn> columnsToAdd) const {
  auto ctmStrings = toIrisRepresentation(columnsToMatch);
  auto ctaStrings = toIrisRepresentation(columnsToAdd);
  return crossJoin(rightSide, std::move(ctmStrings), std::move(ctaStrings));
}

TableHandle TableHandle::naturalJoin(const TableHandle &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto qtImpl = impl_->naturalJoin(*rightSide.impl_, std::move(columnsToMatch),
      std::move(columnsToAdd));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::naturalJoin(const TableHandle &rightSide,
    std::vector<MatchWithColumn> columnsToMatch, std::vector<SelectColumn> columnsToAdd) const {
  auto ctmStrings = toIrisRepresentation(columnsToMatch);
  auto ctaStrings = toIrisRepresentation(columnsToAdd);
  return naturalJoin(rightSide, std::move(ctmStrings), std::move(ctaStrings));
}

TableHandle TableHandle::exactJoin(const TableHandle &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto qtImpl = impl_->exactJoin(*rightSide.impl_, std::move(columnsToMatch),
      std::move(columnsToAdd));
  return TableHandle(std::move(qtImpl));
}

TableHandle TableHandle::exactJoin(const TableHandle &rightSide,
    std::vector<MatchWithColumn> columnsToMatch, std::vector<SelectColumn> columnsToAdd) const {
  auto ctmStrings = toIrisRepresentation(columnsToMatch);
  auto ctaStrings = toIrisRepresentation(columnsToAdd);
  return exactJoin(rightSide, std::move(ctmStrings), std::move(ctaStrings));
}

void TableHandle::bindToVariable(std::string variable) const {
  auto res = SFCallback<>::createForFuture();
  bindToVariableAsync(std::move(variable), std::move(res.first));
  (void)res.second.get();
}

void TableHandle::bindToVariableAsync(std::string variable,
    std::shared_ptr<SFCallback<>> callback) const {
  return impl_->bindToVariableAsync(std::move(variable), std::move(callback));
}

internal::TableHandleStreamAdaptor TableHandle::stream(bool wantHeaders) const {
  return {*this, wantHeaders};
}

void TableHandle::observe() const {
  impl_->observe();
}

std::shared_ptr<arrow::flight::FlightStreamReader> TableHandle::getFlightStreamReader() const {
  return getManager().createFlightWrapper().getFlightStreamReader(*this);
}

std::shared_ptr<SubscriptionHandle> TableHandle::subscribe(
    std::shared_ptr<TickingCallback> callback) {
  return impl_->subscribe(std::move(callback));
}

void TableHandle::unsubscribe(std::shared_ptr<SubscriptionHandle> callback) {
  impl_->unsubscribe(std::move(callback));
}

const std::string &TableHandle::getTicketAsString() const {
  return impl_->ticket().ticket();
}

namespace internal {
TableHandleStreamAdaptor::TableHandleStreamAdaptor(TableHandle table, bool wantHeaders) :
    table_(std::move(table)), wantHeaders_(wantHeaders) {}
TableHandleStreamAdaptor::~TableHandleStreamAdaptor() = default;

std::ostream &operator<<(std::ostream &s, const TableHandleStreamAdaptor &o) {
  printTableData(s, o.table_, o.wantHeaders_);
  return s;
}

std::string ConvertToString::toString(
    const deephaven::client::SelectColumn &selectColumn) {
  SimpleOstringstream oss;
  selectColumn.getIrisRepresentableImpl()->streamIrisRepresentation(oss);
  return std::move(oss.str());
}
}  // namespace internal

namespace {
void printTableData(std::ostream &s, const TableHandle &tableHandle, bool wantHeaders) {
  auto fsr = tableHandle.getFlightStreamReader();

  if (wantHeaders) {
    auto cols = tableHandle.getAllCols();
    auto streamName = [](std::ostream &s, const Column &c) {
      s << c.name();
    };
    s << separatedList(cols.begin(), cols.end(), "\t", streamName) << '\n';
  }

  while (true) {
    arrow::flight::FlightStreamChunk chunk;
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr->Next(&chunk)));
    if (chunk.data == nullptr) {
      break;
    }
    const auto *data = chunk.data.get();
    const auto &columns = data->columns();
    for (int64_t rowNum = 0; rowNum < data->num_rows(); ++rowNum) {
      if (rowNum != 0) {
        s << '\n';
      }
      auto streamArrayCell = [rowNum](std::ostream &s, const std::shared_ptr<arrow::Array> &a) {
        // This is going to be rather inefficient
        auto rsc = a->GetScalar(rowNum);
        const auto &vsc = *rsc.ValueOrDie();
        s << vsc.ToString();
      };
      s << separatedList(columns.begin(), columns.end(), "\t", streamArrayCell);
    }
  }
}
}  // namespace
}  // namespace deephaven::client

