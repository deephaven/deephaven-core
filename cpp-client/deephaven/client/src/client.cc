/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/client.h"

#include <grpc/support/log.h>
#include <arrow/array.h>
#include <arrow/scalar.h>
#include "deephaven/client/columns.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/impl/columns_impl.h"
#include "deephaven/client/impl/boolean_expression_impl.h"
#include "deephaven/client/impl/aggregate_impl.h"
#include "deephaven/client/impl/client_impl.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/impl/update_by_operation_impl.h"
#include "deephaven/client/impl/util.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
using io::deephaven::proto::backplane::grpc::HandshakeRequest;
using io::deephaven::proto::backplane::grpc::HandshakeResponse;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::grpc::UpdateByEmOptions;
using deephaven::client::server::Server;
using deephaven::client::Column;
using deephaven::client::DateTimeCol;
using deephaven::client::NumCol;
using deephaven::client::StrCol;
using deephaven::client::impl::StrColImpl;
using deephaven::client::impl::AggregateComboImpl;
using deephaven::client::impl::AggregateImpl;
using deephaven::client::impl::ClientImpl;
using deephaven::client::impl::moveVectorData;
using deephaven::client::impl::UpdateByOperationImpl;
using deephaven::client::subscription::SubscriptionHandle;
using deephaven::client::utility::Executor;
using deephaven::client::utility::okOrThrow;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::utility::base64Encode;
using deephaven::dhcore::utility::makeReservedVector;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::SFCallback;
using deephaven::dhcore::utility::SimpleOstringstream;
using deephaven::dhcore::utility::stringf;

// typedef io::deephaven::proto::backplane::grpc::UpdateByDelta UpdateByDelta;
typedef io::deephaven::proto::backplane::grpc::BadDataBehavior grpcBadDataBehavior;
typedef io::deephaven::proto::backplane::grpc::MathContext grpcMathContext;
typedef io::deephaven::proto::backplane::grpc::MathContext::RoundingMode RoundingMode;
typedef io::deephaven::proto::backplane::grpc::UpdateByNullBehavior UpdateByNullBehavior;
typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation::UpdateByColumn UpdateByColumn;
typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation::UpdateByColumn::UpdateBySpec UpdateBySpec;
typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation GrpcUpdateByOperation;
typedef io::deephaven::proto::backplane::grpc::UpdateByWindowScale::UpdateByWindowTime UpdateByWindowTime;

namespace deephaven::client {
namespace {
void printTableData(std::ostream &s, const TableHandle &tableHandle, bool wantHeaders);
}  // namespace

Client Client::connect(const std::string &target, const ClientOptions &options) {
  auto server = Server::createFromTarget(target, options);
  auto executor = Executor::create("Client executor for " + server->me());
  auto flightExecutor = Executor::create("Flight executor for " + server->me());
  auto impl = ClientImpl::create(std::move(server), executor, flightExecutor, options.sessionType_);
  return Client(std::move(impl));
}

Client::Client() = default;

Client::Client(std::shared_ptr<impl::ClientImpl> impl) : impl_(std::move(impl)) {
}
Client::Client(Client &&other) noexcept = default;
Client &Client::operator=(Client &&other) noexcept = default;

// There is only one Client associated with the server connection. Clients can only be moved, not
// copied. When the Client owning the state is destructed, we tear down the state via close().
Client::~Client() {
  close();
}

// Tear down Client state.
void Client::close() {
  // Move to local variable to be defensive.
  auto temp = std::move(impl_);
  if (temp != nullptr) {
    temp->shutdown();
  }
}

TableHandleManager Client::getManager() const {
  return TableHandleManager(impl_->managerImpl());
}


TableHandleManager::TableHandleManager() = default;
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

TableHandle TableHandleManager::timeTable(std::chrono::system_clock::time_point startTime,
    std::chrono::system_clock::duration period) const {
  auto stNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(startTime.time_since_epoch()).count();
  auto dNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(period).count();
  return timeTable(stNanos, dNanos);
}

std::string TableHandleManager::newTicket() const {
  return impl_->newTicket();
}

TableHandle TableHandleManager::makeTableHandleFromTicket(std::string ticket) const {
  auto handleImpl = impl_->makeTableHandleFromTicket(std::move(ticket));
  return TableHandle(std::move(handleImpl));
}

void TableHandleManager::runScript(std::string code) const {
  auto res = SFCallback<>::createForFuture();
  impl_->runScriptAsync(std::move(code), std::move(res.first));
  (void)res.second.get();
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
TableHandle::TableHandle(std::shared_ptr<impl::TableHandleImpl> impl) : impl_(std::move(impl)) {
}
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

TableHandle TableHandle::updateBy(std::vector<UpdateByOperation> ops, std::vector<std::string> by) const {
  auto opImpls = makeReservedVector<std::shared_ptr<UpdateByOperationImpl>>(ops.size());
  for (const auto &op : ops) {
    opImpls.push_back(op.impl_);
  }
  auto thImpl = impl_->updateBy(std::move(opImpls), std::move(by));
  return TableHandle(std::move(thImpl));
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

int64_t TableHandle::numRows() const {
  return impl_->numRows();
}

bool TableHandle::isStatic() const {
  return impl_->isStatic();
}

std::shared_ptr<Schema> TableHandle::schema() const {
  return impl_->schema();
}

std::shared_ptr<arrow::flight::FlightStreamReader> TableHandle::getFlightStreamReader() const {
  return getManager().createFlightWrapper().getFlightStreamReader(*this);
}

std::shared_ptr<SubscriptionHandle> TableHandle::subscribe(
    std::shared_ptr<TickingCallback> callback) {
  return impl_->subscribe(std::move(callback));
}

std::shared_ptr<SubscriptionHandle>
TableHandle::subscribe(onTickCallback_t onTick, void *onTickUserData,
    onErrorCallback_t onError, void *onErrorUserData) {
  return impl_->subscribe(onTick, onTickUserData, onError, onErrorUserData);
}

void TableHandle::unsubscribe(std::shared_ptr<SubscriptionHandle> callback) {
  impl_->unsubscribe(std::move(callback));
}

const std::string &TableHandle::getTicketAsString() const {
  return impl_->ticket().ticket();
}

std::string TableHandle::toString(bool wantHeaders) const {
  SimpleOstringstream oss;
  oss << stream(true);
  return std::move(oss.str());
}

UpdateByOperation::UpdateByOperation() = default;
UpdateByOperation::UpdateByOperation(std::shared_ptr<impl::UpdateByOperationImpl> impl) :
    impl_(std::move(impl)) {}
UpdateByOperation::UpdateByOperation(const UpdateByOperation &other) = default;
UpdateByOperation &UpdateByOperation::operator=(const UpdateByOperation &other) = default;
UpdateByOperation::UpdateByOperation(UpdateByOperation &&other) noexcept = default;
UpdateByOperation &UpdateByOperation::operator=(UpdateByOperation &&other) noexcept = default;
UpdateByOperation::~UpdateByOperation() = default;

namespace update_by {
namespace {
UpdateByNullBehavior convertDeltaControl(DeltaControl dc) {
  switch (dc) {
    case DeltaControl::NULL_DOMINATES: return UpdateByNullBehavior::NULL_DOMINATES;
    case DeltaControl::VALUE_DOMINATES: return UpdateByNullBehavior::VALUE_DOMINATES;
    case DeltaControl::ZERO_DOMINATES: return UpdateByNullBehavior::ZERO_DOMINATES;
    default: {
      auto message = stringf("Unexpected DeltaControl %o", (int)dc);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
  }
}

grpcBadDataBehavior convertBadDataBehavior(BadDataBehavior bdb) {
  switch (bdb) {
    case BadDataBehavior::RESET: return grpcBadDataBehavior::RESET;
    case BadDataBehavior::SKIP: return grpcBadDataBehavior::SKIP;
    case BadDataBehavior::THROW: return grpcBadDataBehavior::THROW;
    case BadDataBehavior::POISON: return grpcBadDataBehavior::POISON;
    default: {
      auto message = stringf("Unexpected BadDataBehavior %o", (int)bdb);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
  }
}

grpcMathContext convertMathContext(MathContext mctx) {
  int32_t precision;
  RoundingMode roundingMode;
  switch (mctx) {
    case MathContext::UNLIMITED: {
      precision = 0;
      roundingMode = RoundingMode::MathContext_RoundingMode_HALF_UP;
      break;
    }
    case MathContext::DECIMAL32: {
      precision = 7;
      roundingMode = RoundingMode::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    case MathContext::DECIMAL64: {
      precision = 16;
      roundingMode = RoundingMode::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    case MathContext::DECIMAL128: {
      precision = 34;
      roundingMode = RoundingMode::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    default: {
      auto message = stringf("Unexpected MathContext %o", (int)mctx);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
  }
  grpcMathContext result;
  result.set_precision(precision);
  result.set_rounding_mode(roundingMode);
  return result;
}

UpdateByEmOptions convertOperationControl(const OperationControl &oc) {
  auto onNull = convertBadDataBehavior(oc.onNull);
  auto onNan = convertBadDataBehavior(oc.onNaN);
  auto bigValueContext = convertMathContext(oc.bigValueContext);

  UpdateByEmOptions result;
  result.set_on_null_value(onNull);
  result.set_on_nan_value(onNan);
  *result.mutable_big_value_context() = std::move(bigValueContext);
  return result;
};

/**
 * decayTime will be specified as either std::chrono::nanoseconds, or as a string.
 * If it is nanoseconds, we set the nanos field of the UpdateByWindowTime proto. Otherwise (if it is
 * a string), then we set the duration_string field.
 */
UpdateByWindowTime convertDecayTime(std::string timestampCol, durationSpecifier_t decayTime) {
  struct visitor_t {
    void operator()(std::chrono::nanoseconds nanos) {
      result.set_nanos(nanos.count());
    }
    void operator()(std::string duration) {
      *result.mutable_duration_string() = std::move(duration);
    }
    UpdateByWindowTime result;
  };
  visitor_t v;
  // Unconditionally set the column field with the value from timestampCol
  *v.result.mutable_column() = std::move(timestampCol);

  // Conditionally set either the nanos field or the duration_string with the nanoseconds or string
  // part of the variant.
  std::visit(v, std::move(decayTime));
  return std::move(v.result);
}

class UpdateByBuilder {
public:
  explicit UpdateByBuilder(std::vector<std::string> cols) {
    moveVectorData(std::move(cols), gup_.mutable_column()->mutable_match_pairs());
  }

  template<typename MEMBER>
  void touchEmpty(MEMBER mutableMember) {
    (void)(gup_.mutable_column()->mutable_spec()->*mutableMember)();
  }

  template<typename MEMBER>
  void setNullBehavior(MEMBER mutableMember, DeltaControl deltaControl) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    auto nb = convertDeltaControl(deltaControl);
    which->mutable_options()->set_null_behavior(nb);
  }

  template<typename MEMBER>
  void setTicks(MEMBER mutableMember, double decayTicks, const OperationControl &opControl) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_options() = convertOperationControl(opControl);
    which->mutable_window_scale()->mutable_ticks()->set_ticks(decayTicks);
  }

  template<typename MEMBER>
  void setTime(MEMBER mutableMember, std::string timestampCol, durationSpecifier_t decayTime,
      const OperationControl &opControl) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_options() = convertOperationControl(opControl);
    *which->mutable_window_scale()->mutable_time() =
        convertDecayTime(std::move(timestampCol), std::move(decayTime));
  }

  template<typename MEMBER>
  void setRevAndFwdTicks(MEMBER mutableMember, int revTicks, int fwdTicks) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    which->mutable_reverse_window_scale()->mutable_ticks()->set_ticks(revTicks);
    which->mutable_forward_window_scale()->mutable_ticks()->set_ticks(fwdTicks);
  }

  template<typename MEMBER>
  void setRevAndFwdTime(MEMBER mutableMember, std::string timestampCol,
      durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_reverse_window_scale()->mutable_time() =
        convertDecayTime(timestampCol, std::move(revTime));
    *which->mutable_forward_window_scale()->mutable_time() =
        convertDecayTime(std::move(timestampCol), std::move(fwdTime));
  }

  template<typename MEMBER>
  void setWeightedRevAndFwdTicks(MEMBER mutableMember, std::string weightCol, int revTicks,
      int fwdTicks) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_weight_column() = std::move(weightCol);
    setRevAndFwdTicks(mutableMember, revTicks, fwdTicks);
  }

  template<typename MEMBER>
  void setWeightedRevAndFwdTime(MEMBER mutableMember, std::string timestampCol,
      std::string weightCol, durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_weight_column() = std::move(weightCol);
    setRevAndFwdTime(mutableMember, std::move(timestampCol), std::move(revTime), std::move(fwdTime));
  }

  UpdateByOperation build() {
    auto impl = std::make_shared<UpdateByOperationImpl>(std::move(gup_));
    return UpdateByOperation(std::move(impl));
  }

  GrpcUpdateByOperation gup_;
};
}  // namespace

UpdateByOperation cumSum(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_sum);
  return ubb.build();
}

UpdateByOperation cumProd(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_product);
  return ubb.build();
}

UpdateByOperation cumMin(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_min);
  return ubb.build();
}

UpdateByOperation cumMax(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_max);
  return ubb.build();
}

UpdateByOperation forwardFill(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_fill);
  return ubb.build();
}

UpdateByOperation delta(std::vector<std::string> cols, DeltaControl deltaControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setNullBehavior(&UpdateBySpec::mutable_delta, deltaControl);
  return ubb.build();
}

UpdateByOperation emaTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_ema, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emaTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_ema, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation emsTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_ems, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emsTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_ems, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation emminTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_em_min, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emminTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_em_min, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation emmaxTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_em_max, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emmaxTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_em_max, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation emstdTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_em_std, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emstdTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_em_std, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation rollingSumTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_sum, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingSumTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_sum, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingGroupTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_group, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingGroupTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_group, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingAvgTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_avg, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingAvgTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_avg, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingMinTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_min, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingMinTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_min, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingMaxTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_max, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingMaxTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_max, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingProdTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_product, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingProdTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_product, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingCountTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_count, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingCountTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_count, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingStdTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_std, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingStdTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_std, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingWavgTick(std::string weightCol, std::vector<std::string> cols,
    int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setWeightedRevAndFwdTicks(&UpdateBySpec::mutable_rolling_wavg, std::move(weightCol), revTicks,
      fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingWavgTime(std::string timestampCol, std::string weightCol,
    std::vector<std::string> cols, durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setWeightedRevAndFwdTime(&UpdateBySpec::mutable_rolling_wavg, std::move(timestampCol),
      std::move(weightCol), std::move(revTime), std::move(fwdTime));
  return ubb.build();
}
}  // namespace update_by

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
