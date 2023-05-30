/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/table_handle_impl.h"

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include "deephaven/client/arrowutil/arrow_flight.h"
#include "deephaven/client/impl/boolean_expression_impl.h"
#include "deephaven/client/impl/columns_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/client.h"
#include "deephaven/client/columns.h"
#include "deephaven/client/subscription/subscribe_thread.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/table/table.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/utility/callbacks.h"
#include "deephaven/dhcore/utility/utility.h"

using io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
using io::deephaven::proto::backplane::grpc::SortDescriptor;
using io::deephaven::proto::backplane::grpc::TableReference;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse;
using deephaven::client::SortDirection;
using deephaven::client::arrowutil::ArrowUtil;
using deephaven::client::impl::ColumnImpl;
using deephaven::client::impl::DateTimeColImpl;
using deephaven::client::impl::NumColImpl;
using deephaven::client::impl::StrColImpl;
using deephaven::client::server::Server;
using deephaven::client::subscription::SubscriptionThread;
using deephaven::client::subscription::SubscriptionHandle;
using deephaven::client::utility::Executor;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::valueOrThrow;
using deephaven::dhcore::chunk::ChunkMaker;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::column::MutableColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;
using deephaven::dhcore::container::RowSequenceIterator;
using deephaven::dhcore::ElementTypeId;
using deephaven::dhcore::table::Schema;
using deephaven::dhcore::table::Table;
using deephaven::dhcore::ticking::TickingCallback;
using deephaven::dhcore::ticking::TickingUpdate;
using deephaven::dhcore::utility::Callback;
using deephaven::dhcore::utility::CBPromise;
using deephaven::dhcore::utility::getWhat;
using deephaven::dhcore::utility::makeReservedVector;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::SFCallback;
using deephaven::dhcore::utility::streamf;
using deephaven::dhcore::utility::stringf;

namespace deephaven::client {
namespace impl {
std::pair<std::shared_ptr<internal::ExportedTableCreationCallback>, std::shared_ptr<internal::LazyState>>
TableHandleImpl::createEtcCallback(const TableHandleManagerImpl *thm) {
  CBPromise<Ticket> ticketPromise;
  auto ticketFuture = ticketPromise.makeFuture();
  auto cb = std::make_shared<internal::ExportedTableCreationCallback>(std::move(ticketPromise));
  auto ls = std::make_shared<internal::LazyState>(thm->server(), thm->flightExecutor(),
      std::move(ticketFuture));
  return std::make_pair(std::move(cb), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::create(std::shared_ptr<TableHandleManagerImpl> thm,
    Ticket ticket, std::shared_ptr<internal::LazyState> lazyState) {
  return std::make_shared<TableHandleImpl>(Private(), std::move(thm), std::move(ticket),
      std::move(lazyState));
}

TableHandleImpl::TableHandleImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&thm,
    Ticket &&ticket, std::shared_ptr<internal::LazyState> &&lazyState) :
    managerImpl_(std::move(thm)), ticket_(std::move(ticket)), lazyState_(std::move(lazyState)) {
}

// TODO(kosak): Need to actually send Release to the server
TableHandleImpl::~TableHandleImpl() = default;

std::shared_ptr<TableHandleImpl> TableHandleImpl::select(std::vector<std::string> columnSpecs) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->selectAsync(ticket_, std::move(columnSpecs),
      std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::update(std::vector<std::string> columnSpecs) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->updateAsync(ticket_, std::move(columnSpecs),
      std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::view(std::vector<std::string> columnSpecs) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->viewAsync(ticket_, std::move(columnSpecs),
      std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::dropColumns(std::vector<std::string> columnSpecs) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->dropColumnsAsync(ticket_, std::move(columnSpecs),
      std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::updateView(std::vector<std::string> columnSpecs) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->updateViewAsync(ticket_, std::move(columnSpecs),
      std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::where(std::string condition) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->whereAsync(ticket_, std::move(condition),
      std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::sort(std::vector<SortPair> sortPairs) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
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
  auto resultTicket = managerImpl_->server()->sortAsync(ticket_, std::move(sortDescriptors),
      std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::preemptive(int32_t sampleIntervalMs) {
  throw std::runtime_error("TODO: kosak");
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

  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->comboAggregateDescriptorAsync(ticket_,
      std::move(descriptors), std::move(columnSpecs), false, std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
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
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->comboAggregateDescriptorAsync(ticket_,
      std::move(descriptors), std::move(groupByColumns), false, std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
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
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->headOrTailByAsync(ticket_, head, n,
      std::move(columnSpecs), std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::tail(int64_t n) {
  return headOrTailHelper(false, n);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::head(int64_t n) {
  return headOrTailHelper(true, n);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::headOrTailHelper(bool head, int64_t n) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->headOrTailAsync(ticket_, head, n, std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::ungroup(bool nullFill,
    std::vector<std::string> groupByColumns) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->ungroupAsync(ticket_, nullFill, std::move(groupByColumns),
      std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::merge(std::string keyColumn,
    std::vector<Ticket> sourceTickets) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->mergeAsync(std::move(sourceTickets),
      std::move(keyColumn), std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::crossJoin(const TableHandleImpl &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->crossJoinAsync(ticket_, rightSide.ticket_,
      std::move(columnsToMatch), std::move(columnsToAdd), std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::naturalJoin(const TableHandleImpl &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->naturalJoinAsync(ticket_, rightSide.ticket_,
      std::move(columnsToMatch), std::move(columnsToAdd), std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::exactJoin(const TableHandleImpl &rightSide,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) const {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->exactJoinAsync(ticket_, rightSide.ticket_,
      std::move(columnsToMatch), std::move(columnsToAdd), std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::asOfJoin(AsOfJoinTablesRequest::MatchRule matchRule,
    const TableHandleImpl &rightSide, std::vector<std::string> columnsToMatch,
    std::vector<std::string> columnsToAdd) {
  auto [cb, ls] = TableHandleImpl::createEtcCallback(managerImpl_.get());
  auto resultTicket = managerImpl_->server()->asOfJoinAsync(matchRule, ticket_,
      rightSide.ticket(), std::move(columnsToMatch), std::move(columnsToAdd), std::move(cb));
  return TableHandleImpl::create(managerImpl_, std::move(resultTicket), std::move(ls));
}

std::shared_ptr<SubscriptionHandle> TableHandleImpl::subscribe(
    std::shared_ptr<TickingCallback> callback) {
  // On the flight executor thread, we invoke DoExchange (waiting for a successful response).
  // We wait for that response here. That makes the first part of this call synchronous. If there
  // is an error in the DoExchange invocation, the caller will get an exception here. The
  // remainder of the interaction (namely, the sending of a BarrageSubscriptionRequest and the
  // parsing of all the replies) is done on a newly-created thread dedicated to that job.
  auto schema = lazyState_->getSchema();
  auto handle = SubscriptionThread::start(managerImpl_->server(), managerImpl_->flightExecutor().get(),
    schema, ticket_, std::move(callback));
  subscriptions_.insert(handle);
  return handle;
}

namespace {
class CStyleTickingCallback final : public TickingCallback {
public:
  CStyleTickingCallback(TableHandle::onTickCallback_t onTick, void *onTickUserData,
     TableHandle::onErrorCallback_t onError, void *onErrorUserData) : onTick_(onTick),
     onTickUserData_(onTickUserData), onError_(onError), onErrorUserData_(onErrorUserData) {}

  void onTick(TickingUpdate update) final {
    onTick_(std::move(update), onTickUserData_);
  }

  void onFailure(std::exception_ptr eptr) final {
    auto what = getWhat(std::move(eptr));
    onError_(std::move(what), onErrorUserData_);
  }

private:
  TableHandle::onTickCallback_t onTick_ = nullptr;
  void *onTickUserData_ = nullptr;
  TableHandle::onErrorCallback_t onError_ = nullptr;
  void *onErrorUserData_ = nullptr;
};
}

std::shared_ptr<SubscriptionHandle>
TableHandleImpl::subscribe(TableHandle::onTickCallback_t onTick, void *onTickUserData,
    TableHandle::onErrorCallback_t onError, void *onErrorUserData) {
  auto cb = std::make_shared<CStyleTickingCallback>(onTick, onTickUserData, onError, onErrorUserData);
  return subscribe(std::move(cb));
}

void TableHandleImpl::unsubscribe(std::shared_ptr<SubscriptionHandle> handle) {
  auto node = subscriptions_.extract(handle);
  if (node.empty()) {
    return;
  }
  node.value()->cancel(true);
}

std::vector<std::shared_ptr<ColumnImpl>> TableHandleImpl::getColumnImpls() {
  auto schema = lazyState_->getSchema();
  std::vector<std::shared_ptr<ColumnImpl>> result;
  result.reserve(schema->names().size());
  for (const auto &name : schema->names()) {
    result.push_back(ColumnImpl::create(name));
  }
  return result;
}

std::shared_ptr<NumColImpl> TableHandleImpl::getNumColImpl(std::string columnName) {
  lookupHelper(columnName,
      {
          ElementTypeId::INT8,
          ElementTypeId::INT16,
          ElementTypeId::INT32,
          ElementTypeId::INT64,
          ElementTypeId::FLOAT,
          ElementTypeId::DOUBLE,
      });
  return NumColImpl::create(std::move(columnName));
}

std::shared_ptr<StrColImpl> TableHandleImpl::getStrColImpl(std::string columnName) {
  lookupHelper(columnName, {ElementTypeId::STRING});
  return StrColImpl::create(std::move(columnName));
}

std::shared_ptr<DateTimeColImpl> TableHandleImpl::getDateTimeColImpl(std::string columnName) {
  lookupHelper(columnName, {ElementTypeId::TIMESTAMP});
  return DateTimeColImpl::create(std::move(columnName));
}

void TableHandleImpl::lookupHelper(const std::string &columnName,
    std::initializer_list<ElementTypeId::Enum> validTypes) {
  auto schema = lazyState_->getSchema();
  auto index = *schema->getColumnIndex(columnName, true);
  auto actualType = schema->types()[index];
  for (auto type : validTypes) {
    if (actualType == type) {
      return;
    }
  }

  auto render = [](std::ostream &s, ElementTypeId::Enum item) {
    // TODO(kosak): render this as a human-readable string.
    s << (int)item;
  };
  auto message = stringf("Column lookup for %o: Expected Arrow type: one of {%o}. Actual type %o",
    columnName, separatedList(validTypes.begin(), validTypes.end(), ", ", render), (int)actualType);
  throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
}

void TableHandleImpl::bindToVariableAsync(std::string variable, std::shared_ptr<SFCallback<>> callback) {
  struct cb_t final : public SFCallback<BindTableToVariableResponse> {
    explicit cb_t(std::shared_ptr<SFCallback<>> outerCb) : outerCb_(std::move(outerCb)) {}

    void onSuccess(BindTableToVariableResponse /*item*/) final {
      outerCb_->onSuccess();
    }

    void onFailure(std::exception_ptr ep) final {
      outerCb_->onFailure(std::move(ep));
    }

    std::shared_ptr<SFCallback<>> outerCb_;
  };
  if (!managerImpl_->consoleId().has_value()) {
    auto eptr = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(
        "Client was created without specifying a script language")));
    callback->onFailure(std::move(eptr));
    return;
  }
  auto cb = std::make_shared<cb_t>(std::move(callback));
  managerImpl_->server()->bindToVariableAsync(*managerImpl_->consoleId(), ticket_,
      std::move(variable), std::move(cb));
}

void TableHandleImpl::observe() {
  lazyState_->waitUntilReady();
}

namespace internal {
ExportedTableCreationCallback::ExportedTableCreationCallback(CBPromise<Ticket> &&ticketPromise) :
  ticketPromise_(std::move(ticketPromise)) {}
ExportedTableCreationCallback::~ExportedTableCreationCallback() = default;

void ExportedTableCreationCallback::onSuccess(ExportedTableCreationResponse item) {
  if (!item.result_id().has_ticket()) {
    const char *message = "ExportedTableCreationResponse did not contain a ticket";
    auto ep = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(message)));
    onFailure(std::move(ep));
    return;
  }
  ticketPromise_.setValue(item.result_id().ticket());
}

void ExportedTableCreationCallback::onFailure(std::exception_ptr error) {
  ticketPromise_.setError(std::move(error));
}

void LazyState::waitUntilReady() {
  (void)ticketFuture_.value();
}

namespace {
struct ArrowToElementTypeId final : public arrow::TypeVisitor {
  arrow::Status Visit(const arrow::Int8Type &type) final {
    typeId_ = ElementTypeId::INT8;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &type) final {
    typeId_ = ElementTypeId::INT16;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &type) final {
    typeId_ = ElementTypeId::INT32;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &type) final {
    typeId_ = ElementTypeId::INT64;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &type) final {
    typeId_ = ElementTypeId::FLOAT;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &type) final {
    typeId_ = ElementTypeId::DOUBLE;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &type) final {
    typeId_ = ElementTypeId::BOOL;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt16Type &type) final {
    typeId_ = ElementTypeId::CHAR;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &type) final {
    typeId_ = ElementTypeId::STRING;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType &type) final {
    typeId_ = ElementTypeId::TIMESTAMP;
    return arrow::Status::OK();
  }


  ElementTypeId::Enum typeId_ = ElementTypeId::INT8;  // arbitrary initializer
};

}  // namespace

class GetSchemaCallback final :
    public SFCallback<Ticket>,
    public Callback<>,
    public std::enable_shared_from_this<GetSchemaCallback> {
public:
  GetSchemaCallback(std::shared_ptr<Server> server, std::shared_ptr<Executor> flightExecutor,
     CBPromise<std::shared_ptr<Schema>> schemaPromise) : server_(std::move(server)),
     flightExecutor_(std::move(flightExecutor)), schemaPromise_(std::move(schemaPromise)) {}
  ~GetSchemaCallback() final = default;

  void onFailure(std::exception_ptr ep) final {
    schemaPromise_.setError(std::move(ep));
  }

  void onSuccess(Ticket ticket) final {
    ticket_ = std::move(ticket);
    flightExecutor_->invoke(shared_from_this());
  }

  void invoke() final {
    try {
      invokeHelper();
    } catch (...) {
      schemaPromise_.setError(std::current_exception());
    }
  }

  void invokeHelper() {
    arrow::flight::FlightCallOptions options;
    options.headers.push_back(server_->getAuthHeader());

    arrow::flight::FlightDescriptor fd;
    if (!ArrowUtil::tryConvertTicketToFlightDescriptor(ticket_.ticket(), &fd)) {
      auto message = stringf("Couldn't convert ticket %o to a flight descriptor", ticket_.ticket());
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }

    std::unique_ptr<arrow::flight::SchemaResult> schemaResult;
    auto gsResult = server_->flightClient()->GetSchema(options, fd, &schemaResult);
    okOrThrow(DEEPHAVEN_EXPR_MSG(gsResult));

    std::shared_ptr<arrow::Schema> arrowSchema;
    auto schemaRes = schemaResult->GetSchema(nullptr, &arrowSchema);
    okOrThrow(DEEPHAVEN_EXPR_MSG(schemaRes));

    auto names = makeReservedVector<std::string>(arrowSchema->fields().size());
    auto types = makeReservedVector<ElementTypeId::Enum>(arrowSchema->fields().size());
    for (const auto &f : arrowSchema->fields()) {
      ArrowToElementTypeId v;
      okOrThrow(DEEPHAVEN_EXPR_MSG(f->type()->Accept(&v)));
      names.push_back(f->name());
      types.push_back(v.typeId_);
    }
    auto schema = Schema::create(std::move(names), std::move(types));
    schemaPromise_.setValue(std::move(schema));
  }

  std::shared_ptr<Server> server_;
  std::shared_ptr<Executor> flightExecutor_;
  CBPromise<std::shared_ptr<Schema>> schemaPromise_;
  Ticket ticket_;
};

LazyState::LazyState(std::shared_ptr<Server> server, std::shared_ptr<Executor> flightExecutor,
    CBFuture<Ticket> ticketFuture) : server_(std::move(server)),
    flightExecutor_(std::move(flightExecutor)), ticketFuture_(std::move(ticketFuture)),
    requestSent_(false), schemaFuture_(schemaPromise_.makeFuture()) {}

LazyState::~LazyState() = default;

std::shared_ptr<Schema> LazyState::getSchema() {
  // Shortcut if we have column definitions
  if (schemaFuture_.valid()) {
    // value or exception
    return schemaFuture_.value();
  }

  auto [cb, fut] = SFCallback<std::shared_ptr<Schema>>::createForFuture();
  getSchemaAsync(std::move(cb));
  auto resultTuple = fut.get();
  return std::get<0>(resultTuple);
}

void LazyState::getSchemaAsync(
    std::shared_ptr<SFCallback<std::shared_ptr<Schema>>> cb) {
  schemaFuture_.invoke(std::move(cb));

  if (requestSent_.test_and_set()) {
    return;
  }

  auto cdCallback = std::make_shared<GetSchemaCallback>(server_, flightExecutor_, std::move(schemaPromise_));
  ticketFuture_.invoke(std::move(cdCallback));
}
}  // namespace internal
}  // namespace impl
}  // namespace deephaven::client
