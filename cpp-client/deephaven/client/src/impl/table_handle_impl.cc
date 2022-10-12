/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/table_handle_impl.h"

#include <map>
#include <set>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include "deephaven/client/arrowutil/arrow_util.h"
#include "deephaven/client/impl/boolean_expression_impl.h"
#include "deephaven/client/impl/columns_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/client.h"
#include "deephaven/client/columns.h"
#include "deephaven/client/chunk/chunk_filler.h"
#include "deephaven/client/chunk/chunk_maker.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/table/table.h"
#include "deephaven/client/ticking.h"
#include "deephaven/client/subscription/subscribe_thread.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/utility/misc.h"
#include "deephaven/client/utility/utility.h"
#include "deephaven/flatbuf/Barrage_generated.h"

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
using deephaven::client::chunk::ChunkFiller;
using deephaven::client::chunk::ChunkMaker;
using deephaven::client::column::ColumnSource;
using deephaven::client::column::MutableColumnSource;
using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;
using deephaven::client::container::RowSequenceIterator;
using deephaven::client::server::Server;
using deephaven::client::table::Table;
using deephaven::client::subscription::startSubscribeThread;
using deephaven::client::subscription::SubscriptionHandle;
using deephaven::client::utility::Callback;
using deephaven::client::utility::CBPromise;
using deephaven::client::utility::ColumnDefinitions;
using deephaven::client::utility::Executor;
using deephaven::client::utility::makeReservedVector;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::separatedList;
using deephaven::client::utility::SFCallback;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::valueOrThrow;

using io::deephaven::barrage::flatbuf::BarrageMessageType;
using io::deephaven::barrage::flatbuf::BarrageModColumnMetadata;
using io::deephaven::barrage::flatbuf::BarrageUpdateMetadata;
using io::deephaven::barrage::flatbuf::ColumnConversionMode;
using io::deephaven::barrage::flatbuf::CreateBarrageMessageWrapper;
using io::deephaven::barrage::flatbuf::CreateBarrageSubscriptionOptions;
using io::deephaven::barrage::flatbuf::CreateBarrageSubscriptionRequest;

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
  auto colDefs = lazyState_->getColumnDefinitions();
  auto handle = startSubscribeThread(managerImpl_->server(), managerImpl_->flightExecutor().get(),
      colDefs, ticket_, std::move(callback));

  subscriptions_.insert(handle);
  return handle;
}

void TableHandleImpl::unsubscribe(std::shared_ptr<SubscriptionHandle> handle) {
  auto node = subscriptions_.extract(handle);
  if (node.empty()) {
    return;
  }
  node.value()->cancel();
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
    auto message = stringf(R"(Column name "%o" is not in the table)", columnName);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
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
  throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
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

class GetColumnDefsCallback final :
    public SFCallback<Ticket>,
    public Callback<>,
    public std::enable_shared_from_this<GetColumnDefsCallback> {
public:
  GetColumnDefsCallback(std::shared_ptr<Server> server, std::shared_ptr<Executor> flightExecutor,
     CBPromise<std::shared_ptr<ColumnDefinitions>> colDefsPromise) : server_(std::move(server)),
     flightExecutor_(std::move(flightExecutor)), colDefsPromise_(std::move(colDefsPromise)) {}
  ~GetColumnDefsCallback() final = default;

  void onFailure(std::exception_ptr ep) final {
    colDefsPromise_.setError(std::move(ep));
  }

  void onSuccess(Ticket ticket) final {
    ticket_ = std::move(ticket);
    flightExecutor_->invoke(shared_from_this());
  }

  void invoke() final {
    try {
      invokeHelper();
    } catch (...) {
      colDefsPromise_.setError(std::current_exception());
    }
  }

  void invokeHelper() {
    arrow::flight::FlightCallOptions options;
    options.headers.push_back(server_->makeBlessing());

    arrow::flight::FlightDescriptor fd;
    if (!ArrowUtil::tryConvertTicketToFlightDescriptor(ticket_.ticket(), &fd)) {
      auto message = stringf("Couldn't convert ticket %o to a flight descriptor", ticket_.ticket());
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }

    std::unique_ptr<arrow::flight::SchemaResult> schemaResult;
    auto gsResult = server_->flightClient()->GetSchema(options, fd, &schemaResult);
    okOrThrow(DEEPHAVEN_EXPR_MSG(gsResult));

    std::shared_ptr<arrow::Schema> schema;
    auto schemaRes = schemaResult->GetSchema(nullptr, &schema);
    okOrThrow(DEEPHAVEN_EXPR_MSG(schemaRes));

    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> colVec;
    std::map<std::string, std::shared_ptr<arrow::DataType>> colMap;

    for (const auto &f : schema->fields()) {
      colVec.emplace_back(f->name(), f->type());
      colMap[f->name()] = f->type();
    }
    std::shared_ptr<ColumnDefinitions> colDefs(new ColumnDefinitions(std::move(colVec),
        std::move(colMap)));
    colDefsPromise_.setValue(std::move(colDefs));
  }

  std::shared_ptr<Server> server_;
  std::shared_ptr<Executor> flightExecutor_;
  CBPromise<std::shared_ptr<ColumnDefinitions>> colDefsPromise_;
  Ticket ticket_;
};

LazyState::LazyState(std::shared_ptr<Server> server, std::shared_ptr<Executor> flightExecutor,
    CBFuture<Ticket> ticketFuture) : server_(std::move(server)),
    flightExecutor_(std::move(flightExecutor)), ticketFuture_(std::move(ticketFuture)),
    requestSent_(false), colDefsFuture_(colDefsPromise_.makeFuture()) {}

LazyState::~LazyState() = default;

std::shared_ptr<ColumnDefinitions> LazyState::getColumnDefinitions() {
  // Shortcut if we have column definitions
  if (colDefsFuture_.valid()) {
    // value or exception
    return colDefsFuture_.value();
  }

  auto [cb, fut] = SFCallback<std::shared_ptr<ColumnDefinitions>>::createForFuture();
  getColumnDefinitionsAsync(std::move(cb));
  auto resultTuple = fut.get();
  return std::get<0>(resultTuple);
}

void LazyState::getColumnDefinitionsAsync(
    std::shared_ptr<SFCallback<std::shared_ptr<ColumnDefinitions>>> cb) {
  colDefsFuture_.invoke(std::move(cb));

  if (requestSent_.test_and_set()) {
    return;
  }

  auto cdCallback = std::make_shared<GetColumnDefsCallback>(server_, flightExecutor_,
      std::move(colDefsPromise_));
  ticketFuture_.invoke(std::move(cdCallback));
}
}  // namespace internal
}  // namespace impl
}  // namespace deephaven::client
