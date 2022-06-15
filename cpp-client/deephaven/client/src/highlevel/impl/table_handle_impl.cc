/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
#include "deephaven/client/highlevel/sad/chunk_filler.h"
#include "deephaven/client/highlevel/sad/chunk_maker.h"
#include "deephaven/client/highlevel/sad/sad_context.h"
#include "deephaven/client/highlevel/sad/sad_row_sequence.h"
#include "deephaven/client/highlevel/sad/sad_table.h"
#include "deephaven/client/highlevel/sad/sad_ticking_table.h"
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/utility/utility.h"
#include "deephaven/flatbuf/Barrage_generated.h"

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
using deephaven::client::highlevel::sad::ChunkFiller;
using deephaven::client::highlevel::sad::ChunkMaker;
using deephaven::client::highlevel::sad::SadColumnSource;
using deephaven::client::highlevel::sad::SadContext;
using deephaven::client::highlevel::sad::SadIntArrayColumnSource;
using deephaven::client::highlevel::sad::SadLongArrayColumnSource;
using deephaven::client::highlevel::sad::SadDoubleArrayColumnSource;
using deephaven::client::highlevel::sad::SadMutableColumnSource;
using deephaven::client::highlevel::sad::SadRowSequence;
using deephaven::client::highlevel::sad::SadRowSequenceBuilder;
using deephaven::client::highlevel::sad::SadTable;
using deephaven::client::highlevel::sad::SadTickingTable;
using deephaven::client::utility::Callback;
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
using io::deephaven::barrage::flatbuf::GetBarrageMessageWrapper;

namespace deephaven::client::highlevel {
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

namespace {
class ThreadNubbin;
class SubscribeNubbin final : public Callback<> {
  typedef deephaven::client::lowlevel::Server Server;

public:
  SubscribeNubbin(std::shared_ptr<Server> server, std::vector<int8_t> ticketBytes,
      std::shared_ptr<internal::ColumnDefinitions> colDefs,
      std::promise<void> promise, std::shared_ptr<TickingCallback> callback);
  void invoke() final;

  static std::shared_ptr<ThreadNubbin> sadClown_;

private:
  void invokeHelper();

  std::shared_ptr<Server> server_;
  std::vector<int8_t> ticketBytes_;
  std::shared_ptr<internal::ColumnDefinitions> colDefs_;
  std::promise<void> promise_;
  std::shared_ptr<TickingCallback> callback_;
};

// A simple extension to arrow::Buffer that owns its DetachedBuffer storage
class ZamboniBuffer : public arrow::Buffer {
public:
  explicit ZamboniBuffer(flatbuffers::DetachedBuffer buffer);

private:
  flatbuffers::DetachedBuffer buffer_;
};

class ThreadNubbin {
public:
  ThreadNubbin(std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
      std::shared_ptr<internal::ColumnDefinitions> colDefs,
      std::shared_ptr<TickingCallback> callback);

  static void runForever(const std::shared_ptr<ThreadNubbin> &self);

private:
  void runForeverHelper();
  void runForeverHelperImpl();

public:
  std::unique_ptr<arrow::flight::FlightStreamReader> fsr_;
  std::shared_ptr<internal::ColumnDefinitions> colDefs_;
  std::shared_ptr<TickingCallback> callback_;
};

struct Constants {
  static constexpr const int8_t SHORT_VALUE = 1;
  static constexpr const int8_t INT_VALUE = 2;
  static constexpr const int8_t LONG_VALUE = 3;
  static constexpr const int8_t BYTE_VALUE = 4;

  static constexpr const int8_t VALUE_MASK = 7;

  static constexpr const int8_t OFFSET = 8;
  static constexpr const int8_t SHORT_ARRAY = 16;
  static constexpr const int8_t BYTE_ARRAY = 24;
  static constexpr const int8_t END = 32;
  static constexpr const int8_t CMD_MASK = 0x78;
};

class DataInput {
public:
  DataInput(const void *start, size_t size) : data_(static_cast<const char*>(start)), size_(size) {}
  int64_t readLong();
  int32_t readInt();
  int16_t readShort();
  int8_t readByte();

private:
  const char *data_ = nullptr;
  size_t size_ = 0;
};

std::shared_ptr<SadMutableColumnSource> makeColumnSource(const arrow::DataType &dataType);
int64_t readValue(DataInput *in, int command);

std::shared_ptr<SadRowSequence> readExternalCompressedDelta(DataInput *in);
}  // namespace

namespace {
SubscribeNubbin::SubscribeNubbin(std::shared_ptr<Server> server, std::vector<int8_t> ticketBytes,
    std::shared_ptr<internal::ColumnDefinitions> colDefs, std::promise<void> promise,
    std::shared_ptr<TickingCallback> callback) :
    server_(std::move(server)), ticketBytes_(std::move(ticketBytes)), colDefs_(std::move(colDefs)),
        promise_(std::move(promise)), callback_(std::move(callback)) {}

ZamboniBuffer::ZamboniBuffer(flatbuffers::DetachedBuffer buffer) :
    arrow::Buffer(buffer.data(), int64_t(buffer.size())), buffer_(std::move(buffer)) {}

void SubscribeNubbin::invoke() {
  try {
    invokeHelper();
    // If you made it this far, then you have been successful!
    promise_.set_value();
  } catch (const std::exception &e) {
    promise_.set_exception(std::make_exception_ptr(e));
  }
}

constexpr const uint32_t deephavenMagicNumber = 0x6E687064U;

std::shared_ptr<ThreadNubbin> SubscribeNubbin::sadClown_;

void SubscribeNubbin::invokeHelper() {
  arrow::flight::FlightCallOptions fco;
  fco.headers.push_back(server_->makeBlessing());
  auto *client = server_->flightClient();

  arrow::flight::FlightDescriptor dummy;
  char magicData[4];
  uint32_t src = deephavenMagicNumber;
  memcpy(magicData, &src, sizeof(magicData));

  dummy.type = arrow::flight::FlightDescriptor::DescriptorType::CMD;
  dummy.cmd = std::string(magicData, 4);
  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightStreamReader> fsr;
  okOrThrow(DEEPHAVEN_EXPR_MSG(client->DoExchange(fco, dummy, &fsw, &fsr)));

  // Make a BarrageMessageWrapper
  // ...Whose payload is a BarrageSubscriptionRequest
  // ......which has BarrageSubscriptionOptions

  flatbuffers::FlatBufferBuilder payloadBuilder(4096);

  auto subOptions = CreateBarrageSubscriptionOptions(payloadBuilder,
      ColumnConversionMode::ColumnConversionMode_Stringify, true, 0, 4096);

  auto ticket = payloadBuilder.CreateVector(ticketBytes_);
  auto subreq = CreateBarrageSubscriptionRequest(payloadBuilder, ticket, {}, {}, subOptions);
  payloadBuilder.Finish(subreq);
  // TODO: fix sad cast
  const auto *payloadp = (int8_t*)payloadBuilder.GetBufferPointer();
  const auto payloadSize = payloadBuilder.GetSize();

  // TODO: I'd really like to just point this buffer backwards to the thing I just created, rather
  // then copying it. But, eh, version 2.
  flatbuffers::FlatBufferBuilder wrapperBuilder(4096);
  auto payload = wrapperBuilder.CreateVector(payloadp, payloadSize);
  auto messageWrapper = CreateBarrageMessageWrapper(wrapperBuilder, deephavenMagicNumber,
      BarrageMessageType::BarrageMessageType_BarrageSubscriptionRequest, payload);
  wrapperBuilder.Finish(messageWrapper);
  auto wrapperBuffer = wrapperBuilder.Release();

  auto buffer = std::make_shared<ZamboniBuffer>(std::move(wrapperBuffer));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteMetadata(std::move(buffer))));

  auto threadNubbin = std::make_shared<ThreadNubbin>(std::move(fsr),
      std::move(colDefs_), std::move(callback_));
  sadClown_ = threadNubbin;
  std::thread t(&ThreadNubbin::runForever, std::move(threadNubbin));
  t.detach();
}

ThreadNubbin::ThreadNubbin(std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<internal::ColumnDefinitions> colDefs, std::shared_ptr<TickingCallback> callback) :
    fsr_(std::move(fsr)), colDefs_(std::move(colDefs)), callback_(std::move(callback)) {}

void ThreadNubbin::runForever(const std::shared_ptr<ThreadNubbin> &self) {
  std::cerr << "Hi, ThreadNubbin is awake\n";
  try {
    self->runForeverHelper();
  } catch (const std::exception &e) {
    streamf(std::cerr, "Caught exception in main table handle loop: %o\n", e.what());
    self->callback_->onFailure(std::make_exception_ptr(e));
  }
  std::cerr << "ThreadNubbin is exiting. Bye.\n";
}

void ThreadNubbin::runForeverHelper() {
    std::exception_ptr eptr;
    try {
        runForeverHelperImpl();
    }
    catch (...) {
        eptr = std::current_exception();
    }
    callback_->onFailure(eptr);
}

void ThreadNubbin::runForeverHelperImpl() {
  const auto &vec = colDefs_->vec();
  // Create some MutableColumnSources and keep two views on them: a Mutable view which we
  // will keep around locally in order to effect changes, and a readonly view used to make the
  // table.
  std::vector<std::shared_ptr<SadMutableColumnSource>> mutableColumns;
  std::vector<std::shared_ptr<SadColumnSource>> tableColumns;
  mutableColumns.reserve(vec.size());
  tableColumns.reserve(vec.size());
  for (const auto &entry : vec) {
    auto cs = makeColumnSource(*entry.second);
    tableColumns.push_back(cs);
    mutableColumns.push_back(std::move(cs));
  }

  auto sadTable = SadTickingTable::create(std::move(tableColumns));
  arrow::flight::FlightStreamChunk flightStreamChunk;
  while (true) {
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr_->Next(&flightStreamChunk)));
    // streamf(std::cerr, "GOT A NUBBIN\n%o\n", chunk.data->ToString());
    if (flightStreamChunk.app_metadata == nullptr) {
      std::cerr << "TODO(kosak) -- chunk.app_metdata == nullptr\n";
      continue;
    }

    const auto *q = flightStreamChunk.app_metadata->data();
    const auto *zamboni = GetBarrageMessageWrapper(q);
    if (zamboni->magic() != deephavenMagicNumber) {
      continue;
    }
    if (zamboni->msg_type() != BarrageMessageType::BarrageMessageType_BarrageUpdateMetadata) {
      continue;
    }

    auto rowSequence = sadTable->getRowSequence();
    streamf(std::cerr, "My rowseq is currently %o\n", rowSequence);

    const auto *pp = zamboni->msg_payload()->data();
    auto *nubbinp = flatbuffers::GetRoot<BarrageUpdateMetadata>(pp);
    streamf(std::cerr, "num_add_batches=%o, num_mod_batches=%o\n", nubbinp->num_add_batches(), nubbinp->num_mod_batches());

    DataInput diAdded(nubbinp->added_rows()->data(), nubbinp->added_rows()->size());
    auto addedRows = readExternalCompressedDelta(&diAdded);
    DataInput diRemoved(nubbinp->removed_rows()->data(), nubbinp->removed_rows()->size());
    auto removedRows = readExternalCompressedDelta(&diRemoved);

    DataInput di(nubbinp->shift_data()->data(), nubbinp->shift_data()->size());
    auto startIndex = readExternalCompressedDelta(&di);
    auto endIndex = readExternalCompressedDelta(&di);
    auto destIndex = readExternalCompressedDelta(&di);

    streamf(std::cerr, "RemovedRows: {%o}\n", *removedRows);
    streamf(std::cerr, "AddedRows: {%o}\n", *addedRows);
    streamf(std::cerr, "shift start: {%o}\n", *startIndex);
    streamf(std::cerr, "shift end: {%o}\n", *endIndex);
    streamf(std::cerr, "shift dest: {%o}\n", *destIndex);

    // removes
    // shifts
    // adds
    // modifies

    if (!addedRows->empty()) {
      streamf(std::cerr, "There was some new data: %o rows, %o columns:\n",
          flightStreamChunk.data->num_rows(), flightStreamChunk.data->num_columns());
      const auto &srcCols = flightStreamChunk.data->columns();
      for (size_t colNum = 0; colNum < srcCols.size(); ++colNum) {
        streamf(std::cerr, "Column %o\n", colNum);
        const auto &srcCol = srcCols[colNum];
        for (uint32_t ii = 0; ii < addedRows->size(); ++ii) {
          const auto &res = srcCol->GetScalar(ii);
          streamf(std::cerr, "%o: %o\n", ii, res.ValueOrDie()->ToString());
        }
      }
    }

    // 1. Removes
    sadTable->erase(*removedRows);

    // 2. Shifts
    if (nubbinp->shift_data()->size() != 0) {
      sadTable->shift(*startIndex, *endIndex, *destIndex);
    }

    // 3. Adds
    {
      auto unwrappedTable = sadTable->add(*addedRows);
      auto rowKeys = unwrappedTable->getUnorderedRowKeys();

      const auto &srcCols = flightStreamChunk.data->columns();
      auto ncols = srcCols.size();
      if (ncols != sadTable->numColumns()) {
        auto message = stringf("Received %o columns, but my table has %o columns", ncols,
            sadTable->numColumns());
        throw std::runtime_error(message);
      }

      auto numRows = unwrappedTable->numRows();
      auto sequentialRows = SadRowSequence::createSequential(0, (int64_t)numRows);

      for (size_t i = 0; i < ncols; ++i) {
        const auto &srcColArrow = *srcCols[i];

        auto &destColDh = mutableColumns[i];
        auto context = destColDh->createContext(numRows);
        auto chunk = ChunkMaker::createChunkFor(*destColDh, numRows);

        ChunkFiller::fillChunk(srcColArrow, *sequentialRows, chunk.get());
        destColDh->fillFromChunkUnordered(context.get(), *chunk, *rowKeys, numRows);
      }
    }

    // 4, Modifies
    const auto &mcms = *nubbinp->mod_column_nodes();
    for (size_t i = 0; i < mcms.size(); ++i) {
      const auto &elt = mcms.Get(i);
      DataInput diModified(elt->modified_rows()->data(), elt->modified_rows()->size());
      auto modIndex = readExternalCompressedDelta(&diModified);

      if (modIndex->empty()) {
        continue;
      }

      auto zamboniRows = modIndex->size();

      const auto &srcCols = flightStreamChunk.data->columns();
      const auto &srcColArrow = *srcCols[i];

      auto &destColDh = mutableColumns[i];
      auto context = destColDh->createContext(zamboniRows);
      auto chunk = ChunkMaker::createChunkFor(*destColDh, zamboniRows);

      auto sequentialRows = SadRowSequence::createSequential(0, (int64_t)zamboniRows);

      ChunkFiller::fillChunk(srcColArrow, *sequentialRows, chunk.get());
      destColDh->fillFromChunk(context.get(), *chunk, *modIndex);
    }

    rowSequence = sadTable->getRowSequence();
    streamf(std::cerr, "Now my index looks like this: [%o]\n", *rowSequence);

    // TODO(kosak): Do something about the sharing story for SadTable
    callback_->onTick(sadTable);
  }
}
}  // namespace

namespace {
std::shared_ptr<SadRowSequence> readExternalCompressedDelta(DataInput *in) {
  SadRowSequenceBuilder builder;

  int64_t offset = 0;

  int64_t pending = -1;
  auto consume = [&pending, &builder](int64_t v) {
    auto s = pending;
    if (s == -1) {
      pending = v;
    } else if (v < 0) {
      builder.addRange(s, -v);
      pending = -1;
    } else {
      builder.add(s);
      pending = v;
    }
  };

  while (true) {
    int64_t actualValue;
    int command = in->readByte();

    switch (command & Constants::CMD_MASK) {
      case Constants::OFFSET: {
        int64_t value = readValue(in, command);
        actualValue = offset + (value < 0 ? -value : value);
        consume(value < 0 ? -actualValue : actualValue);
        offset = actualValue;
        break;
      }

      case Constants::SHORT_ARRAY: {
        int shortCount = (int) readValue(in, command);
        for (int ii = 0; ii < shortCount; ++ii) {
          int16_t shortValue = in->readShort();
          actualValue = offset + (shortValue < 0 ? -shortValue : shortValue);
          consume(shortValue < 0 ? -actualValue : actualValue);
          offset = actualValue;
        }
        break;
      }

      case Constants::BYTE_ARRAY: {
        int byteCount = (int) readValue(in, command);
        for (int ii = 0; ii < byteCount; ++ii) {
          int8_t byteValue = in->readByte();
          actualValue = offset + (byteValue < 0 ? -byteValue : byteValue);
          consume(byteValue < 0 ? -actualValue : actualValue);
          offset = actualValue;
        }
        break;
      }

      case Constants::END: {
        if (pending >= 0) {
          builder.add(pending);
        }
        return builder.build();
      }

      default:
        throw std::runtime_error(stringf("Bad command: %o", command));
    }
  }
}

struct MyVisitor final : public arrow::TypeVisitor {
  arrow::Status Visit(const arrow::Int32Type &type) final {
    result_ = SadIntArrayColumnSource::create();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &type) final {
    result_ = SadLongArrayColumnSource::create();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &type) final {
    result_ = SadDoubleArrayColumnSource::create();
    return arrow::Status::OK();
  }

  std::shared_ptr<SadMutableColumnSource> result_;
};

std::shared_ptr<SadMutableColumnSource> makeColumnSource(const arrow::DataType &dataType) {
  MyVisitor v;
  okOrThrow(DEEPHAVEN_EXPR_MSG(dataType.Accept(&v)));
  return std::move(v.result_);
}

int64_t readValue(DataInput *in, int command) {
  switch (command & Constants::VALUE_MASK) {
    case Constants::LONG_VALUE: {
      return in->readLong();
    }
    case Constants::INT_VALUE: {
      return in->readInt();
    }
    case Constants::SHORT_VALUE: {
      return in->readShort();
    }
    case Constants::BYTE_VALUE: {
      return in->readByte();
    }
    default: {
      throw std::runtime_error(stringf("Bad command: %o", command));
    }
  }
}

int8_t DataInput::readByte() {
  int8_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int16_t DataInput::readShort() {
  int16_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int32_t DataInput::readInt() {
  int32_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int64_t DataInput::readLong() {
  int64_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}
} // namespace

void TableHandleImpl::subscribe(std::shared_ptr<TickingCallback> callback) {
  // On the flight executor thread, we invoke DoExchange (waiting for a successful response).
  // We wait for that response here. That makes the first part of this call synchronous. If there
  // is an error in the DoExchange invocation, the caller will get an exception here. The
  // remainder of the interaction (namely, the sending of a BarrageSubscriptionRequest and the
  // parsing of all the replies) is done on a newly-created thread dedicated to that job.

  auto coldefs = lazyState_->getColumnDefinitions();
  std::vector<int8_t> ticketBytes(ticket_.ticket().begin(), ticket_.ticket().end());

  std::promise<void> promise;
  auto future = promise.get_future();
  auto innerCb = std::make_shared<SubscribeNubbin>(managerImpl_->server(), std::move(ticketBytes),
      std::move(coldefs), std::move(promise), std::move(callback));
  managerImpl_->flightExecutor()->invoke(std::move(innerCb));
  future.wait();
}

void TableHandleImpl::unsubscribe(std::shared_ptr<TickingCallback> callback) {
  std::cerr << "TODO(kosak) -- unsubscribe\n";
  std::cerr << "I'm kind of worried about this\n";
  SubscribeNubbin::sadClown_->fsr_->Cancel();
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
}  // namespace deephaven::client::highlevel
