/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/table_handle_impl.h"

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include "deephaven/client/impl/boolean_expression_impl.h"
#include "deephaven/client/impl/columns_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/impl/update_by_operation_impl.h"
#include "deephaven/client/client.h"
#include "deephaven/client/columns.h"
#include "deephaven/client/subscription/subscribe_thread.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/utility/callbacks.h"
#include "deephaven/dhcore/utility/utility.h"

using io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
using io::deephaven::proto::backplane::grpc::ReleaseResponse;
using io::deephaven::proto::backplane::grpc::SortDescriptor;
using io::deephaven::proto::backplane::grpc::TableReference;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse;
using deephaven::client::SortDirection;
using deephaven::client::impl::ColumnImpl;
using deephaven::client::impl::DateTimeColImpl;
using deephaven::client::impl::NumColImpl;
using deephaven::client::impl::StrColImpl;
using deephaven::client::server::Server;
using deephaven::client::subscription::SubscriptionThread;
using deephaven::client::subscription::SubscriptionHandle;
using deephaven::client::utility::ConvertTicketToFlightDescriptor;
using deephaven::client::utility::Executor;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::ValueOrThrow;
using deephaven::dhcore::chunk::ChunkMaker;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::column::MutableColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;
using deephaven::dhcore::container::RowSequenceIterator;
using deephaven::dhcore::ElementTypeId;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::ticking::TickingCallback;
using deephaven::dhcore::ticking::TickingUpdate;
using deephaven::dhcore::utility::Callback;
using deephaven::dhcore::utility::CBPromise;
using deephaven::dhcore::utility::GetWhat;
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::SFCallback;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;

using UpdateByOperationProto = io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation;

namespace deephaven::client::impl {
std::pair<std::shared_ptr<internal::ExportedTableCreationCallback>, std::shared_ptr<internal::LazyState>>
TableHandleImpl::CreateEtcCallback(std::shared_ptr<TableHandleImpl> dependency, const TableHandleManagerImpl *thm,
    TicketType result_ticket) {
  CBPromise<internal::LazyStateInfo> info_promise;
  auto info_future = info_promise.MakeFuture();
  auto cb = std::make_shared<internal::ExportedTableCreationCallback>(std::move(dependency), result_ticket,
      std::move(info_promise));
  auto ls = std::make_shared<internal::LazyState>(thm->Server(), thm->FlightExecutor(), std::move(info_future),
      std::move(result_ticket));
  return std::make_pair(std::move(cb), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Create(std::shared_ptr<TableHandleManagerImpl> thm, TicketType ticket,
    std::shared_ptr<internal::LazyState> lazy_state) {
  return std::make_shared<TableHandleImpl>(Private(), std::move(thm), std::move(ticket), std::move(lazy_state));
}

TableHandleImpl::TableHandleImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&thm, TicketType &&ticket,
    std::shared_ptr<internal::LazyState> &&lazy_state) : managerImpl_(std::move(thm)), ticket_(std::move(ticket)),
                                                         lazyState_(std::move(lazy_state)) {
}

TableHandleImpl::~TableHandleImpl() {
  this->lazyState_->ReleaseAsync();
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Select(std::vector<std::string> column_specs) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->SelectAsync(ticket_, std::move(column_specs), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Update(std::vector<std::string> column_specs) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->UpdateAsync(ticket_, std::move(column_specs), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::View(std::vector<std::string> column_specs) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->ViewAsync(ticket_, std::move(column_specs), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::DropColumns(std::vector<std::string> column_specs) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->DropColumnsAsync(ticket_, std::move(column_specs), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::UpdateView(std::vector<std::string> column_specs) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->UpdateViewAsync(ticket_, std::move(column_specs), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Where(std::string condition) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->WhereAsync(ticket_, std::move(condition), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Sort(std::vector<SortPair> sort_pairs) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  std::vector<SortDescriptor> sort_descriptors;
  sort_descriptors.reserve(sort_pairs.size());

  for (auto &sp: sort_pairs) {
    auto which = sp.Direction() == SortDirection::kAscending ?
        SortDescriptor::ASCENDING : SortDescriptor::DESCENDING;
    SortDescriptor sd;
    sd.set_column_name(std::move(sp.Column()));
    sd.set_is_absolute(sp.Abs());
    sd.set_direction(which);
    sort_descriptors.push_back(std::move(sd));
  }
  server->SortAsync(ticket_, std::move(sort_descriptors), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Preemptive(int32_t sample_interval_ms) {
  throw std::runtime_error("TODO: kosak");
//  auto itdCallback = TableHandleImpl::createItdCallback(scope_->lowLevelSession()->executor());
//  auto resultHandle = scope_->lowLevelSession()->preemptiveAsync(tableHandle_, sampleIntervalMs,
//      itdCallback);
//  return TableHandleImpl::create(scope_, std::move(resultHandle), std::move(itdCallback));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::DefaultAggregateByDescriptor(
    ComboAggregateRequest::Aggregate descriptor, std::vector<std::string> group_by_columns) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  std::vector<ComboAggregateRequest::Aggregate> descriptors;
  descriptors.reserve(1);
  descriptors.push_back(std::move(descriptor));

  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->ComboAggregateDescriptorAsync(ticket_, std::move(descriptors), std::move(group_by_columns),
      false, std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::DefaultAggregateByType(
    ComboAggregateRequest::AggType aggregate_type, std::vector<std::string> group_by_columns) {
  ComboAggregateRequest::Aggregate descriptor;
  descriptor.set_type(aggregate_type);
  return DefaultAggregateByDescriptor(std::move(descriptor), std::move(group_by_columns));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::By(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::GROUP, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::By(
    std::vector<ComboAggregateRequest::Aggregate> descriptors,
    std::vector<std::string> group_by_columns) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->ComboAggregateDescriptorAsync(ticket_, std::move(descriptors), std::move(group_by_columns),
      false, std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::MinBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::MIN, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::MaxBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::MAX, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::SumBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::SUM, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::AbsSumBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::ABS_SUM, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::VarBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::VAR, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::StdBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::STD, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::AvgBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::AVG, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::LastBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::LAST, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::FirstBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::FIRST, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::MedianBy(std::vector<std::string> column_specs) {
  return DefaultAggregateByType(ComboAggregateRequest::MEDIAN, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::PercentileBy(double percentile, bool avg_median,
    std::vector<std::string> column_specs) {
  ComboAggregateRequest::Aggregate descriptor;
  descriptor.set_type(ComboAggregateRequest::PERCENTILE);
  descriptor.set_percentile(percentile);
  descriptor.set_avg_median(avg_median);
  return DefaultAggregateByDescriptor(std::move(descriptor), std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::PercentileBy(double percentile,
    std::vector<std::string> column_specs) {
  return PercentileBy(percentile, false, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::CountBy(std::string count_by_column,
    std::vector<std::string> column_specs) {
  ComboAggregateRequest::Aggregate descriptor;
  descriptor.set_type(ComboAggregateRequest::COUNT);
  descriptor.set_column_name(std::move(count_by_column));
  return DefaultAggregateByDescriptor(std::move(descriptor), std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::WavgBy(std::string weight_column,
    std::vector<std::string> column_specs) {
  ComboAggregateRequest::Aggregate descriptor;
  descriptor.set_type(ComboAggregateRequest::WEIGHTED_AVG);
  descriptor.set_column_name(std::move(weight_column));
  return DefaultAggregateByDescriptor(std::move(descriptor), std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::TailBy(int64_t n,
    std::vector<std::string> column_specs) {
  return HeadOrTailByHelper(n, false, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::HeadBy(int64_t n,
    std::vector<std::string> column_specs) {
  return HeadOrTailByHelper(n, true, std::move(column_specs));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::HeadOrTailByHelper(int64_t n, bool head,
    std::vector<std::string> column_specs) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->HeadOrTailByAsync(ticket_, head, n, std::move(column_specs), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Tail(int64_t n) {
  return HeadOrTailHelper(false, n);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Head(int64_t n) {
  return HeadOrTailHelper(true, n);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::HeadOrTailHelper(bool head, int64_t n) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->HeadOrTailAsync(ticket_, head, n, std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Ungroup(bool null_fill,
    std::vector<std::string> group_by_columns) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->UngroupAsync(ticket_, null_fill, std::move(group_by_columns), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Merge(std::string key_column,
    std::vector<TicketType> source_tickets) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->MergeAsync(std::move(source_tickets), std::move(key_column), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::CrossJoin(const TableHandleImpl &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->CrossJoinAsync(ticket_, right_side.ticket_, std::move(columns_to_match), std::move(columns_to_add),
      std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::NaturalJoin(const TableHandleImpl &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->NaturalJoinAsync(ticket_, right_side.ticket_, std::move(columns_to_match),
      std::move(columns_to_add), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::ExactJoin(const TableHandleImpl &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->ExactJoinAsync(ticket_, right_side.ticket_, std::move(columns_to_match), std::move(columns_to_add),
      std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::AsOfJoin(AsOfJoinTablesRequest::MatchRule match_rule,
    const TableHandleImpl &right_side, std::vector<std::string> columns_to_match,
    std::vector<std::string> columns_to_add) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->AsOfJoinAsync(match_rule, ticket_, right_side.Ticket(), std::move(columns_to_match),
      std::move(columns_to_add), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

std::shared_ptr<TableHandleImpl>
TableHandleImpl::UpdateBy(std::vector<std::shared_ptr<UpdateByOperationImpl>> ops,
    std::vector<std::string> by) {
  auto protos = MakeReservedVector<UpdateByOperationProto>(ops.size());
  for (const auto &op : ops) {
    protos.push_back(op->UpdateByProto());
  }
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto [cb, ls] = TableHandleImpl::CreateEtcCallback(shared_from_this(), managerImpl_.get(), result_ticket);
  server->UpdateByAsync(ticket_, std::move(protos), std::move(by), std::move(cb), result_ticket);
  return TableHandleImpl::Create(managerImpl_, std::move(result_ticket), std::move(ls));
}

namespace {
class CStyleTickingCallback final : public TickingCallback {
public:
  CStyleTickingCallback(TableHandle::onTickCallback_t on_tick, void *on_tick_user_data,
     TableHandle::onErrorCallback_t on_error, void *on_error_user_data) : onTick_(on_tick),
     onTickUserData_(on_tick_user_data), onError_(on_error), onErrorUserData_(on_error_user_data) {}

  void OnTick(TickingUpdate update) final {
    onTick_(std::move(update), onTickUserData_);
  }

  void OnFailure(std::exception_ptr eptr) final {
    auto what = GetWhat(std::move(eptr));
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
TableHandleImpl::Subscribe(TableHandle::onTickCallback_t on_tick, void *on_tick_user_data,
    TableHandle::onErrorCallback_t on_error, void *on_error_user_data) {
  auto cb = std::make_shared<CStyleTickingCallback>(on_tick, on_tick_user_data, on_error, on_error_user_data);
  return Subscribe(std::move(cb));
}

std::shared_ptr<SubscriptionHandle> TableHandleImpl::Subscribe(std::shared_ptr<TickingCallback> callback) {
  // On the flight executor thread, we invoke DoExchange (waiting for a successful response).
  // We wait for that response here. That makes the first part of this call synchronous. If there
  // is an error in the DoExchange invocation, the caller will get an exception here. The
  // remainder of the interaction (namely, the sending of a BarrageSubscriptionRequest and the
  // parsing of all the replies) is done on a newly-created thread dedicated to that job.
  auto schema = lazyState_->GetSchema();
  auto handle = SubscriptionThread::Start(managerImpl_->Server(), managerImpl_->FlightExecutor().get(),
      schema, ticket_, std::move(callback));
  managerImpl_->AddSubscriptionHandle(handle);
  return handle;
}

void TableHandleImpl::Unsubscribe(const std::shared_ptr<SubscriptionHandle> &handle) {
  managerImpl_->RemoveSubscriptionHandle(handle);
  handle->Cancel();
}

std::vector<std::shared_ptr<ColumnImpl>> TableHandleImpl::GetColumnImpls() {
  auto schema = lazyState_->GetSchema();
  std::vector<std::shared_ptr<ColumnImpl>> result;
  result.reserve(schema->Names().size());
  for (const auto &name : schema->Names()) {
    result.push_back(ColumnImpl::Create(name));
  }
  return result;
}

std::shared_ptr<NumColImpl> TableHandleImpl::GetNumColImpl(std::string column_name) {
  LookupHelper(column_name,
      {
          ElementTypeId::kInt8,
          ElementTypeId::kInt16,
          ElementTypeId::kInt32,
          ElementTypeId::kInt64,
          ElementTypeId::kFloat,
          ElementTypeId::kDouble,
      });
  return NumColImpl::Create(std::move(column_name));
}

std::shared_ptr<StrColImpl> TableHandleImpl::GetStrColImpl(std::string column_name) {
  LookupHelper(column_name, {ElementTypeId::kString});
  return StrColImpl::Create(std::move(column_name));
}

std::shared_ptr<DateTimeColImpl> TableHandleImpl::GetDateTimeColImpl(std::string column_name) {
  LookupHelper(column_name, {ElementTypeId::kTimestamp});
  return DateTimeColImpl::Create(std::move(column_name));
}

void TableHandleImpl::LookupHelper(const std::string &column_name,
    std::initializer_list<ElementTypeId::Enum> valid_types) {
  auto schema = lazyState_->GetSchema();
  auto index = *schema->GetColumnIndex(column_name, true);
  auto actual_type = schema->Types()[index];
  for (auto type : valid_types) {
    if (actual_type == type) {
      return;
    }
  }

  auto render = [](std::ostream &s, ElementTypeId::Enum item) {
    // TODO(kosak): render this as a human-readable string.
    s << static_cast<int>(item);
  };
  auto message = Stringf("Column lookup for %o: Expected Arrow type: one of {%o}. Actual type %o",
      column_name, separatedList(valid_types.begin(), valid_types.end(), ", ", render),
      static_cast<int>(actual_type));
  throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
}

void TableHandleImpl::BindToVariableAsync(std::string variable, std::shared_ptr<SFCallback<>> callback) {
  struct CallbackType final : public SFCallback<BindTableToVariableResponse> {
    explicit CallbackType(std::shared_ptr<SFCallback<>> outer_cb) : outerCb_(std::move(outer_cb)) {}

    void OnSuccess(BindTableToVariableResponse /*item*/) final {
      outerCb_->OnSuccess();
    }

    void OnFailure(std::exception_ptr ep) final {
      outerCb_->OnFailure(std::move(ep));
    }

    std::shared_ptr<SFCallback<>> outerCb_;
  };
  if (!managerImpl_->ConsoleId().has_value()) {
    auto eptr = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(
        "Client was created without specifying a script language")));
    callback->OnFailure(std::move(eptr));
    return;
  }
  auto cb = std::make_shared<CallbackType>(std::move(callback));
  managerImpl_->Server()->BindToVariableAsync(*managerImpl_->ConsoleId(), ticket_,
      std::move(variable), std::move(cb));
}

void TableHandleImpl::Observe() {
  lazyState_->WaitUntilReady();
}

int64_t TableHandleImpl::NumRows() const {
  return lazyState_->Info().NumRows();
}

bool TableHandleImpl::IsStatic() const {
  return lazyState_->Info().IsStatic();
}

std::shared_ptr<Schema> TableHandleImpl::Schema() const {
  return lazyState_->GetSchema();
}

namespace internal {
LazyStateInfo::LazyStateInfo(int64_t num_rows, bool is_static) : numRows_(num_rows), isStatic_(is_static) {}
LazyStateInfo::LazyStateInfo(const LazyStateInfo &other) = default;
LazyStateInfo &LazyStateInfo::operator=(const LazyStateInfo &other) = default;
LazyStateInfo::LazyStateInfo(LazyStateInfo &&other) noexcept = default;
LazyStateInfo &LazyStateInfo::operator=(LazyStateInfo &&other) noexcept = default;
LazyStateInfo::~LazyStateInfo() = default;

ExportedTableCreationCallback::ExportedTableCreationCallback(std::shared_ptr<TableHandleImpl> dependency,
    Ticket expected_ticket, CBPromise<LazyStateInfo> info_promise) : dependency_(std::move(dependency)),
                                                                     expectedTicket_(std::move(expected_ticket)), infoPromise_(std::move(info_promise)) {}
ExportedTableCreationCallback::~ExportedTableCreationCallback() = default;

void ExportedTableCreationCallback::OnSuccess(ExportedTableCreationResponse item) {
  if (!item.result_id().has_ticket()) {
    const char *message = "ExportedTableCreationResponse did not contain a ticket";
    auto ep = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(message)));
    OnFailure(std::move(ep));
    return;
  }

  if (item.result_id().ticket().ticket() != expectedTicket_.ticket()) {
    const char *message = "Result ticket was not equal to expected ticket";
    auto ep = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(message)));
    OnFailure(std::move(ep));
    return;
  }

  LazyStateInfo info(item.size(), item.is_static());
  infoPromise_.SetValue(std::move(info));
}

void ExportedTableCreationCallback::OnFailure(std::exception_ptr error) {
  infoPromise_.SetError(std::move(error));
}

void LazyState::WaitUntilReady() {
  (void) infoFuture_.Value();
}

const LazyStateInfo &LazyState::Info() const {
  return infoFuture_.Value();
}

namespace {
struct ArrowToElementTypeId final : public arrow::TypeVisitor {
  arrow::Status Visit(const arrow::Int8Type &/*type*/) final {
    typeId_ = ElementTypeId::kInt8;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &/*type*/) final {
    typeId_ = ElementTypeId::kInt16;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &/*type*/) final {
    typeId_ = ElementTypeId::kInt32;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &/*type*/) final {
    typeId_ = ElementTypeId::kInt64;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &/*type*/) final {
    typeId_ = ElementTypeId::kFloat;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &/*type*/) final {
    typeId_ = ElementTypeId::kDouble;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &/*type*/) final {
    typeId_ = ElementTypeId::kBool;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt16Type &/*type*/) final {
    typeId_ = ElementTypeId::kChar;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &/*type*/) final {
    typeId_ = ElementTypeId::kString;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType &/*type*/) final {
    typeId_ = ElementTypeId::kTimestamp;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListType &/*type*/) final {
    typeId_ = ElementTypeId::kList;
    return arrow::Status::OK();
  }

  ElementTypeId::Enum typeId_ = ElementTypeId::kInt8;  // arbitrary initializer
};

}  // namespace

class GetSchemaCallback final :
    public SFCallback<LazyStateInfo>,
    public Callback<>,
    public std::enable_shared_from_this<GetSchemaCallback> {
public:
  GetSchemaCallback(std::shared_ptr<Server> server, std::shared_ptr<Executor> flight_executor,
     CBPromise<std::shared_ptr<Schema>> schema_promise, Ticket ticket) : server_(std::move(server)),
     flightExecutor_(std::move(flight_executor)), schemaPromise_(std::move(schema_promise)),
     ticket_(std::move(ticket)) {
  }
  ~GetSchemaCallback() final = default;

  void OnFailure(std::exception_ptr ep) final {
    schemaPromise_.SetError(std::move(ep));
  }

  void OnSuccess(LazyStateInfo /*info*/) final {
    flightExecutor_->Invoke(shared_from_this());
  }

  void Invoke() final {
    try {
      InvokeHelper();
    } catch (...) {
      schemaPromise_.SetError(std::current_exception());
    }
  }

  void InvokeHelper() {
    arrow::flight::FlightCallOptions options;
    server_->ForEachHeaderNameAndValue(
        [&options](const std::string &name, const std::string &value) {
          options.headers.emplace_back(name, value);
        }
    );

    auto fd = ConvertTicketToFlightDescriptor(ticket_.ticket());
    std::unique_ptr<arrow::flight::SchemaResult> schema_result;
    auto gs_result = server_->FlightClient()->GetSchema(options, fd, &schema_result);
    OkOrThrow(DEEPHAVEN_EXPR_MSG(gs_result));

    std::shared_ptr<arrow::Schema> arrow_schema;
    auto schema_res = schema_result->GetSchema(nullptr, &arrow_schema);
    OkOrThrow(DEEPHAVEN_EXPR_MSG(schema_res));

    auto names = MakeReservedVector<std::string>(arrow_schema->fields().size());
    auto types = MakeReservedVector<ElementTypeId::Enum>(arrow_schema->fields().size());
    for (const auto &f : arrow_schema->fields()) {
      ArrowToElementTypeId v;
      OkOrThrow(DEEPHAVEN_EXPR_MSG(f->type()->Accept(&v)));
      names.push_back(f->name());
      types.push_back(v.typeId_);
    }
    auto schema = Schema::Create(std::move(names), std::move(types));
    schemaPromise_.SetValue(std::move(schema));
  }

  std::shared_ptr<Server> server_;
  std::shared_ptr<Executor> flightExecutor_;
  CBPromise<std::shared_ptr<Schema>> schemaPromise_;
  Ticket ticket_;
};

LazyState::LazyState(std::shared_ptr<Server> server, std::shared_ptr<Executor> flight_executor,
    CBFuture<LazyStateInfo> info_future, Ticket ticket) : server_(std::move(server)),
      flightExecutor_(std::move(flight_executor)), infoFuture_(std::move(info_future)),
      ticket_(std::move(ticket)), schemaRequestSent_(false), schemaFuture_(schemaPromise_.MakeFuture()) {}

LazyState::~LazyState() = default;

std::shared_ptr<Schema> LazyState::GetSchema() {
  // Shortcut if we have column definitions
  if (schemaFuture_.Valid()) {
    // value or exception
    return schemaFuture_.Value();
  }

  auto [cb, fut] = SFCallback<std::shared_ptr<Schema>>::CreateForFuture();
  GetSchemaAsync(std::move(cb));
  auto result_tuple = fut.get();
  return std::get<0>(result_tuple);
}

void LazyState::GetSchemaAsync(std::shared_ptr<SFCallback<std::shared_ptr<Schema>>> cb) {
  schemaFuture_.Invoke(std::move(cb));

  if (schemaRequestSent_.test_and_set()) {
    return;
  }

  auto cd_callback = std::make_shared<GetSchemaCallback>(server_, flightExecutor_, std::move(schemaPromise_), ticket_);
  infoFuture_.Invoke(std::move(cd_callback));
}

void LazyState::ReleaseAsync() {
  struct DualCallback final : public SFCallback<LazyStateInfo>, public SFCallback<ReleaseResponse>,
      public std::enable_shared_from_this<DualCallback> {
    DualCallback(std::shared_ptr<Server> server, Ticket ticket) : server_(std::move(server)),
        ticket_(std::move(ticket)) {}
    ~DualCallback() final = default;

    void OnSuccess(LazyStateInfo /*info*/) final {
      // Once the ExportedTableCreationResponse has come back, then we can issue an Release, using ourself
      // as a callback object again.
      server_->ReleaseAsync(ticket_, shared_from_this());
    }

    void OnSuccess(ReleaseResponse resp) final {
      // Do nothing
    }

    void OnFailure(std::exception_ptr ep) final {
      // Do nothing
    }

    std::shared_ptr<Server> server_;
    Ticket ticket_;
  };

  auto cb = std::make_shared<DualCallback>(server_, ticket_);
  infoFuture_.Invoke(std::move(cb));
}
}  // namespace internal
}  // namespace deephaven::client::impl

