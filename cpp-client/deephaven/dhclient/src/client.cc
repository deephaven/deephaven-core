/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/client.h"

#include <stdexcept>

#include <grpc/support/log.h>

#include <arrow/array.h>
#include <arrow/scalar.h>
#include "deephaven/client/arrowutil/arrow_client_table.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/impl/aggregate_impl.h"
#include "deephaven/client/impl/client_impl.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/impl/update_by_operation_impl.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/utility/utility.h"

using io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
using io::deephaven::proto::backplane::grpc::Ticket;
using deephaven::client::arrowutil::ArrowClientTable;
using deephaven::client::impl::AggregateComboImpl;
using deephaven::client::impl::AggregateImpl;
using deephaven::client::impl::ClientImpl;
using deephaven::client::impl::UpdateByOperationImpl;
using deephaven::client::server::Server;
using deephaven::client::subscription::SubscriptionHandle;
using deephaven::client::utility::Executor;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::ValueOrThrow;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::utility::GetWhat;
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::SimpleOstringstream;


namespace deephaven::client {
namespace {
void PrintTableData(std::ostream &s, const TableHandle &table_handle, bool want_headers);
void CheckNotClosedOrThrow(const std::shared_ptr<ClientImpl> &impl);
}  // namespace

Client Client::Connect(const std::string &target, const ClientOptions &options) {
  auto server = Server::CreateFromTarget(target, options);
  auto executor = Executor::Create("Client executor for " + server->me());
  auto flight_executor = Executor::Create("Flight executor for " + server->me());
  void *const server_for_logging = server.get();
  auto impl = ClientImpl::Create(std::move(server), executor, flight_executor, options.sessionType_);
  gpr_log(GPR_INFO,
      "Client target=%s created ClientImpl(%p), Server(%p).",
      target.c_str(),
      static_cast<void*>(impl.get()),
      server_for_logging);
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
  gpr_log(GPR_INFO, "Destructing Client ClientImpl(%p).",
      static_cast<void*>(impl_.get()));
  try {
    Close();
  } catch (...) {
    auto what = GetWhat(std::current_exception());
    gpr_log(GPR_INFO, "Client destructor is ignoring thrown exception: %s", what.c_str());
  }
}

// Tear down Client state.
void Client::Close() {
  gpr_log(GPR_INFO, "Closing Client ClientImpl(%p), before close use_count=%ld.",
      static_cast<void*>(impl_.get()), impl_.use_count());
  // Move to local variable to be defensive.
  auto temp = std::move(impl_);
  if (temp != nullptr) {
    temp->Shutdown();
  }
}

TableHandleManager Client::GetManager() const {
  CheckNotClosedOrThrow(impl_);
  return TableHandleManager(impl_->ManagerImpl());
}

Client::OnCloseCbId Client::AddOnCloseCallback(std::function<void()> cb) {
  CheckNotClosedOrThrow(impl_);
  return impl_->AddOnCloseCallback(std::move(cb));
}

bool Client::RemoveOnCloseCallback(OnCloseCbId cb_id) {
  CheckNotClosedOrThrow(impl_);
  return impl_->RemoveOnCloseCallback(std::move(cb_id));
}

TableHandleManager::TableHandleManager() = default;
TableHandleManager::TableHandleManager(std::shared_ptr<impl::TableHandleManagerImpl> impl) : impl_(std::move(impl)) {}
TableHandleManager::TableHandleManager(TableHandleManager &&other) noexcept = default;
TableHandleManager &TableHandleManager::operator=(TableHandleManager &&other) noexcept = default;
TableHandleManager::~TableHandleManager() = default;

TableHandle TableHandleManager::EmptyTable(int64_t size) const {
  auto qs_impl = impl_->EmptyTable(size);
  return TableHandle(std::move(qs_impl));
}

TableHandle TableHandleManager::FetchTable(std::string table_name) const {
  auto qs_impl = impl_->FetchTable(std::move(table_name));
  return TableHandle(std::move(qs_impl));
}

TableHandle TableHandleManager::TimeTable(DurationSpecifier period, TimePointSpecifier start_time,
    bool blink_table) const {
  auto impl = impl_->TimeTable(std::move(period), std::move(start_time), blink_table);
  return TableHandle(std::move(impl));
}

TableHandle TableHandleManager::InputTable(const TableHandle &initial_table,
    std::vector<std::string> key_columns) const {
  auto th_impl = impl_->InputTable(*initial_table.Impl(), std::move(key_columns));
  // Populate the InputTable with the contents of 'initial_table'
  th_impl->AddTable(*initial_table.Impl());
  return TableHandle(std::move(th_impl));
}

std::string TableHandleManager::NewTicket() const {
  return impl_->NewTicket();
}

TableHandle TableHandleManager::MakeTableHandleFromTicket(std::string ticket) const {
  auto handle_impl = impl_->MakeTableHandleFromTicket(std::move(ticket));
  return TableHandle(std::move(handle_impl));
}

void TableHandleManager::RunScript(std::string code) const {
  impl_->RunScript(std::move(code));
}

namespace {
ComboAggregateRequest::Aggregate
CreateDescForMatchPairs(ComboAggregateRequest::AggType aggregate_type,
    std::vector<std::string> column_specs) {
  ComboAggregateRequest::Aggregate result;
  result.set_type(aggregate_type);
  for (auto &cs : column_specs) {
    result.mutable_match_pairs()->Add(std::move(cs));
  }
  return result;
}

ComboAggregateRequest::Aggregate CreateDescForColumn(ComboAggregateRequest::AggType aggregate_type,
    std::string column_spec) {
  ComboAggregateRequest::Aggregate result;
  result.set_type(aggregate_type);
  result.set_column_name(std::move(column_spec));
  return result;
}

Aggregate createAggForMatchPairs(ComboAggregateRequest::AggType aggregate_type,
    std::vector<std::string> column_specs) {
  auto ad = CreateDescForMatchPairs(aggregate_type, std::move(column_specs));
  auto impl = AggregateImpl::Create(std::move(ad));
  return Aggregate(std::move(impl));
}
}  // namespace

Aggregate::Aggregate() = default;
Aggregate::Aggregate(const Aggregate &other) = default;
Aggregate::Aggregate(Aggregate &&other) noexcept = default;
Aggregate &Aggregate::operator=(const Aggregate &other) = default;
Aggregate &Aggregate::operator=(Aggregate &&other) noexcept = default;
Aggregate::~Aggregate() = default;

Aggregate::Aggregate(std::shared_ptr<impl::AggregateImpl> impl) : impl_(std::move(impl)) {
}

Aggregate Aggregate::AbsSum(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::ABS_SUM, std::move(column_specs));
}

Aggregate Aggregate::Avg(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::AVG, std::move(column_specs));
}

Aggregate Aggregate::Count(std::string column_spec) {
  auto ad = CreateDescForColumn(ComboAggregateRequest::COUNT, std::move(column_spec));
  auto impl = AggregateImpl::Create(std::move(ad));
  return Aggregate(std::move(impl));
}

Aggregate Aggregate::First(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::FIRST, std::move(column_specs));
}

Aggregate Aggregate::Group(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::GROUP, std::move(column_specs));
}

Aggregate Aggregate::Last(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::LAST, std::move(column_specs));
}

Aggregate Aggregate::Max(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::MAX, std::move(column_specs));
}

Aggregate Aggregate::Med(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::MEDIAN, std::move(column_specs));
}

Aggregate Aggregate::Min(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::MIN, std::move(column_specs));
}

Aggregate Aggregate::Pct(double percentile, bool avg_median, std::vector<std::string> column_specs) {
  ComboAggregateRequest::Aggregate pd;
  pd.set_type(ComboAggregateRequest::PERCENTILE);
  pd.set_percentile(percentile);
  pd.set_avg_median(avg_median);
  for (auto &cs : column_specs) {
    pd.mutable_match_pairs()->Add(std::move(cs));
  }
  auto impl = AggregateImpl::Create(std::move(pd));
  return Aggregate(std::move(impl));
}

Aggregate Aggregate::Std(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::STD, std::move(column_specs));
}

Aggregate Aggregate::Sum(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::SUM, std::move(column_specs));
}

Aggregate Aggregate::Var(std::vector<std::string> column_specs) {
  return createAggForMatchPairs(ComboAggregateRequest::VAR, std::move(column_specs));
}

Aggregate Aggregate::WAvg(std::string weight_column, std::vector<std::string> column_specs) {
  ComboAggregateRequest::Aggregate pd;
  pd.set_type(ComboAggregateRequest::WEIGHTED_AVG);
  for (auto &cs : column_specs) {
    pd.mutable_match_pairs()->Add(std::move(cs));
  }
  pd.set_column_name(std::move(weight_column));
  auto impl = AggregateImpl::Create(std::move(pd));
  return Aggregate(std::move(impl));
}

AggregateCombo AggregateCombo::Create(std::initializer_list<Aggregate> list) {
  std::vector<ComboAggregateRequest::Aggregate> aggregates;
  aggregates.reserve(list.size());
  for (const auto &item : list) {
    aggregates.push_back(item.Impl()->Descriptor());
  }
  auto impl = AggregateComboImpl::Create(std::move(aggregates));
  return AggregateCombo(std::move(impl));
}

AggregateCombo AggregateCombo::Create(std::vector<Aggregate> vec) {
  std::vector<ComboAggregateRequest::Aggregate> aggregates;
  aggregates.reserve(vec.size());
  for (auto &item : vec) {
    aggregates.push_back(std::move(item.Impl()->Descriptor()));
  }
  auto impl = AggregateComboImpl::Create(std::move(aggregates));
  return AggregateCombo(std::move(impl));
}

AggregateCombo::AggregateCombo(std::shared_ptr<impl::AggregateComboImpl> impl) : impl_(std::move(impl)) {}
AggregateCombo::AggregateCombo(const deephaven::client::AggregateCombo &other) = default;
AggregateCombo &AggregateCombo::operator=(const AggregateCombo &other) = default;
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

TableHandleManager TableHandle::GetManager() const {
  return TableHandleManager(impl_->ManagerImpl());
}

TableHandle TableHandle::Where(std::string condition) const {
  auto qt_impl = impl_->Where(std::move(condition));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Sort(std::vector<SortPair> sortPairs) const {
  auto qt_impl = impl_->Sort(std::move(sortPairs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Select(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->Select(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Update(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->Update(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::LazyUpdate(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->LazyUpdate(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::View(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->View(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::DropColumns(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->DropColumns(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::UpdateView(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->UpdateView(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::By(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->By(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::By(AggregateCombo combo, std::vector<std::string> group_by_columns) const {
  auto qt_impl = impl_->By(combo.Impl()->Aggregates(), std::move(group_by_columns));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::MinBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->MinBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::MaxBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->MaxBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::SumBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->SumBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::AbsSumBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->AbsSumBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::VarBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->VarBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::StdBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->StdBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::AvgBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->AvgBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::LastBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->LastBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::FirstBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->FirstBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::MedianBy(std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->MedianBy(std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::PercentileBy(double percentile, bool avg_median,
    std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->PercentileBy(percentile, avg_median, std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::CountBy(std::string count_by_column,
    std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->CountBy(std::move(count_by_column), std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::WAvgBy(std::string weight_column,
    std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->WavgBy(std::move(weight_column), std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::TailBy(int64_t n, std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->TailBy(n, std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::HeadBy(int64_t n, std::vector<std::string> column_specs) const {
  auto qt_impl = impl_->HeadBy(n, std::move(column_specs));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Head(int64_t n) const {
  auto qt_impl = impl_->Head(n);
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Tail(int64_t n) const {
  auto qt_impl = impl_->Tail(n);
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Ungroup(bool null_fill, std::vector<std::string> group_by_columns) const {
  auto qt_impl = impl_->Ungroup(null_fill, std::move(group_by_columns));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Merge(std::string key_columns, std::vector<TableHandle> sources) const {
  std::vector<Ticket> source_handles;
  source_handles.reserve(sources.size() + 1);
  source_handles.push_back(impl_->Ticket());
  for (const auto &s : sources) {
    source_handles.push_back(s.Impl()->Ticket());
  }
  auto qt_impl = impl_->Merge(std::move(key_columns), std::move(source_handles));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::CrossJoin(const TableHandle &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) const {
  auto qt_impl = impl_->CrossJoin(*right_side.impl_, std::move(columns_to_match),
      std::move(columns_to_add));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::NaturalJoin(const TableHandle &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) const {
  auto qt_impl = impl_->NaturalJoin(*right_side.impl_, std::move(columns_to_match),
      std::move(columns_to_add));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::ExactJoin(const TableHandle &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) const {
  auto qt_impl = impl_->ExactJoin(*right_side.impl_, std::move(columns_to_match),
      std::move(columns_to_add));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Aj(const TableHandle &right_side,
    std::vector<std::string> on, std::vector<std::string> joins) const {
  auto qt_impl = impl_->Aj(*right_side.impl_, std::move(on), std::move(joins));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::Raj(const TableHandle &right_side,
    std::vector<std::string> on, std::vector<std::string> joins) const {
  auto qt_impl = impl_->Raj(*right_side.impl_, std::move(on), std::move(joins));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::LeftOuterJoin(const TableHandle &right_side, std::vector<std::string> on,
    std::vector<std::string> joins) const {
  auto qt_impl = impl_->LeftOuterJoin(*right_side.impl_, std::move(on), std::move(joins));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::UpdateBy(std::vector<UpdateByOperation> ops, std::vector<std::string> by) const {
  auto op_impls = MakeReservedVector<std::shared_ptr<UpdateByOperationImpl>>(ops.size());
  for (const auto &op : ops) {
    op_impls.push_back(op.impl_);
  }
  auto th_impl = impl_->UpdateBy(std::move(op_impls), std::move(by));
  return TableHandle(std::move(th_impl));
}

TableHandle TableHandle::SelectDistinct(std::vector<std::string> columns) const {
  auto qt_impl = impl_->SelectDistinct(std::move(columns));
  return TableHandle(std::move(qt_impl));
}

TableHandle TableHandle::WhereIn(const TableHandle &filter_table,
    std::vector<std::string> columns) const {
  auto th_impl = impl_->WhereIn(*filter_table.impl_, std::move(columns));
  return TableHandle(std::move(th_impl));
}

void TableHandle::AddTable(const deephaven::client::TableHandle &table_to_add) {
  impl_->AddTable(*table_to_add.impl_);
}

void TableHandle::RemoveTable(const deephaven::client::TableHandle &table_to_remove) {
  impl_->RemoveTable(*table_to_remove.impl_);
}

void TableHandle::BindToVariable(std::string variable) const {
  return impl_->BindToVariable(std::move(variable));
}

internal::TableHandleStreamAdaptor TableHandle::Stream(bool want_headers) const {
  return {*this, want_headers};
}

int64_t TableHandle::NumRows() const {
  return impl_->NumRows();
}

bool TableHandle::IsStatic() const {
  return impl_->IsStatic();
}

std::shared_ptr<Schema> TableHandle::Schema() const {
  return impl_->Schema();
}

std::shared_ptr<arrow::flight::FlightStreamReader> TableHandle::GetFlightStreamReader() const {
  return GetManager().CreateFlightWrapper().GetFlightStreamReader(*this);
}

std::shared_ptr<arrow::Table> TableHandle::ToArrowTable() const {
  auto res = GetFlightStreamReader()->ToTable();
  return ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(std::move(res)));
}

std::shared_ptr<ClientTable> TableHandle::ToClientTable() const {
  auto at = ToArrowTable();
  return ArrowClientTable::Create(std::move(at));
}

std::shared_ptr<SubscriptionHandle> TableHandle::Subscribe(
    std::shared_ptr<TickingCallback> callback) {
  return impl_->Subscribe(std::move(callback));
}

std::shared_ptr<SubscriptionHandle>
TableHandle::Subscribe(onTickCallback_t on_tick, void *on_tick_user_data,
    onErrorCallback_t on_error, void *on_error_user_data) {
  return impl_->Subscribe(on_tick, on_tick_user_data, on_error, on_error_user_data);
}

void TableHandle::Unsubscribe(const std::shared_ptr<SubscriptionHandle> &handle) {
  impl_->Unsubscribe(handle);
}

const std::string &TableHandle::GetTicketAsString() const {
  return impl_->Ticket().ticket();
}

std::string TableHandle::ToString(bool want_headers) const {
  SimpleOstringstream oss;
  oss << Stream(want_headers);
  return std::move(oss.str());
}

namespace internal {
TableHandleStreamAdaptor::TableHandleStreamAdaptor(TableHandle table, bool want_headers) :
    table_(std::move(table)), wantHeaders_(want_headers) {}
TableHandleStreamAdaptor::~TableHandleStreamAdaptor() = default;

std::ostream &operator<<(std::ostream &s, const TableHandleStreamAdaptor &o) {
  PrintTableData(s, o.table_, o.wantHeaders_);
  return s;
}
}  // namespace internal

namespace {
void PrintTableData(std::ostream &s, const TableHandle &table_handle, bool want_headers) {
  auto fsr = table_handle.GetFlightStreamReader();

  if (want_headers) {
    auto schema = table_handle.Schema();
    s << separatedList(schema->Names().begin(), schema->Names().end(), "\t") << '\n';
  }

  while (true) {
    auto chunk = fsr->Next();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(chunk));
    if (chunk->data == nullptr) {
      break;
    }
    const auto *data = chunk->data.get();
    const auto &columns = data->columns();
    for (int64_t row_num = 0; row_num < data->num_rows(); ++row_num) {
      if (row_num != 0) {
        s << '\n';
      }
      auto stream_array_cell = [row_num](std::ostream &s, const std::shared_ptr<arrow::Array> &a) {
        // This is going to be rather inefficient
        auto rsc = a->GetScalar(row_num);
        const auto &vsc = *rsc.ValueOrDie();
        s << vsc.ToString();
      };
      s << separatedList(columns.begin(), columns.end(), "\t", stream_array_cell);
    }
  }
}
void CheckNotClosedOrThrow(const std::shared_ptr<ClientImpl> &impl) {
  if (impl == nullptr) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("client is already closed"));
  }
}
}  // namespace
}  // namespace deephaven::client
