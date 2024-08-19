/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/table_handle_impl.h"

#include <deque>
#include <memory>
#include <mutex>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/impl/update_by_operation_impl.h"
#include "deephaven/client/client.h"
#include "deephaven/client/impl/util.h"
#include "deephaven/client/subscription/subscribe_thread.h"
#include "deephaven/client/subscription/subscription_handle.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ranges.h"

using io::deephaven::proto::backplane::grpc::AddTableRequest;
using io::deephaven::proto::backplane::grpc::AddTableResponse;
using io::deephaven::proto::backplane::grpc::AjRajTablesRequest;
using io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
using io::deephaven::proto::backplane::grpc::CrossJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::DeleteTableRequest;
using io::deephaven::proto::backplane::grpc::DeleteTableResponse;
using io::deephaven::proto::backplane::grpc::DropColumnsRequest;
using io::deephaven::proto::backplane::grpc::ExactJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse;
using io::deephaven::proto::backplane::grpc::HeadOrTailByRequest;
using io::deephaven::proto::backplane::grpc::HeadOrTailRequest;
using io::deephaven::proto::backplane::grpc::LeftJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::MergeTablesRequest;
using io::deephaven::proto::backplane::grpc::NaturalJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::ReleaseRequest;
using io::deephaven::proto::backplane::grpc::ReleaseResponse;
using io::deephaven::proto::backplane::grpc::SelectDistinctRequest;
using io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest;
using io::deephaven::proto::backplane::grpc::SortDescriptor;
using io::deephaven::proto::backplane::grpc::SortTableRequest;
using io::deephaven::proto::backplane::grpc::TableReference;
using io::deephaven::proto::backplane::grpc::TableService;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::grpc::UngroupRequest;
using io::deephaven::proto::backplane::grpc::UpdateByRequest;
using io::deephaven::proto::backplane::grpc::UnstructuredFilterTableRequest;
using io::deephaven::proto::backplane::grpc::WhereInRequest;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableRequest;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse;
using deephaven::client::impl::MoveVectorData;
using deephaven::client::server::Server;
using deephaven::client::subscription::SubscriptionThread;
using deephaven::client::subscription::SubscriptionHandle;
using deephaven::client::utility::ArrowUtil;
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
using deephaven::dhcore::utility::GetWhat;
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::dhcore::utility::separatedList;

using UpdateByOperationProto = io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation;

namespace deephaven::client::impl {
std::shared_ptr<TableHandleImpl>
TableHandleImpl::Create(std::shared_ptr<TableHandleManagerImpl> thm,
    ExportedTableCreationResponse response) {
  return std::make_shared<TableHandleImpl>(Private(), std::move(thm),
      std::move(*response.mutable_result_id()->mutable_ticket()), response.size(),
      response.is_static());
}

TableHandleImpl::TableHandleImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&thm,
    TicketType &&ticket, int64_t num_rows, bool is_static) : managerImpl_(std::move(thm)),
    ticket_(std::move(ticket)), num_rows_(num_rows), is_static_(is_static) {}

TableHandleImpl::~TableHandleImpl() {
  try {
    managerImpl_->Server()->Release(std::move(ticket_));
  } catch (...) {
    auto what = GetWhat(std::current_exception());
    gpr_log(GPR_INFO, "TableHandleImpl destructor is ignoring thrown exception: %s", what.c_str());
  }
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Select(std::vector<std::string> column_specs) {
  return SelectOrUpdateHelper(std::move(column_specs), &TableService::Stub::Select);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Update(std::vector<std::string> column_specs) {
  return SelectOrUpdateHelper(std::move(column_specs), &TableService::Stub::Update);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::LazyUpdate(std::vector<std::string> column_specs) {
  return SelectOrUpdateHelper(std::move(column_specs), &TableService::Stub::LazyUpdate);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::View(std::vector<std::string> column_specs) {
  return SelectOrUpdateHelper(std::move(column_specs), &TableService::Stub::View);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::UpdateView(std::vector<std::string> column_specs) {
  return SelectOrUpdateHelper(std::move(column_specs), &TableService::Stub::UpdateView);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::DropColumns(std::vector<std::string> column_specs) {
  auto *server = managerImpl_->Server().get();
  DropColumnsRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  MoveVectorData(std::move(column_specs), req.mutable_column_names());

  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->DropColumns(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl>
TableHandleImpl::SelectOrUpdateHelper(std::vector<std::string> column_specs,
    selectOrUpdateMethod_t which_method) {
  auto *server = managerImpl_->Server().get();
  SelectOrUpdateRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  for (auto &cs: column_specs) {
    *req.mutable_column_specs()->Add() = std::move(cs);
  }
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return (server->TableStub()->*which_method)(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Where(std::string condition) {
  auto *server = managerImpl_->Server().get();
  UnstructuredFilterTableRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  *req.mutable_filters()->Add() = std::move(condition);
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->UnstructuredFilter(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Sort(std::vector<SortPair> sort_pairs) {
  auto *server = managerImpl_->Server().get();
  SortTableRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  for (auto &sp: sort_pairs) {
    auto which = sp.Direction() == SortDirection::kAscending ?
        SortDescriptor::ASCENDING : SortDescriptor::DESCENDING;
    SortDescriptor sd;
    sd.set_column_name(std::move(sp.Column()));
    sd.set_is_absolute(sp.Abs());
    sd.set_direction(which);
    *req.mutable_sorts()->Add() = std::move(sd);
  }
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->Sort(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::DefaultAggregateByDescriptor(
    ComboAggregateRequest::Aggregate descriptor, std::vector<std::string> group_by_columns) {
  auto descriptors = MakeReservedVector<ComboAggregateRequest::Aggregate>(1);
  descriptors.push_back(std::move(descriptor));
  return By(std::move(descriptors), std::move(group_by_columns));
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
  ComboAggregateRequest req;
  *req.mutable_result_id() = std::move(result_ticket);
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  for (auto &agg : descriptors) {
    *req.mutable_aggregates()->Add() = std::move(agg);
  }
  for (auto &gbc : group_by_columns) {
    *req.mutable_group_by_columns()->Add() = std::move(gbc);
  }
  req.set_force_combo(false);
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->ComboAggregate(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
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
  HeadOrTailByRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  req.set_num_rows(n);
  for (auto &cs : column_specs) {
    req.mutable_group_by_column_specs()->Add(std::move(cs));
  }
  const auto &which = head ? &TableService::Stub::HeadBy : &TableService::Stub::TailBy;
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return (server->TableStub()->*which)(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Tail(int64_t n) {
  return HeadOrTailHelper(false, n);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Head(int64_t n) {
  return HeadOrTailHelper(true, n);
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::HeadOrTailHelper(bool head, int64_t n) {
  auto *server = managerImpl_->Server().get();
  HeadOrTailRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  req.set_num_rows(n);
  ExportedTableCreationResponse resp;
  const auto &which = head ? &TableService::Stub::Head : &TableService::Stub::Tail;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return (server->TableStub()->*which)(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Ungroup(bool null_fill,
    std::vector<std::string> group_by_columns) {
  auto *server = managerImpl_->Server().get();
  UngroupRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  req.set_null_fill(null_fill);
  MoveVectorData(std::move(group_by_columns), req.mutable_columns_to_ungroup());
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->Ungroup(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Merge(std::string key_column,
    std::vector<TicketType> source_tickets) {
  auto *server = managerImpl_->Server().get();
  MergeTablesRequest req;
  *req.mutable_result_id() = server->NewTicket();
  for (auto &t: source_tickets) {
    *req.mutable_source_ids()->Add()->mutable_ticket() = std::move(t);
  }
  req.set_key_column(std::move(key_column));
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->MergeTables(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::CrossJoin(const TableHandleImpl &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
  auto *server = managerImpl_->Server().get();
  CrossJoinTablesRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_left_id()->mutable_ticket() = ticket_;
  *req.mutable_right_id()->mutable_ticket() = right_side.ticket_;
  MoveVectorData(std::move(columns_to_match), req.mutable_columns_to_match());
  MoveVectorData(std::move(columns_to_add), req.mutable_columns_to_add());
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->CrossJoinTables(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::NaturalJoin(const TableHandleImpl &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
  auto *server = managerImpl_->Server().get();
  NaturalJoinTablesRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_left_id()->mutable_ticket() = ticket_;
  *req.mutable_right_id()->mutable_ticket() = right_side.ticket_;
  MoveVectorData(std::move(columns_to_match), req.mutable_columns_to_match());
  MoveVectorData(std::move(columns_to_add), req.mutable_columns_to_add());
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->NaturalJoinTables(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::ExactJoin(const TableHandleImpl &right_side,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
  auto *server = managerImpl_->Server().get();
  ExactJoinTablesRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_left_id()->mutable_ticket() = ticket_;
  *req.mutable_right_id()->mutable_ticket() = right_side.ticket_;
  MoveVectorData(std::move(columns_to_match), req.mutable_columns_to_match());
  MoveVectorData(std::move(columns_to_add), req.mutable_columns_to_add());
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->ExactJoinTables(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

namespace {
AjRajTablesRequest MakeAjRajTablesRequest(Ticket left_table_ticket, Ticket right_table_ticket,
    std::vector<std::string> on, std::vector<std::string> joins, Ticket result) {
  if (on.empty()) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Need at least one 'on' column"));
  }
  AjRajTablesRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_left_id()->mutable_ticket() = std::move(left_table_ticket);
  *req.mutable_right_id()->mutable_ticket() = std::move(right_table_ticket);
  // The final 'on' column is the as of column
  *req.mutable_as_of_column() = std::move(on.back());
  on.pop_back();
  // The remaining 'on' columns are the exact_match_columns
  MoveVectorData(std::move(on), req.mutable_exact_match_columns());
  MoveVectorData(std::move(joins), req.mutable_columns_to_add());
  return req;
}
}  // namespace

std::shared_ptr<TableHandleImpl> TableHandleImpl::Aj(const TableHandleImpl &right_side,
    std::vector<std::string> on, std::vector<std::string> joins) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto req = MakeAjRajTablesRequest(ticket_, right_side.ticket_, std::move(on), std::move(joins),
      std::move(result_ticket));
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->AjTables(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::Raj(const TableHandleImpl &right_side,
    std::vector<std::string> on, std::vector<std::string> joins) {
  auto *server = managerImpl_->Server().get();
  auto result_ticket = server->NewTicket();
  auto req = MakeAjRajTablesRequest(ticket_, std::move(right_side.ticket_),
      std::move(on), std::move(joins), std::move(result_ticket));
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->RajTables(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl> TableHandleImpl::LeftOuterJoin(const TableHandleImpl &right_side,
    std::vector<std::string> on, std::vector<std::string> joins) {
  auto *server = managerImpl_->Server().get();
  LeftJoinTablesRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_left_id()->mutable_ticket() = ticket_;
  *req.mutable_right_id()->mutable_ticket() = right_side.ticket_;
  MoveVectorData(std::move(on), req.mutable_columns_to_match());
  MoveVectorData(std::move(joins), req.mutable_columns_to_add());
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->LeftJoinTables(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl>
TableHandleImpl::UpdateBy(std::vector<std::shared_ptr<UpdateByOperationImpl>> ops,
    std::vector<std::string> by) {
  auto *server = managerImpl_->Server().get();
  UpdateByRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  for (const auto &op : ops) {
    *req.mutable_operations()->Add() = op->UpdateByProto();
  }
  MoveVectorData(std::move(by), req.mutable_group_by_columns());
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->UpdateBy(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl>
TableHandleImpl::SelectDistinct(std::vector<std::string> columns) {
  auto *server = managerImpl_->Server().get();
  SelectDistinctRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_source_id()->mutable_ticket() = ticket_;
  MoveVectorData(std::move(columns), req.mutable_column_names());
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->SelectDistinct(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

std::shared_ptr<TableHandleImpl>
TableHandleImpl::WhereIn(const deephaven::client::impl::TableHandleImpl &filter_table,
    std::vector<std::string> columns) {
  auto *server = managerImpl_->Server().get();
  WhereInRequest req;
  *req.mutable_result_id() = server->NewTicket();
  *req.mutable_left_id()->mutable_ticket() = ticket_;
  *req.mutable_right_id()->mutable_ticket() = filter_table.ticket_;
  req.set_inverted(false);
  MoveVectorData(std::move(columns), req.mutable_columns_to_match());
  ExportedTableCreationResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->TableStub()->WhereIn(ctx, req, &resp);
  });
  return TableHandleImpl::Create(managerImpl_, std::move(resp));
}

void TableHandleImpl::AddTable(const TableHandleImpl &table_to_add) {
  auto *server = managerImpl_->Server().get();
  AddTableRequest req;
  *req.mutable_input_table() = ticket_;
  *req.mutable_table_to_add() = table_to_add.ticket_;
  AddTableResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->InputTableStub()->AddTableToInputTable(ctx, req, &resp);
  });
}

void TableHandleImpl::RemoveTable(const TableHandleImpl &table_to_remove) {
  auto *server = managerImpl_->Server().get();
  DeleteTableRequest req;
  *req.mutable_input_table() = ticket_;
  *req.mutable_table_to_remove() = table_to_remove.ticket_;
  DeleteTableResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->InputTableStub()->DeleteTableFromInputTable(ctx, req, &resp);
  });
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
  auto schema = Schema();
  auto handle = SubscriptionThread::Start(managerImpl_->Server(), managerImpl_->FlightExecutor().get(),
      schema, ticket_, std::move(callback));
  managerImpl_->AddSubscriptionHandle(handle);
  return handle;
}

void TableHandleImpl::Unsubscribe(const std::shared_ptr<SubscriptionHandle> &handle) {
  managerImpl_->RemoveSubscriptionHandle(handle);
  handle->Cancel();
}

void TableHandleImpl::LookupHelper(const std::string &column_name,
    std::initializer_list<ElementTypeId::Enum> valid_types) {
  auto schema = Schema();
  auto index = *schema->GetColumnIndex(column_name, true);
  auto actual_type = schema->Types()[index];
  for (auto type : valid_types) {
    if (actual_type == type) {
      return;
    }
  }

  auto renderable_valid_types = MakeReservedVector<int32_t>(valid_types.size());
  for (const auto &item : valid_types) {
    renderable_valid_types.push_back(static_cast<int32_t>(item));
  }
  auto message = fmt::format("Column lookup for {}: Expected Arrow type: one of {{{}}}. Actual type {}",
      column_name, renderable_valid_types, static_cast<int>(actual_type));
  throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
}

void TableHandleImpl::BindToVariable(std::string variable) {
  const auto &console_id = managerImpl_->ConsoleId();
  if (!console_id.has_value()) {
    auto message = DEEPHAVEN_LOCATION_STR(
        "Client was created without specifying a script language");
    throw std::runtime_error(message);
  }
  BindTableToVariableRequest req;
  *req.mutable_console_id() = *console_id;
  req.set_variable_name(std::move(variable));
  *req.mutable_table_id() = ticket_;

  auto *server = managerImpl_->Server().get();
  BindTableToVariableResponse resp;
  server->SendRpc([&](grpc::ClientContext *ctx) {
    return server->ConsoleStub()->BindTableToVariable(ctx, req, &resp);
  });
}

std::shared_ptr<Schema> TableHandleImpl::Schema() {
  std::unique_lock guard(mutex_);
  if (schema_request_sent_) {
    // Schema request already sent by someone else. So wait for the successful result or error.
    guard.unlock();
    return schema_future_.get();
  }

  // While still under lock, initialize the future and set the flag indicating that the schema
  // request is on its way.
  std::promise<std::shared_ptr<SchemaType>> schema_promise;
  schema_future_ = schema_promise.get_future().share();
  schema_request_sent_ = true;
  guard.unlock();

  try {
    auto *server = managerImpl_->Server().get();

    arrow::flight::FlightCallOptions options;
    server->ForEachHeaderNameAndValue(
        [&options](const std::string &name, const std::string &value) {
          options.headers.emplace_back(name, value);
        }
    );

    auto fd = ArrowUtil::ConvertTicketToFlightDescriptor(ticket_.ticket());
    auto gs_result = server->FlightClient()->GetSchema(options, fd);
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(gs_result));

    auto schema_result = (*gs_result)->GetSchema(nullptr);
    auto arrow_schema = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(schema_result));
    auto deephaven_schema = ArrowUtil::MakeDeephavenSchema(*arrow_schema);
    schema_promise.set_value(std::move(deephaven_schema));
  } catch (...) {
    schema_promise.set_exception(std::current_exception());
  }

  return schema_future_.get();
}
}  // namespace deephaven::client::impl
