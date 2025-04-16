/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/table_maker.h"

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "deephaven/client/flight.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/third_party/fmt/format.h"

#include "arrow/array/array_base.h"
#include "arrow/array/builder_nested.h"

using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::client::TableHandle;

#include <memory>

namespace deephaven::client::utility {
TableMaker::TableMaker() = default;
TableMaker::~TableMaker() = default;

void TableMaker::FinishAddColumn(std::string name, std::shared_ptr<arrow::Array> data,
    std::string deephaven_server_type_name) {
  if (!column_infos_.empty()) {
    auto num_rows = column_infos_.back().data_->length();
    if (data->length() != num_rows) {
      auto message = fmt::format("Column sizes not consistent: expected {}, have {}", num_rows,
          data->length());
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }

  const auto &arrow_type = data->type();
  column_infos_.emplace_back(std::move(name), arrow_type, std::move(deephaven_server_type_name),
      std::move(data));
}

TableHandle TableMaker::MakeTable(const TableHandleManager &manager) const {
  auto schema = MakeSchema();

  auto wrapper = manager.CreateFlightWrapper();
  auto ticket = manager.NewTicket();
  auto flight_descriptor = ArrowUtil::ConvertTicketToFlightDescriptor(ticket);

  arrow::flight::FlightCallOptions options;
  wrapper.AddHeaders(&options);

  auto res = wrapper.FlightClient()->DoPut(options, flight_descriptor, schema);
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res));
  auto data = GetColumnsNotEmpty();
  auto num_rows = data.back()->length();
  auto batch = arrow::RecordBatch::Make(schema, num_rows, std::move(data));

  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->WriteRecordBatch(*batch)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->DoneWriting()));

  std::shared_ptr<arrow::Buffer> buf;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->reader->ReadMetadata(&buf)));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->Close()));
  return manager.MakeTableHandleFromTicket(std::move(ticket));
}

std::shared_ptr<arrow::Table> TableMaker::MakeArrowTable() const {
  auto schema = MakeSchema();

  // Extract the data_vec column.
  auto data_vec = MakeReservedVector<std::shared_ptr<arrow::Array>>(column_infos_.size());
  for (const auto &info : column_infos_) {
    data_vec.push_back(info.data_);
  }
  auto data = GetColumnsNotEmpty();
  return arrow::Table::Make(std::move(schema), std::move(data));
}

std::vector<std::shared_ptr<arrow::Array>> TableMaker::GetColumnsNotEmpty() const {
  std::vector<std::shared_ptr<arrow::Array>> result;
  for (const auto &info : column_infos_) {
    result.emplace_back(info.data_);
  }
  if (result.empty()) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Can't make table with no columns"));
  }
  return result;
}

std::shared_ptr<arrow::Schema> TableMaker::MakeSchema() const {
  arrow::SchemaBuilder sb;
  for (const auto &info : column_infos_) {
    auto kv_metadata = std::make_shared<arrow::KeyValueMetadata>();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(kv_metadata->Set("deephaven:type",
        info.deepaven_server_type_name_)));

    auto field = std::make_shared<arrow::Field>(info.name_, info.arrow_type_, true,
        std::move(kv_metadata));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(sb.AddField(field)));
  }

  return ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(sb.Finish()));
}

TableMaker::ColumnInfo::ColumnInfo(std::string name, std::shared_ptr<arrow::DataType> arrow_type,
    std::string deepaven_server_type_name, std::shared_ptr<arrow::Array> data) :
    name_(std::move(name)), arrow_type_(std::move(arrow_type)),
    deepaven_server_type_name_(std::move(deepaven_server_type_name)), data_(std::move(data)) {}
TableMaker::ColumnInfo::ColumnInfo(ColumnInfo &&other) noexcept = default;
TableMaker::ColumnInfo::~ColumnInfo() = default;

namespace internal {
const char DeephavenServerConstants::kBool[] = "java.lang.Boolean";
const char DeephavenServerConstants::kChar16[] = "char";
const char DeephavenServerConstants::kInt8[] = "byte";
const char DeephavenServerConstants::kInt16[] = "short";
const char DeephavenServerConstants::kInt32[] = "int";
const char DeephavenServerConstants::kInt64[] = "long";
const char DeephavenServerConstants::kFloat[] = "float";
const char DeephavenServerConstants::kDouble[] = "double";
const char DeephavenServerConstants::kString[] = "java.lang.String";
const char DeephavenServerConstants::kDateTime[] = "java.time.ZonedDateTime";
const char DeephavenServerConstants::kLocalDate[] = "java.time.LocalDate";
const char DeephavenServerConstants::kLocalTime[] = "java.time.LocalTime";
const char DeephavenServerConstants::kList[] = "what.goes.here";
}  // namespace internal
}  // namespace deephaven::client::utility
