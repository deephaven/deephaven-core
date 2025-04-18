/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/table_maker.h"

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/flight/client.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include "deephaven/client/client.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"

#include "arrow/array/array_base.h"

using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::client::TableHandle;
using deephaven::client::utility::internal::DeephavenMetadataConstants;

#include <memory>

namespace deephaven::client::utility {
TableMaker::TableMaker() = default;
TableMaker::~TableMaker() = default;

void TableMaker::FinishAddColumn(std::string name, std::shared_ptr<arrow::Array> data,
    std::string deephaven_metadata_type_name,
    std::optional<std::string> deephaven_metadata_component_type_name) {
  auto kv_metadata = std::make_shared<arrow::KeyValueMetadata>();
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(
    kv_metadata->Set(DeephavenMetadataConstants::Keys::kType, std::move(deephaven_metadata_type_name))));
  if (deephaven_metadata_component_type_name.has_value()) {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(
        kv_metadata->Set(DeephavenMetadataConstants::Keys::kComponentType,
            std::move(*deephaven_metadata_component_type_name))));
  }

  column_infos_.emplace_back(std::move(name), data->type(), std::move(kv_metadata),
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
  auto result = MakeReservedVector<std::shared_ptr<arrow::Array>>(column_infos_.size());
  for (const auto &info : column_infos_) {
    result.emplace_back(info.data_);
  }
  if (result.empty()) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Can't make table with no columns"));
  }
  return result;
}

std::shared_ptr<arrow::Schema> TableMaker::MakeSchema() const {
  ValidateSchema();

  arrow::SchemaBuilder sb;
  for (const auto &info : column_infos_) {
    auto field = std::make_shared<arrow::Field>(info.name_, info.arrow_type_, true,
        info.arrow_metadata_);
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(sb.AddField(field)));
  }

  return ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(sb.Finish()));
}

void TableMaker::ValidateSchema() const {
  if (column_infos_.empty()) {
    return;
  }

  auto num_rows = column_infos_.front().data_->length();
  for (size_t i = 1; i != column_infos_.size(); ++i) {
    const auto &ci = column_infos_[i];
    if (ci.data_->length() != num_rows) {
      auto message = fmt::format("Column sizes not consistent: column 0 has size {}, but column {} has size {}",
          num_rows, i, ci.data_->length());
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
}

TableMaker::ColumnInfo::ColumnInfo(std::string name, std::shared_ptr<arrow::DataType> arrow_type,
    std::shared_ptr<arrow::KeyValueMetadata> arrow_metadata, std::shared_ptr<arrow::Array> data) :
    name_(std::move(name)), arrow_type_(std::move(arrow_type)),
    arrow_metadata_(std::move(arrow_metadata)), data_(std::move(data)) {}
TableMaker::ColumnInfo::ColumnInfo(ColumnInfo &&other) noexcept = default;
TableMaker::ColumnInfo::~ColumnInfo() = default;

namespace internal {
const char DeephavenMetadataConstants::Keys::kType[] = "deephaven:type";
const char DeephavenMetadataConstants::Keys::kComponentType[] = "deephaven:componentType";

const char DeephavenMetadataConstants::Types::kBool[] = "java.lang.Boolean";
const char DeephavenMetadataConstants::Types::kChar16[] = "char";
const char DeephavenMetadataConstants::Types::kInt8[] = "byte";
const char DeephavenMetadataConstants::Types::kInt16[] = "short";
const char DeephavenMetadataConstants::Types::kInt32[] = "int";
const char DeephavenMetadataConstants::Types::kInt64[] = "long";
const char DeephavenMetadataConstants::Types::kFloat[] = "float";
const char DeephavenMetadataConstants::Types::kDouble[] = "double";
const char DeephavenMetadataConstants::Types::kString[] = "java.lang.String";
const char DeephavenMetadataConstants::Types::kDateTime[] = "java.time.ZonedDateTime";
const char DeephavenMetadataConstants::Types::kLocalDate[] = "java.time.LocalDate";
const char DeephavenMetadataConstants::Types::kLocalTime[] = "java.time.LocalTime";
}  // namespace internal
}  // namespace deephaven::client::utility
