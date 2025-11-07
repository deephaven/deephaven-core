/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/arrow_util.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <optional>
#include <utility>

#include <arrow/status.h>
#include <arrow/flight/types.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/visitor.h>
#include "deephaven/client/arrowutil/arrow_array_converter.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"

using deephaven::client::arrowutil::ArrowArrayConverter;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::CharChunk;
using deephaven::dhcore::chunk::DateTimeChunk;
using deephaven::dhcore::chunk::DoubleChunk;
using deephaven::dhcore::chunk::FloatChunk;
using deephaven::dhcore::chunk::Int8Chunk;
using deephaven::dhcore::chunk::Int16Chunk;
using deephaven::dhcore::chunk::Int32Chunk;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::chunk::LocalDateChunk;
using deephaven::dhcore::chunk::LocalTimeChunk;
using deephaven::dhcore::chunk::StringChunk;
using deephaven::dhcore::chunk::UInt8Chunk;
using deephaven::dhcore::chunk::UInt16Chunk;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::column::ColumnSourceVisitor;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::LocalDate;
using deephaven::dhcore::LocalTime;
using deephaven::dhcore::ElementType;
using deephaven::dhcore::ElementTypeId;
using deephaven::dhcore::utility::MakeReservedVector;

namespace deephaven::client::utility {
void OkOrThrow(const deephaven::dhcore::utility::DebugInfo &debug_info,
    const arrow::Status &status) {
  if (status.ok()) {
    return;
  }

  auto msg = fmt::format("Status: {}. Caller: {}", status.ToString(), debug_info);
  throw std::runtime_error(msg);
}

arrow::flight::FlightDescriptor ArrowUtil::ConvertTicketToFlightDescriptor(const std::string &ticket) {
  if (ticket.length() != 5 || ticket[0] != 'e') {
    const char *message = "Ticket is not in correct format for export";
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  uint32_t value;
  std::memcpy(&value, ticket.data() + 1, sizeof(value));
  return arrow::flight::FlightDescriptor::Path({"export", std::to_string(value)});
};

namespace {
struct ArrowToElementTypeId final : public arrow::TypeVisitor {
  arrow::Status Visit(const arrow::Int8Type &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kInt8);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kInt16);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kInt32);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kInt64);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kFloat);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kDouble);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kBool);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt16Type &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kChar);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kString);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kTimestamp);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListType &list_type) final {
    ArrowToElementTypeId inner;
    auto result = list_type.value_type()->Accept(&inner);
    if (result.ok()) {
      element_type_ = inner.element_type_.WrapList();
    }
    return result;
  }

  arrow::Status Visit(const arrow::Time64Type &/*type*/) final {
    element_type_ = ElementType::Of(ElementTypeId::kLocalTime);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Date64Type &type) final {
    if (type.unit() != arrow::DateUnit::MILLI) {
      auto message = fmt::format("Expected Date64Type with milli units, got {}",
          type.ToString());
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
    element_type_ = ElementType::Of(ElementTypeId::kLocalDate);
    return arrow::Status::OK();
  }

  ElementType element_type_;
};
}  // namespace

std::optional<ElementType> ArrowUtil::GetElementType(const arrow::DataType &data_type,
    bool must_succeed) {
  ArrowToElementTypeId visitor;
  auto result = data_type.Accept(&visitor);
  if (result.ok()) {
    return visitor.element_type_;
  }
  if (!must_succeed) {
    return {};
  }
  auto message = fmt::format("Can't find Deephaven mapping for arrow data type {}",
      data_type.ToString());
  throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
}

std::shared_ptr<arrow::DataType> ArrowUtil::GetArrowType(const ElementType &element_type) {
  if (element_type.ListDepth() > 0) {
    auto inner = GetArrowType(element_type.UnwrapList());
    return std::make_shared<arrow::ListType>(std::move(inner));
  }

  switch (element_type.Id()) {
    case ElementTypeId::kChar: return std::make_shared<arrow::UInt16Type>();
    case ElementTypeId::kInt8: return std::make_shared<arrow::Int8Type>();
    case ElementTypeId::kInt16: return std::make_shared<arrow::Int16Type>();
    case ElementTypeId::kInt32: return std::make_shared<arrow::Int32Type>();
    case ElementTypeId::kInt64: return std::make_shared<arrow::Int64Type>();
    case ElementTypeId::kFloat: return std::make_shared<arrow::FloatType>();
    case ElementTypeId::kDouble: return std::make_shared<arrow::DoubleType>();
    case ElementTypeId::kBool: return std::make_shared<arrow::BooleanType>();
    case ElementTypeId::kString: return std::make_shared<arrow::StringType>();
    case ElementTypeId::kTimestamp: return std::make_shared<arrow::TimestampType>(
        arrow::TimeUnit::NANO, "UTC");
    case ElementTypeId::kLocalDate: return std::make_shared<arrow::Date64Type>();
    case ElementTypeId::kLocalTime: return std::make_shared<arrow::Time64Type>(arrow::TimeUnit::NANO);

    default: {
      auto message = fmt::format("Unexpected element_type_id {}",
          static_cast<int>(element_type.Id()));
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
}

std::shared_ptr<Schema> ArrowUtil::MakeDeephavenSchema(const arrow::Schema &schema) {
  const auto &fields = schema.fields();
  auto names = MakeReservedVector<std::string>(fields.size());
  auto types = MakeReservedVector<ElementType>(fields.size());
  for (const auto &f: fields) {
    auto type_id = ArrowUtil::GetElementType(*f->type(), true);
    names.push_back(f->name());
    types.push_back(*type_id);
  }
  return Schema::Create(std::move(names), std::move(types));
}

std::shared_ptr<arrow::Table> ArrowUtil::MakeArrowTable(const ClientTable &client_table) {
  auto ncols = client_table.NumColumns();
  auto nrows = client_table.NumRows();
  auto arrays = MakeReservedVector<std::shared_ptr<arrow::Array>>(ncols);

  for (size_t i = 0; i != ncols; ++i) {
    auto column_source = client_table.GetColumn(i);
    auto arrow_array = ArrowArrayConverter::ColumnSourceToArray(*column_source, nrows);
    arrays.emplace_back(std::move(arrow_array));
  }

  auto schema = MakeArrowSchema(*client_table.Schema());

  return arrow::Table::Make(std::move(schema), arrays);
}

std::shared_ptr<arrow::Schema> ArrowUtil::MakeArrowSchema(
    const deephaven::dhcore::clienttable::Schema &dh_schema) {
  arrow::SchemaBuilder builder;
  for (int32_t i = 0; i != dh_schema.NumCols(); ++i) {
    const auto &name = dh_schema.Names()[i];
    auto element_type = dh_schema.ElementTypes()[i];
    auto arrow_type = GetArrowType(element_type);
    auto field = std::make_shared<arrow::Field>(name, std::move(arrow_type));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder.AddField(field)));
  }
  return ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(builder.Finish()));
}
}  // namespace deephaven::client::utility
