/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/arrowutil/arrow_client_table.h"
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <arrow/table.h>
#include "deephaven/client/arrowutil/arrow_array_converter.h"
#include "deephaven/client/arrowutil/arrow_column_source.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/column/array_column_source.h"
#include "deephaven/dhcore/container/container.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"

using deephaven::client::arrowutil::ArrowArrayConverter;
using deephaven::client::arrowutil::BooleanArrowColumnSource;
using deephaven::client::arrowutil::CharArrowColumnSource;
using deephaven::client::arrowutil::DateTimeArrowColumnSource;
using deephaven::client::arrowutil::DoubleArrowColumnSource;
using deephaven::client::arrowutil::FloatArrowColumnSource;
using deephaven::client::arrowutil::Int8ArrowColumnSource;
using deephaven::client::arrowutil::Int16ArrowColumnSource;
using deephaven::client::arrowutil::Int32ArrowColumnSource;
using deephaven::client::arrowutil::Int64ArrowColumnSource;
using deephaven::client::arrowutil::StringArrowColumnSource;
using deephaven::client::utility::ArrowUtil;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::ValueOrThrow;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::column::ContainerArrayColumnSource;
using deephaven::dhcore::container::Container;
using deephaven::dhcore::container::ContainerBase;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::DeephavenTraits;
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::dhcore::utility::VerboseCast;

namespace deephaven::client::arrowutil {

std::shared_ptr<ClientTable> ArrowClientTable::Create(std::shared_ptr<arrow::Table> arrow_table) {
  auto schema = ArrowUtil::MakeDeephavenSchema(*arrow_table->schema());
  auto row_sequence = RowSequence::CreateSequential(0, arrow_table->num_rows());

  auto column_sources = MakeReservedVector<std::shared_ptr<ColumnSource>>(arrow_table->num_columns());
  for (const auto &chunked_array : arrow_table->columns()) {
    auto cs = ArrowArrayConverter::ChunkedArrayToColumnSource(chunked_array);
    column_sources.push_back(std::move(cs));
  }

  return std::make_shared<ArrowClientTable>(Private(), std::move(arrow_table),
      std::move(schema), std::move(row_sequence), std::move(column_sources));
}

ArrowClientTable::ArrowClientTable(deephaven::client::arrowutil::ArrowClientTable::Private,
    std::shared_ptr<arrow::Table> arrow_table, std::shared_ptr<SchemaType> schema,
    std::shared_ptr<RowSequence> row_sequence,
    std::vector<std::shared_ptr<ColumnSource>> column_sources) :
    arrow_table_(std::move(arrow_table)), schema_(std::move(schema)),
    row_sequence_(std::move(row_sequence)), column_sources_(std::move(column_sources)) {}
ArrowClientTable::ArrowClientTable(ArrowClientTable &&other) noexcept = default;
ArrowClientTable &ArrowClientTable::operator=(ArrowClientTable &&other) noexcept = default;
ArrowClientTable::~ArrowClientTable() = default;

std::shared_ptr<ColumnSource> ArrowClientTable::GetColumn(size_t column_index) const {
  if (column_index >= column_sources_.size()) {
    auto message = fmt::format("column_index ({}) >= num columns ({})", column_index,
        column_sources_.size());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  return column_sources_[column_index];
}
}  // namespace deephaven::client::arrowutil
