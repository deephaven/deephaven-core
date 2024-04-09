/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <string>
#include <cstdint>
#include <arrow/array.h>
#include <arrow/table.h>
#include "deephaven/client/arrowutil/arrow_value_converter.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/types.h"

namespace deephaven::client::arrowutil {
class ArrowClientTable final : public deephaven::dhcore::clienttable::ClientTable {
  struct Private {};
  using ClientTable = deephaven::dhcore::clienttable::ClientTable;
  using SchemaType = deephaven::dhcore::clienttable::Schema;
public:
  static std::shared_ptr<ClientTable> Create(std::shared_ptr<arrow::Table> arrow_table);

  ArrowClientTable(Private, std::shared_ptr<arrow::Table> arrow_table,
      std::shared_ptr<SchemaType> schema, std::shared_ptr<RowSequence> row_sequence,
      std::vector<std::shared_ptr<ColumnSource>> column_sources);
  ArrowClientTable(ArrowClientTable &&other) noexcept;
  ArrowClientTable &operator=(ArrowClientTable &&other) noexcept;
  ~ArrowClientTable() final;

  [[nodiscard]]
  std::shared_ptr<RowSequence> GetRowSequence() const final {
    return row_sequence_;
  }

  std::shared_ptr<ColumnSource> GetColumn(size_t column_index) const final;

  [[nodiscard]]
  size_t NumRows() const final {
    return arrow_table_->num_rows();
  }

  [[nodiscard]]
  size_t NumColumns() const final {
    return arrow_table_->num_columns();
  }

  [[nodiscard]]
  std::shared_ptr<SchemaType> Schema() const final {
    return schema_;
  }

private:
  std::shared_ptr<arrow::Table> arrow_table_;
  std::shared_ptr<SchemaType> schema_;
  std::shared_ptr<RowSequence> row_sequence_;
  std::vector<std::shared_ptr<ColumnSource>> column_sources_;
};
}  // namespace deephaven::client::arrowutil
