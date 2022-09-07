/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <memory>
#include <vector>
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/immerutil/abstract_flex_vector.h"
#include "deephaven/client/subscription/space_mapper.h"
#include "deephaven/client/table/table.h"
#include "deephaven/client/utility/misc.h"

namespace deephaven::client::subscription {
class ImmerTableState final {
  typedef deephaven::client::column::ColumnSource ColumnSource;
  typedef deephaven::client::container::RowSequence RowSequence;
  typedef deephaven::client::immerutil::AbstractFlexVectorBase AbstractFlexVectorBase;
  typedef deephaven::client::table::Schema Schema;
  typedef deephaven::client::table::Table Table;
  typedef deephaven::client::utility::ColumnDefinitions ColumnDefinitions;

public:
  explicit ImmerTableState(std::shared_ptr<ColumnDefinitions> colDefs);
  ~ImmerTableState();

  std::shared_ptr<RowSequence> addKeys(const RowSequence &rowsToAddKeySpace);
  void addData(const std::vector<std::shared_ptr<arrow::Array>> &data,
      const RowSequence &rowsToAddIndexSpace);

  std::shared_ptr<RowSequence> erase(const RowSequence &rowsToRemoveKeySpace);

  std::vector<std::shared_ptr<RowSequence>> modifyKeys(
      const std::vector<std::shared_ptr<RowSequence>> &rowsToModifyKeySpace);
  void modifyData(const std::vector<std::shared_ptr<arrow::Array>> &data,
      const std::vector<std::shared_ptr<RowSequence>> &rowsToModifyIndexSpace);

  void applyShifts(const RowSequence &startIndex, const RowSequence &endIndex,
      const RowSequence &destIndex);

  std::shared_ptr<Table> snapshot() const;

private:
  void modifyColumn(size_t colNum,
      std::unique_ptr<AbstractFlexVectorBase> modifiedData,
      const RowSequence &rowsToModifyKeySpace);

  std::shared_ptr<ColumnDefinitions> colDefs_;
  std::shared_ptr<Schema> schema_;
  std::vector<std::unique_ptr<AbstractFlexVectorBase>> flexVectors_;
  // Keeps track of keyspace -> index space mapping
  SpaceMapper spaceMapper_;
};
}  // namespace deephaven::client::subscription
