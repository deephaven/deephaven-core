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

  /**
   * When the caller wants to add data to the ImmerTableState, they do it in two steps:
   * addKeys and then addData. First, they call addKeys, which updates the (key space) ->
   * (position space) mapping. Immediately after this call (but before addData), the key mapping
   * will be inconsistent with respect to the data. But then, the caller calls addData (perhaps
   * all at once, or perhaps in slices) to fill in the data. Once the caller is done, the keys
   * and data will be consistent once again. Note that addKey/addData only support adding new keys.
   * It is an error to try to re-add any existing key.
   */
  std::shared_ptr<RowSequence> addKeys(const RowSequence &rowsToAddKeySpace);
  void addData(const std::vector<std::shared_ptr<arrow::Array>> &data,
      size_t dataRowOffset, const RowSequence &rowsToAddIndexSpace);

  std::shared_ptr<RowSequence> erase(const RowSequence &rowsToRemoveKeySpace);

  /**
   * When the caller wants to modify data in the ImmerTableState, they do it in two steps:
   * modifyKeys and then modifyData. First, they call convertKeysToIndices, which gives them the
   * (key space) -> (position space) mapping. Then, the caller calls modifyData (perhaps all at
   * once, perhaps in slices) to fill in the data. Note that modifyKey/modifyData only support
   * modifying rows. It is an error to try to use a key that is not already in the map.
   */
  std::shared_ptr<RowSequence> convertKeysToIndices(const RowSequence &keysRowSpace) const;

  void modifyData(size_t colNum, const arrow::Array &data, size_t srcOffset,
      const RowSequence &rowsToModify);

  void applyShifts(const RowSequence &startIndex, const RowSequence &endIndex,
      const RowSequence &destIndex);

  std::shared_ptr<Table> snapshot() const;

private:
  std::shared_ptr<ColumnDefinitions> colDefs_;
  std::shared_ptr<Schema> schema_;
  std::vector<std::unique_ptr<AbstractFlexVectorBase>> flexVectors_;
  // Keeps track of keyspace -> index space mapping
  SpaceMapper spaceMapper_;
};
}  // namespace deephaven::client::subscription
