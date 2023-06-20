/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <memory>
#include <vector>
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/immerutil/abstract_flex_vector.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/ticking/space_mapper.h"

namespace deephaven::dhcore::ticking {
class ImmerTableState final {
  typedef deephaven::dhcore::chunk::AnyChunk AnyChunk;
  typedef deephaven::dhcore::chunk::Chunk Chunk;
  typedef deephaven::dhcore::column::ColumnSource ColumnSource;
  typedef deephaven::dhcore::container::RowSequence RowSequence;
  typedef deephaven::dhcore::immerutil::AbstractFlexVectorBase AbstractFlexVectorBase;
  typedef deephaven::dhcore::clienttable::ClientTable ClientTable;
  typedef deephaven::dhcore::clienttable::Schema Schema;

public:
  explicit ImmerTableState(std::shared_ptr<Schema> schema);
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

  void addData(const std::vector<std::shared_ptr<ColumnSource>> &src, const std::vector<size_t> &begins,
      const std::vector<size_t> &ends, const RowSequence &rowsToAddIndexSpace);

  std::shared_ptr<RowSequence> erase(const RowSequence &rowsToRemoveKeySpace);

  /**
   * When the caller wants to modify data in the ImmerTableState, they do it in two steps:
   * modifyKeys and then modifyData. First, they call convertKeysToIndices, which gives them the
   * (key space) -> (position space) mapping. Then, the caller calls modifyData (perhaps all at
   * once, perhaps in slices) to fill in the data. Note that modifyKey/modifyData only support
   * modifying rows. It is an error to try to use a key that is not already in the map.
   */
  std::shared_ptr<RowSequence> convertKeysToIndices(const RowSequence &keysRowSpace) const;

  void modifyData(size_t colNum, const ColumnSource &src, size_t begin, size_t end,
      const RowSequence &rowsToModify);

  void applyShifts(const RowSequence &startIndex, const RowSequence &endIndex,
      const RowSequence &destIndex);

  std::shared_ptr<ClientTable> snapshot() const;

private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::unique_ptr<AbstractFlexVectorBase>> flexVectors_;
  // Keeps track of keyspace -> index space mapping
  SpaceMapper spaceMapper_;
};
}  // namespace deephaven::dhcore::ticking
