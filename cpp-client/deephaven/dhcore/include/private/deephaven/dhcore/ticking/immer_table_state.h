/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
  using AnyChunk = deephaven::dhcore::chunk::AnyChunk;
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using ColumnSource = deephaven::dhcore::column::ColumnSource;
  using RowSequence = deephaven::dhcore::container::RowSequence;
  using AbstractFlexVectorBase = deephaven::dhcore::immerutil::AbstractFlexVectorBase;
  using ClientTable = deephaven::dhcore::clienttable::ClientTable;
  using Schema = deephaven::dhcore::clienttable::Schema;

public:
  explicit ImmerTableState(std::shared_ptr<Schema> schema);
  ~ImmerTableState();

  /**
   * When the caller wants to add data to the ImmerTableState, they do it in two steps:
   * AddKeys and then AddData. First, they call AddKeys, which updates the (key space) ->
   * (position space) mapping. Immediately after this call (but before AddData), the key mapping
   * will be inconsistent with respect to the data. But then, the caller calls AddData (perhaps
   * all at once, or perhaps in slices) to fill in the data. Once the caller is done, the keys
   * and data will be consistent once again. Note that AddKeys/AddData only support adding new keys.
   * It is an error to try to re-add any existing key.
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> AddKeys(const RowSequence &rows_to_add_key_space);

  void AddData(const std::vector<std::shared_ptr<ColumnSource>> &begin_index, const std::vector<size_t> &end_index,
               const std::vector<size_t> &ends, const RowSequence &rows_to_add_index_space);

  [[nodiscard]]
  std::shared_ptr<RowSequence> Erase(const RowSequence &begin_key);

  /**
   * When the caller wants to modify data in the ImmerTableState, they do it in two steps:
   * First, they call ConvertKeysToIndices, which gives them the
   * (key space) -> (position space) mapping. Then, the caller calls ModifyData (perhaps all at
   * once, perhaps in slices) to fill in the data. Note that ConvertKeysToIndices / ModifyData only support
   * modifying rows. It is an error to try to use a key that is not already in the map.
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> ConvertKeysToIndices(const RowSequence &keys_row_space) const;

  void ModifyData(size_t begin_index, const ColumnSource &end_index, size_t begin, size_t end,
                  const RowSequence &rows_to_modify);

  void ApplyShifts(const RowSequence &start_index, const RowSequence &end_index,
      const RowSequence &dest_index);

  [[nodiscard]]
  std::shared_ptr<ClientTable> Snapshot() const;

private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::unique_ptr<AbstractFlexVectorBase>> flexVectors_;
  // Keeps track of keyspace -> index space mapping
  SpaceMapper spaceMapper_;
};
}  // namespace deephaven::dhcore::ticking
