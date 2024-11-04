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
   * @param rows_to_add_key_space Keys to add, represented in key space
   * @return Added keys, represented in index space
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> AddKeys(const RowSequence &rows_to_add_key_space);

  /**
   * For each column i, insert the interval of data [begins[i], ends[i]) taken from the column source
   * sources[i], into the table at the index space positions indicated by 'rowsToAddIndexSpace'.
   * </summary>
   * @param sources The ColumnSources
   * @param begins The array of start indices (inclusive) for each column
   * @param ends The array of end indices (exclusive) for each column
   * @param rows_to_add_index_space Index space positions where the data should be inserted
   */
  void AddData(const std::vector<std::shared_ptr<ColumnSource>> &sources, const std::vector<size_t> &begins,
               const std::vector<size_t> &ends, const RowSequence &rows_to_add_index_space);

  /**
   * Erases the data at the positions in 'rowsToEraseKeySpace'.
   * @param rowsToEraseKeySpace The keys, represented in key space, to erase
   * @return The keys, represented in index space, that were erased
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> Erase(const RowSequence &rows_to_erase_key_space);

  /**
   * Converts a RowSequence of keys represented in key space to a RowSequence of keys represented in index space.
   * It is an error to try to use a key that is not already in the map.
   * @param keys_row_space Keys represented in key space
   * @return Keys represented in index space
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> ConvertKeysToIndices(const RowSequence &keys_row_space) const;


  /**
   * Modifies column 'col_num' with the contiguous data sourced in 'src'
   * at the half-open interval [begin, end), to be stored in the destination
   * at the positions indicated by 'rows_to_modify_index_space'.
   * @param col_num Index of the column to be modified
   * @param src A ColumnSource containing the source data
   * @param begin The start of the source range
   * @param end One past the end of the source range
   * @param rows_to_modify_index_space The positions to be modified in the destination,
   * represented in index space.
   */
  void ModifyData(size_t col_num, const ColumnSource &src, size_t begin, size_t end,
      const RowSequence &rows_to_modify_index_space);

  /**
   * Applies shifts to the keys in key space. This does not affect the ordering of the keys,
   * nor will it cause keys to overlap with other keys. Logically there is a set of tuples
   * (first_key, last_key, dest_key) which is to be interpreted as take all the existing keys
   * in the *closed* range [first_key, last_key] and move them to the range starting at dest_key.
   * These tuples have been "transposed" into three different RowSequence data structures
   * for possible better compression.
   * @param first_index The RowSequence containing the begin_keys
   * @param last_index The RowSequence containing the end_keys
   * @param dest_index The RowSequence containing the dest_indexes
   */
  void ApplyShifts(const RowSequence &first_index, const RowSequence &last_index,
      const RowSequence &dest_index);

  /**
   * Takes a snapshot of the current table state
   * @return A ClientTable representing the current table state
   */
  [[nodiscard]]
  std::shared_ptr<ClientTable> Snapshot() const;

private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::unique_ptr<AbstractFlexVectorBase>> flexVectors_;
  // Keeps track of keyspace -> index space mapping
  SpaceMapper spaceMapper_;
};
}  // namespace deephaven::dhcore::ticking
