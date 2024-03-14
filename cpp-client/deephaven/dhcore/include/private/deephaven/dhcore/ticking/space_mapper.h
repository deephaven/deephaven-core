/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdlib>
#include "deephaven/dhcore/container/row_sequence.h"
#include "roaring/roaring.hh"

namespace deephaven::dhcore::ticking {
class SpaceMapper {
  using RowSequence = deephaven::dhcore::container::RowSequence;

public:
  SpaceMapper();
  ~SpaceMapper();

  [[nodiscard]]
  uint64_t AddRange(uint64_t begin_key, uint64_t end_key);
  [[nodiscard]]
  uint64_t EraseRange(uint64_t begin_key, uint64_t end_key);
  void ApplyShift(uint64_t begin_key, uint64_t end_key, uint64_t dest_key);

  /**
   * Adds 'keys' (specified in key space) to the map, and returns the positions (in position
   * space) of those keys after insertion. 'keys' are required to not already been in the map.
   * The algorithm behaves as though all the keys are inserted in the map and then
   * 'ConvertKeysToIndices' is called.
   *
   * Example:
   *   SpaceMapper currently holds [100 300]
   *   AddKeys called with [1, 2, 200, 201, 400, 401]
   *   SpaceMapper final state is [1, 2, 100, 200, 201, 300, 400, 401]
   *   The returned result is [0, 1, 3, 4, 6, 7]
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> AddKeys(const RowSequence &begin_key);
  /**
   * Looks up 'keys' (specified in key space) in the map, and returns the positions (in position
   * space) of those keys.
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> ConvertKeysToIndices(const RowSequence &begin_key) const;

  /**
   * Note: this call iterates over the Roaring64Map and is not constant-time.
   */
  [[nodiscard]]
  size_t Cardinality() const {
    return set_.cardinality();
  }

  [[nodiscard]]
  uint64_t ZeroBasedRank(uint64_t value) const;

private:
  roaring::Roaring64Map set_;
};
}  // namespace deephaven::dhcore::ticking
