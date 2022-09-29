/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include "deephaven/client/container/row_sequence.h"
#include "roaring/roaring.hh"

namespace deephaven::client::subscription {
class SpaceMapper {
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  SpaceMapper();
  ~SpaceMapper();

  uint64_t addRange(uint64_t beginKey, uint64_t endKey);
  uint64_t eraseRange(uint64_t beginKey, uint64_t endKey);
  void applyShift(uint64_t beginKey, uint64_t endKey, uint64_t destKey);

  /**
   * Adds 'keys' (specified in key space) to the map, and returns the positions (in position
   * space) of those keys after insertion. 'keys' are required to not already been in the map.
   * The algorithm behaves as though all the keys are inserted in the map and then
   * 'convertKeysToIndices' is called.
   *
   * Example:
   *   SpaceMapper currently holds [100 300]
   *   addKeys called with [1, 2, 200, 201, 400, 401]
   *   SpaceMapper final state is [1, 2, 100, 200, 201, 300, 400, 401]
   *   The returned result is [0, 1, 3, 4, 6, 7]
   */
  std::shared_ptr<RowSequence> addKeys(const RowSequence &keys);
  /**
   * Looks up 'keys' (specified in key space) in the map, and returns the positions (in position
   * space) of those keys.
   */
  std::shared_ptr<RowSequence> convertKeysToIndices(const RowSequence &keys) const;

  /**
   * Note: this call iterates over the Roaring64Map and is not constant-time.
   */
  size_t cardinality() const {
    return set_.cardinality();
  }

  uint64_t zeroBasedRank(uint64_t value) const;

private:
  roaring::Roaring64Map set_;
};
}  // namespace deephaven::client::subscription
