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

  /**
   * Adds the keys in the half-open interval [begin_key, end_key_) to the set.
   * The keys must not already exist in the set. If they do, an exception is thrown.
   * @param begin_key The first key to insert
   * @param end_key One past the last key to insert
   * @return The rank of begin_key
   */
  [[nodiscard]]
  uint64_t AddRange(uint64_t begin_key, uint64_t end_key);


  /**
   * Removes the keys in the half-open interval [begin_key, end_key_) from the set.
   * It is ok if some or all of the keys do not exist in the set.
   * @param begin_key The first key to insert
   * @param end_key One past the last key to insert
   * @return The rank of begin_key, prior to the deletion
   */
  [[nodiscard]]
  uint64_t EraseRange(uint64_t begin_key, uint64_t end_key);

  /**
   * Delete all the keys that currently exist in the range [begin_key, end_key).
   * Call that set of deleted keys K. The cardinality of K might be smaller than
   * (end_key - begin_key) because not all keys in that range are expected to be present.
   *
   * Calculate a new set of keys KNew = {k ∈ K | (k - begin_key + dest_key)}
   * and insert this new set of keys into the map.
   *
   * This has the effect of offsetting all the existing keys by (dest_key - begin_key)
   * @param begin_key The start of the range of keys
   * @param end_key One past the end of the range of keys
   * @param dest_key The start of the target range to move keys to.
   */
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
   * @param keys The keys to add (represented in key space)
   * @return The added keys (represented in position space)
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> AddKeys(const RowSequence &keys);
  /**
   * Looks up 'keys' (specified in key space) in the map, and returns the positions (in position
   * space) of those keys.
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> ConvertKeysToIndices(const RowSequence &keys) const;

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
