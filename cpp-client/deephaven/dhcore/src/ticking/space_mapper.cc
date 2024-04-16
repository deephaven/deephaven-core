/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/ticking/space_mapper.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"

using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::dhcore::utility::separatedList;

namespace deephaven::dhcore::ticking {
SpaceMapper::SpaceMapper() = default;
SpaceMapper::~SpaceMapper() = default;

uint64_t SpaceMapper::AddRange(uint64_t begin_key, uint64_t end_key) {
  roaring::Roaring64Map x;
  auto size = end_key - begin_key;
  auto initial_size = set_.cardinality();
  set_.addRange(begin_key, end_key);
  if (set_.cardinality() != initial_size + size) {
    auto message = fmt::format("Some elements of [{},{}) were already in the set", begin_key,
        end_key);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  return ZeroBasedRank(begin_key);
}

uint64_t SpaceMapper::EraseRange(uint64_t begin_key, uint64_t end_key) {
  auto result = ZeroBasedRank(begin_key);
  set_.removeRange(begin_key, end_key);
  return result;
}

void SpaceMapper::ApplyShift(uint64_t begin_key, uint64_t end_key, uint64_t dest_key) {
  // Shifts do not change the size of the set. So, note the original size as a sanity check.
  auto original_size = set_.cardinality();

  // Note that [begin_key, end_key) is potentially a superset of the keys we have.
  // We need to remove all our keys in the range [begin_key, end_key),
  // and then, for each key k that we removed, add a new key (k - begin_key + dest_key).

  // As we scan the keys in our set, we build this vector which contains contiguous ranges.
  std::vector<std::pair<uint64_t, uint64_t>> new_ranges;
  auto it = set_.begin();
  if (!it.move(begin_key)) {
    // begin_key is bigger than any key in our set, so the shift request has no effect.
    return;
  }

  while (it != set_.end() && *it < end_key) {
    auto offset = *it - begin_key;
    auto new_key = dest_key + offset;
    if (!new_ranges.empty() && new_ranges.back().second == new_key) {
      // This key is contiguous with the last range, so extend it by one.
      ++new_ranges.back().second;
    } else {
      // This key is not contiguous with the last range (or there is no last range), so
      // start a new range here having size 1.
      new_ranges.emplace_back(new_key, new_key + 1);
    }
    ++it;
  }

  set_.removeRange(begin_key, end_key);
  for (const auto &range : new_ranges) {
    set_.addRange(range.first, range.second);
  }

  // Sanity check.
  auto final_size = set_.cardinality();
  if (original_size != final_size) {
    auto message = fmt::format("Unexpected rowkey size change: from {} to {}", original_size,
        final_size);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
}

std::shared_ptr<RowSequence> SpaceMapper::AddKeys(const RowSequence &keys) {
  RowSequenceBuilder builder;
  auto add_interval = [this, &builder](uint64_t begin_key, uint64_t end_key) {
    auto size = end_key - begin_key;
    auto begin_index = AddRange(begin_key, end_key);
    builder.AddInterval(begin_index, begin_index + size);
  };
  keys.ForEachInterval(add_interval);
  return builder.Build();
}

std::shared_ptr<RowSequence> SpaceMapper::ConvertKeysToIndices(const RowSequence &keys) const {
  if (keys.Empty()) {
    return RowSequence::CreateEmpty();
  }

  RowSequenceBuilder builder;
  auto convert_interval = [this, &builder](uint64_t begin_key, uint64_t end_key) {
    auto beginp = set_.begin();
    if (!beginp.move(begin_key)) {
      auto message = fmt::format("begin key {} is too large for the src map", begin_key);
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
    auto next_rank = ZeroBasedRank(begin_key);
    // Confirm we have entries for everything in the range.
    auto currentp = beginp;
    for (auto current_key = begin_key; current_key != end_key; ++current_key) {
      if (current_key != *currentp) {
        auto message = fmt::format("Current key {} is in not the src map", current_key);
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
      }
      ++currentp;
    }
    // It is ok to add a chunk like this because rowkeys [begin_key, end_key) are contiguous;
    // therefore their corresponding index space indices are also contiguous.
    auto size = end_key - begin_key;
    builder.AddInterval(next_rank, next_rank + size);
  };
  keys.ForEachInterval(convert_interval);
  return builder.Build();
}

uint64_t SpaceMapper::ZeroBasedRank(uint64_t value) const {
  // Roaring's convention for rank is to "Return the number of integers that are smaller or equal to x".
  // But we would rather know the number of values that are strictly smaller than x.
  auto result = set_.rank(value);
  // Adjust if 'value' is in the set.
  return set_.contains(value) ? result - 1 : result;
}
}  // namespace deephaven::dhcore::ticking

