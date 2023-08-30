/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/ticking/space_mapper.h"

#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;

namespace deephaven::dhcore::ticking {
namespace {
// We make an "iterator" that refers to a point in a numeric range.
// This is useful because we can use the "range" version of boost::multiset::insert, which
// uses hints internally and should be somewhat faster for inserting contiguous values.
struct SimpleRangeIterator {
  explicit SimpleRangeIterator(uint64_t value) : value_(value) {}

  uint64_t operator*() const { return value_; }

  SimpleRangeIterator &operator++() {
    ++value_;
    return *this;
  }

  uint64_t value_;
};
}
SpaceMapper::SpaceMapper() = default;
SpaceMapper::~SpaceMapper() = default;

uint64_t SpaceMapper::AddRange(uint64_t begin_key, uint64_t end_key) {
  roaring::Roaring64Map x;
  auto size = end_key - begin_key;
  auto initial_size = set_.cardinality();
  set_.addRange(begin_key, end_key);
  if (set_.cardinality() != initial_size + size) {
    auto message = Stringf("Some elements of [%o,%o) were already in the set", begin_key,
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
  auto size = end_key - begin_key;
  set_.removeRange(begin_key, end_key);
  set_.addRange(dest_key, dest_key + size);
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
      auto message = Stringf("begin key %o is not in the src map", begin_key);
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
    auto next_rank = ZeroBasedRank(begin_key);
    // Confirm we have entries for everything in the range.
    auto currentp = beginp;
    for (auto current_key = begin_key; current_key != end_key; ++current_key) {
      if (current_key != *currentp) {
        auto message = Stringf("Current key %o is in not the src map", current_key);
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
      }
      ++currentp;
    }
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

