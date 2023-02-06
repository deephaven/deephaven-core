/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/space_mapper.h"

#include <optional>
#include "deephaven/client/utility/utility.h"

using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;
using deephaven::client::utility::separatedList;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven::client::subscription {
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

  friend bool operator!=(const SimpleRangeIterator &lhs, const SimpleRangeIterator &rhs) {
    return lhs.value_ != rhs.value_;
  }

  uint64_t value_;
};
}
SpaceMapper::SpaceMapper() = default;
SpaceMapper::~SpaceMapper() = default;

uint64_t SpaceMapper::addRange(uint64_t beginKey, uint64_t endKey) {
  roaring::Roaring64Map x;
  auto size = endKey - beginKey;
  auto initialSize = set_.cardinality();
  set_.addRange(beginKey, endKey);
  if (set_.cardinality() != initialSize + size) {
    auto message = stringf("Some elements of [%o,%o) were already in the set", beginKey,
        endKey);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
  return zeroBasedRank(beginKey);
}

uint64_t SpaceMapper::eraseRange(uint64_t beginKey, uint64_t endKey) {
  auto result = zeroBasedRank(beginKey);
  set_.removeRange(beginKey, endKey);
  return result;
}

void SpaceMapper::applyShift(uint64_t beginKey, uint64_t endKey, uint64_t destKey) {
  auto size = endKey - beginKey;
  set_.removeRange(beginKey, endKey);
  set_.addRange(destKey, destKey + size);
}

std::shared_ptr<RowSequence> SpaceMapper::addKeys(const RowSequence &keys) {
  RowSequenceBuilder builder;
  auto addInterval = [this, &builder](uint64_t beginKey, uint64_t endKey) {
    auto size = endKey - beginKey;
    auto beginIndex = addRange(beginKey, endKey);
    builder.addInterval(beginIndex, beginIndex + size);
  };
  keys.forEachInterval(addInterval);
  return builder.build();
}

std::shared_ptr<RowSequence> SpaceMapper::convertKeysToIndices(const RowSequence &keys) const {
  if (keys.empty()) {
    return RowSequence::createEmpty();
  }

  RowSequenceBuilder builder;
  auto convertInterval = [this, &builder](uint64_t beginKey, uint64_t endKey) {
    auto beginp = set_.begin();
    if (!beginp.move(beginKey)) {
      auto message = stringf("begin key %o is not in the src map", beginKey);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
    auto nextRank = zeroBasedRank(beginKey);
    // Confirm we have entries for everything in the range.
    auto currentp = beginp;
    for (auto currentKey = beginKey; currentKey != endKey; ++currentKey) {
      if (currentKey != *currentp) {
        auto message = stringf("current key %o is in not the src map", currentKey);
        throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
      }
      ++currentp;
    }
    auto size = endKey - beginKey;
    builder.addInterval(nextRank, nextRank + size);
  };
  keys.forEachInterval(convertInterval);
  return builder.build();
}

uint64_t SpaceMapper::zeroBasedRank(uint64_t value) const {
  // Roaring's convention for rank is to "Return the number of integers that are smaller or equal to x".
  // But we would rather know the number of values that are strictly smaller than x.
  auto result = set_.rank(value);
  // Adjust if 'value' is in the set.
  return set_.contains(value) ? result - 1 : result;
}
}  // namespace deephaven::client::subscription
