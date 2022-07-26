/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/space_mapper.h"
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
  auto size = endKey - beginKey;
  if (size == 0) {
    return 0;  // arbitrary
  }
  auto initialSize = set_.size();
  set_.insert(SimpleRangeIterator(beginKey), SimpleRangeIterator(endKey));
  if (set_.size() != initialSize + size) {
    throw std::runtime_error(stringf("Some elements of [%o,%o) were already in the set", beginKey,
        endKey));
  }
  return set_.find_rank(beginKey);
}

uint64_t SpaceMapper::eraseRange(uint64_t beginKey, uint64_t endKey) {
  size_t size = endKey - beginKey;
  if (size == 0) {
    return 0;  // arbitrary
  }
  auto ip = set_.find(beginKey);
  // This is ok (I think) even in the not-found case because set_.rank(set_.end()) is defined, and
  // is set_.size(). The not-found case will throw an exception shortly in the test inside the loop.
  auto result = set_.rank(ip);
  for (auto current = beginKey; current != endKey; ++current) {
    if (ip == set_.end() || *ip != current) {
      throw std::runtime_error(stringf("key %o was not in the set", current));
    }
    ip = set_.erase(ip);
  }
  return result;
}

void SpaceMapper::applyShift(uint64_t beginKey, uint64_t endKey, uint64_t destKey) {
  if (beginKey == endKey) {
    return;
  }
  if (destKey > beginKey) {
    auto amountToAdd = destKey - beginKey;
    // positive shift: work backwards
    auto ip = set_.lower_bound(endKey);
    // ip is the first element >= endKey, or it is end()
    if (ip == set_.begin()) {
      return;
    }
    --ip;
    // ip is the last element < endKey
    while (true) {
      if (*ip < beginKey) {
        // exceeded range
        return;
      }
      std::optional<decltype(ip)> prev;
      if (ip != set_.begin()) {
        prev = std::prev(ip);
      }
      auto node = set_.extract(ip);
      node.value() = node.value() + amountToAdd;
      set_.insert(std::move(node));
      if (!prev.has_value()) {
        return;
      }
      ip = *prev;
    }
    return;
  }

  // destKey <= beginKey, shifts are negative, so work in the forward direction
  auto amountToSubtract = beginKey - destKey;
  // negative shift: work forwards
  auto ip = set_.lower_bound(beginKey);
  // ip == end, or the first element >= beginKey
  while (true) {
    if (ip == set_.end() || *ip >= endKey) {
      return;
    }
    auto nextp = std::next(ip);
    auto node = set_.extract(ip);
    node.value() = node.value() - amountToSubtract;
    set_.insert(std::move(node));
    ip = nextp;
  }
}

std::shared_ptr<RowSequence> SpaceMapper::addKeys(const RowSequence &keys) {
  RowSequenceBuilder builder;
  auto addChunk = [this, &builder](uint64_t beginKey, uint64_t endKey) {
    auto size = endKey - beginKey;
    auto beginIndex = addRange(beginKey, endKey);
    builder.addRange(beginIndex, beginIndex + size);
  };
  keys.forEachChunk(addChunk);
  return builder.build();
}

std::shared_ptr<RowSequence> SpaceMapper::convertKeysToIndices(const RowSequence &keys) const {
  RowSequenceBuilder builder;
  auto convertChunk = [this, &builder](uint64_t begin, uint64_t end) {
    auto beginp = set_.find(begin);
    if (beginp == set_.end()) {
      throw std::runtime_error(stringf("begin key %o is not in the src map", begin));
    }
    auto nextRank = set_.rank(beginp);
    // Confirm we have entries for everything in the range.
    auto currentp = beginp;
    for (auto current = begin; current != end; ++current) {
      if (current != *currentp) {
        throw std::runtime_error(stringf("current key %o is in not the src map", begin));
      }
      ++currentp;
    }
    auto size = end - begin;
    builder.addRange(nextRank, nextRank + size);
  };
  keys.forEachChunk(convertChunk);
  return builder.build();
}
}  // namespace deephaven::client::subscription
