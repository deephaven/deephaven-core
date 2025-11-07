/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <set>

#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ostream.h"

namespace deephaven::dhcore::container {
class RowSequenceIterator;

/**
 * Represets a monotonically increasing sequence of row numbers. The coordinate space of those
 * row numbers (key space vs index space) is unspecified here.
 */
class RowSequence {
public:
  /**
   * Create an empty RowSequence.
   */
  [[nodiscard]]
  static std::shared_ptr<RowSequence> CreateEmpty();
  /**
   * Create a dense RowSequence providing values from the half open interval [begin, end).
   */
  [[nodiscard]]
  static std::shared_ptr<RowSequence> CreateSequential(uint64_t begin, uint64_t end);

  /**
   * Destructor.
   */
  virtual ~RowSequence();

  /**
   * Create a RowSequenceIterator.
   */
  [[nodiscard]]
  RowSequenceIterator GetRowSequenceIterator() const;

  /**
   * Return a RowSequence consisting of the first 'Size' elements of this RowSequence.
   * If Size >= this->Size(), the method may return this RowSequence.
   */
  [[nodiscard]]
  virtual std::shared_ptr<RowSequence> Take(size_t size) const = 0;
  /**
   * Return a RowSequence consisting of the remaining elements of this RowSequence after dropping
   * 'size' elements. If Size >= this->Size(), returns the empty RowSequence.
   */
  [[nodiscard]]
  virtual std::shared_ptr<RowSequence> Drop(size_t size) const = 0;

  /**
   * Iterates over the RowSequence and invokes the callback on each of the contiguous intervals
   * that it contains. The intervals passed to the callback will be represented as half-open
   * intervals [begin, end).
   */
  virtual void ForEachInterval(
      const std::function<void(uint64_t begin_key, uint64_t end_key)> &callback) const = 0;

  /**
   * The number of elements in this RowSequence.
   */
  [[nodiscard]]
  virtual size_t Size() const = 0;

  /**
   * Whether this RowSquence is empty (i.e. whether Size() == 0).
   */
  [[nodiscard]]
  bool Empty() const {
    return Size() == 0;
  }

  /**
   * Print this RowSequence to a std::ostream.
   */
  friend std::ostream &operator<<(std::ostream &s, const RowSequence &o);
};

/**
 * An iterator for RowSequence objects. This is not a standard C++ style iterator but rather a more
 * heavyweight object that supports forward iteration one step at a time.
 */
class RowSequenceIterator {
  static constexpr size_t kChunkSize = 8192;

public:
  /**
   * Constructor. Used internally.
   */
  explicit RowSequenceIterator(std::shared_ptr<RowSequence> row_sequence);
  /**
   * Move constructor.
   */
  RowSequenceIterator(RowSequenceIterator &&other) noexcept;
  /**
   * Destructor.
   */
  ~RowSequenceIterator();
  /**
   * Get the next value in the iteration.
   * @param result A pointer to caller-managed storage to store the next value.
   * @return True if the next value was stored in *result. False if there are no more values.
   */
  [[nodiscard]]
  bool TryGetNext(uint64_t *result);

private:
  void RefillRanges();

  std::shared_ptr<RowSequence> residual_;
  std::vector<std::pair<uint64_t, uint64_t>> ranges_;
  size_t rangeIndex_ = 0;
  uint64_t offset_ = 0;
};


/**
 * Builder class for RowSequence.
 */
class RowSequenceBuilder {
public:
  /**
   * Constructor.
   */
  RowSequenceBuilder();
  /**
   * Destructor.
   */
  ~RowSequenceBuilder();

  /**
   * Adds the half-open interval [begin, end) to the RowSequence. The added interval need not be
   * disjoint.
   */
  void AddInterval(uint64_t begin, uint64_t end);

  /**
   * Adds 'key' to the RowSequence. If the key is already present, does nothing.
   */
  void Add(uint64_t key) {
    AddInterval(key, key + 1);
  }

  /**
   * Builds the RowSequence.
   */
  [[nodiscard]]
  std::shared_ptr<RowSequence> Build();

private:
  /**
   * We store our half-open intervals as a map from (begin of interval) to (end of interval).
   * Our code maintains the invariants that map entries do not contain overlapping ranges,
   * and if two adjacent ranges are contiguous, they will get merged into one.
   */
  using ranges_t = std::map<uint64_t, uint64_t>;
  ranges_t ranges_;
  size_t size_ = 0;
};
}  // namespace deephaven::dhcore::container

// Add the specialization for the RowSequence formatter
template<> struct fmt::formatter<deephaven::dhcore::container::RowSequence> : fmt::ostream_formatter {};
