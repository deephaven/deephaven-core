/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <set>

namespace deephaven::client::container {
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
  static std::shared_ptr<RowSequence> createEmpty();
  /**
   * Create a dense RowSequence providing values from the half open interval [begin, end).
   */
  static std::shared_ptr<RowSequence> createSequential(uint64_t begin, uint64_t end);

  /**
   * Destructor.
   */
  virtual ~RowSequence();

  /**
   * Create a RowSequenceIterator.
   */
  RowSequenceIterator getRowSequenceIterator() const;

  /**
   * Return a RowSequence consisting of the first 'size' elements of this RowSequence.
   * If size >= this->size(), the method may return this RowSequence.
   */
  virtual std::shared_ptr<RowSequence> take(size_t size) const = 0;
  /**
   * Return a RowSequence consisting of the remaining elements of this RowSequence after dropping
   * 'size' elements. If size >= this->size(), returns the empty RowSequence.
   */
  virtual std::shared_ptr<RowSequence> drop(size_t size) const = 0;

  /**
   * Iterates over the RowSequence and invokes the callback on each of the contiguous intervals
   * that it contains. The intervals passed to the callback will be represented as half-open
   * intervals [begin, end).
   */
  virtual void forEachInterval(
      const std::function<void(uint64_t beginKey, uint64_t endKey)> &callback) const = 0;

  /**
   * The number of elements in this RowSequence.
   */
  virtual size_t size() const = 0;

  /**
   * Whether this RowSquence is empty (i.e. whether size() == 0).
   */
  bool empty() const {
    return size() == 0;
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
  static constexpr size_t chunkSize = 8192;

public:
  /**
   * Constructor. Used internally.
   */
  explicit RowSequenceIterator(std::shared_ptr<RowSequence> rowSequence);
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
  bool tryGetNext(uint64_t *result);

private:
  void refillRanges();

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
   * Adds the half-open interval [begin, end) to the RowSequence.
   */
  void addInterval(uint64_t begin, uint64_t end);

  /**
   * Adds 'key' to the RowSequence.
   */
  void add(uint64_t key) {
    addInterval(key, key + 1);
  }

  /**
   * Builds the RowSequence.
   */
  std::shared_ptr<RowSequence> build();

private:
  /**
   * We store our half-open intervals as a map from (begin of interval) to (end of interval).
   * Our code maintains the invariants that map entries do not contain overlapping ranges,
   * and if two adjacent ranges are contiguous, they will get merged into one.
   */
  typedef std::map<uint64_t, uint64_t> ranges_t;
  ranges_t ranges_;
  size_t size_ = 0;
};
}  // namespace deephaven::client::container
