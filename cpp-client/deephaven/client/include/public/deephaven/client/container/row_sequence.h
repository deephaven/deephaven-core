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

class RowSequence {
public:
  static std::shared_ptr<RowSequence> createEmpty();
  static std::shared_ptr<RowSequence> createSequential(uint64_t begin, uint64_t end);

  virtual ~RowSequence();

  virtual std::shared_ptr<RowSequenceIterator> getRowSequenceIterator() const = 0;

  virtual std::shared_ptr<RowSequence> take(size_t size) const = 0;
  virtual std::shared_ptr<RowSequence> drop(size_t size) const = 0;

  virtual void forEachChunk(const std::function<void(uint64_t beginKey, uint64_t endKey)> &f) const = 0;

  virtual size_t size() const = 0;

  bool empty() const {
    return size() == 0;
  }

  friend std::ostream &operator<<(std::ostream &s, const RowSequence &o);
};

class RowSequenceIterator {
public:
  virtual ~RowSequenceIterator();
  virtual bool tryGetNext(uint64_t *result) = 0;
};

class RowSequenceBuilder {
public:
  RowSequenceBuilder();
  ~RowSequenceBuilder();

  void addRange(uint64_t begin, uint64_t end);

  void add(uint64_t key) {
    addRange(key, key + 1);
  }

  std::shared_ptr<RowSequence> build();

private:
  typedef std::map<uint64_t, uint64_t> ranges_t;
  // maps range.begin to range.end. We ensure that ranges never overlap and that contiguous ranges
  // are collapsed.
  ranges_t ranges_;
  size_t size_ = 0;
};
}  // namespace deephaven::client::container
