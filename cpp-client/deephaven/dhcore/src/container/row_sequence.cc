/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/container/row_sequence.h"

#include <optional>
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::stringf;

namespace deephaven::dhcore::container {
namespace {
class SequentialRowSequence final : public RowSequence {
public:
  static std::shared_ptr<SequentialRowSequence> create(uint64_t begin, uint64_t end);

  SequentialRowSequence(uint64_t begin, uint64_t end) : begin_(begin), end_(end) {}

  std::shared_ptr<RowSequence> take(size_t size) const final;
  std::shared_ptr<RowSequence> drop(size_t size) const final;
  void forEachInterval(const std::function<void(uint64_t, uint64_t)> &f) const final;

  size_t size() const final {
    return end_ - begin_;
  }

private:
  uint64_t begin_ = 0;
  uint64_t end_ = 0;
};
}  // namespace

std::shared_ptr<RowSequence> RowSequence::createEmpty() {
  return SequentialRowSequence::create(0, 0);
}

std::shared_ptr<RowSequence> RowSequence::createSequential(uint64_t begin, uint64_t end) {
  return SequentialRowSequence::create(begin, end);
}

RowSequence::~RowSequence() = default;

RowSequenceIterator RowSequence::getRowSequenceIterator() const {
  return RowSequenceIterator(drop(0));
}

std::ostream &operator<<(std::ostream &s, const RowSequence &o) {
  s << '[';
  auto iter = o.getRowSequenceIterator();
  const char *sep = "";
  uint64_t item;
  while (iter.tryGetNext(&item)) {
    s << sep << item;
    sep = ", ";
  }
  s << ']';
  return s;
}

RowSequenceIterator::RowSequenceIterator(std::shared_ptr<RowSequence> rowSequence) :
  residual_(std::move(rowSequence)) {}
RowSequenceIterator::RowSequenceIterator(RowSequenceIterator &&other) noexcept = default;
RowSequenceIterator::~RowSequenceIterator() = default;

bool RowSequenceIterator::tryGetNext(uint64_t *result) {
  while (true) {
    if (rangeIndex_ == ranges_.size()) {
      rangeIndex_ = 0;
      refillRanges();
      if (ranges_.empty()) {
        return false;
      }
      continue;
    }

    const auto &range = ranges_[rangeIndex_];
    auto rangeSize = range.second - range.first;
    if (offset_ == rangeSize) {
      ++rangeIndex_;
      offset_ = 0;
      continue;
    }

    *result = range.first + offset_;
    ++offset_;
    return true;
  }
}

void RowSequenceIterator::refillRanges() {
  auto thisChunk = residual_->take(chunkSize);
  residual_ = residual_->drop(chunkSize);
  ranges_.clear();
  auto addRange = [this](uint64_t beginKey, uint64_t endKey) {
    ranges_.emplace_back(beginKey, endKey);
  };
  thisChunk->forEachInterval(addRange);
}

namespace {
class MyRowSequence final : public RowSequence {
  // begin->end
  typedef std::map<uint64_t, uint64_t> ranges_t;
public:
  MyRowSequence(std::shared_ptr<ranges_t> ranges, ranges_t::const_iterator beginp,
      size_t entryOffset, size_t size);
  ~MyRowSequence() final = default;

  std::shared_ptr<RowSequence> take(size_t size) const final;
  std::shared_ptr<RowSequence> drop(size_t size) const final;

  void forEachInterval(const std::function<void(uint64_t beginKey, uint64_t endKey)> &f) const final;

  size_t size() const final {
    return size_;
  }

private:
  std::shared_ptr<ranges_t> ranges_;
  ranges_t::const_iterator beginp_;
  size_t entryOffset_ = 0;
  size_t size_ = 0;
};
} // namespace

RowSequenceBuilder::RowSequenceBuilder() = default;
RowSequenceBuilder::~RowSequenceBuilder() = default;

void RowSequenceBuilder::addInterval(uint64_t begin, uint64_t end) {
  if (begin > end) {
    auto message = stringf("Malformed range [%o,%o)", begin, end);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  if (begin == end) {
    return;
  }

  // ip points to the first element greater than begin, or it is end()
  auto ip = ranges_.upper_bound(begin);

  // Reusable storage for the node we will ultimately insert.
  ranges_t::node_type node;

  // The start point `begin` might fall inside an existing interval, or it might be outside all
  // intervals. If it's inside any interval, it will be the one just prior to ip.
  if (ip != ranges_.begin()) {
    auto prevIp = std::prev(ip);
    if (begin <= prevIp->second) {
      // Remove that interval from the map (but, reuse its storage later) and extend the range
      // we are inserting to include the start of the interval.
      begin = prevIp->first;
      size_ -= prevIp->second - prevIp->first;
      node = ranges_.extract(prevIp);
    }
  }

  // Now 'begin' is not inside any existing interval. Determine what to do about 'end' by repeating
  // the below:
  // 1. If it falls short of the next interval, we are done
  // 2. If it falls inside the next interval, remove that interval and extend 'end' to the end of
  //    that interval, and repeat
  // 3. If it falls past the end of the next interval, remove that interval, and repeat
  while (ip != ranges_.end() && end >= ip->first) {
    end = std::max(end, ip->second);
    auto temp = ip++;
    size_ -= temp->second - temp->first;
    node = ranges_.extract(temp);
  }

  // Now we can insert 'node' into the map, reusing existing storage if possible.
  if (node.empty()) {
    // We were not able to reuse any nodes
    ranges_.insert(std::make_pair(begin, end));
  } else {
    // We were able to reuse one node.
    node.key() = begin;
    node.mapped() = end;
    ranges_.insert(std::move(node));
  }
  size_ += end - begin;
}

std::shared_ptr<RowSequence> RowSequenceBuilder::build() {
  auto sp = std::make_shared<ranges_t>(std::move(ranges_));
  auto begin = sp->begin();
  return std::make_shared<MyRowSequence>(std::move(sp), begin, 0, size_);
}

namespace {
MyRowSequence::MyRowSequence(std::shared_ptr<ranges_t> ranges, ranges_t::const_iterator beginp,
    size_t entryOffset, size_t size) : ranges_(std::move(ranges)), beginp_(beginp),
    entryOffset_(entryOffset), size_(size) {}

std::shared_ptr<RowSequence> MyRowSequence::take(size_t size) const {
  auto newSize = std::min(size, size_);
  return std::make_shared<MyRowSequence>(ranges_, beginp_, entryOffset_, newSize);
}

std::shared_ptr<RowSequence> MyRowSequence::drop(size_t size) const {
  auto current = beginp_;
  auto currentOffset = entryOffset_;
  auto sizeToDrop = std::min(size, size_);
  auto newSize = size_ - sizeToDrop;
  while (sizeToDrop != 0) {
    auto entrySize = current->second - current->first;
    if (currentOffset == entrySize) {
      ++current;
      currentOffset = 0;
      continue;
    }
    auto entryRemaining = entrySize - currentOffset;
    auto amountToConsume = std::min(entryRemaining, sizeToDrop);
    currentOffset += amountToConsume;
    sizeToDrop -= amountToConsume;
  }
  return std::make_shared<MyRowSequence>(ranges_, current, currentOffset, newSize);
}

void MyRowSequence::forEachInterval(const std::function<void(uint64_t beginKey, uint64_t endKey)> &f)
    const {
  // The code is similar to "drop"
  auto current = beginp_;
  auto currentOffset = entryOffset_;
  auto remaining = size_;
  while (remaining != 0) {
    auto entrySize = current->second - current->first;
    if (currentOffset == entrySize) {
      ++current;
      currentOffset = 0;
      continue;
    }
    auto entryRemaining = entrySize - currentOffset;
    auto amountToConsume = std::min(entryRemaining, remaining);
    auto begin = current->first + currentOffset;
    auto end = begin + amountToConsume;
    currentOffset += amountToConsume;
    remaining -= amountToConsume;
    f(begin, end);
  }
}

std::shared_ptr<SequentialRowSequence> SequentialRowSequence::create(uint64_t begin, uint64_t end) {
  return std::make_shared<SequentialRowSequence>(begin, end);
}

std::shared_ptr<RowSequence> SequentialRowSequence::take(size_t size) const {
  auto sizeToUse = std::min(size, this->size());
  return create(begin_, begin_ + sizeToUse);
}

std::shared_ptr<RowSequence> SequentialRowSequence::drop(size_t size) const {
  auto sizeToUse = std::min(size, this->size());
  return create(begin_ + sizeToUse, end_);
}

void SequentialRowSequence::forEachInterval(const std::function<void(uint64_t, uint64_t)> &f) const {
  if (begin_ == end_) {
    return;
  }
  f(begin_, end_);
}
}  // namespace
}  // namespace deephaven::client::container
