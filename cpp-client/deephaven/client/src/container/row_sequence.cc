/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::stringf;

namespace deephaven::client::container {
namespace {
class SequentialRowSequence final : public RowSequence {
public:
  static std::shared_ptr<SequentialRowSequence> create(uint64_t begin, uint64_t end);

  SequentialRowSequence(uint64_t begin, uint64_t end) : begin_(begin), end_(end) {}

  std::shared_ptr<RowSequence> take(size_t size) const final;
  std::shared_ptr<RowSequence> drop(size_t size) const final;
  void forEachChunk(const std::function<void(uint64_t, uint64_t)> &f) const final;

  size_t size() const final {
    return end_ - begin_;
  }

private:
  uint64_t begin_ = 0;
  uint64_t end_ = 0;
};
}  // namespace

std::shared_ptr<RowSequence> RowSequence::createEmpty() {
  return RowSequenceBuilder().build();
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
  thisChunk->forEachChunk(addRange);
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

  void forEachChunk(const std::function<void(uint64_t beginKey, uint64_t endKey)> &f) const final;

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

void RowSequenceBuilder::addRange(uint64_t begin, uint64_t end) {
  if (begin > end) {
    auto message = stringf("Malformed range [%o,%o)", begin, end);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  if (begin == end) {
    return;
  }

  // First we look to the right to see if this range can be combined with the next range.
  ranges_t::node_type node;
  auto ip = ranges_.upper_bound(begin);
  // We will use this below.
  std::optional<ranges_t::iterator> prevIp;
  if (ip != ranges_.begin()) {
    prevIp = std::prev(ip);
  }
  // ip points to the first element greater than begin, or it is end()
  if (ip != ranges_.end()) {
    // ip points to the first element greater than begin
    //
    // Example:
    // Assuming our input [begin, end) is [3, 5)
    // If the range to the right is [6, 9), then they are not connected.
    // If the range to the right is [5, 9) then they abut and can be merged.
    // If the range to the right is [4, 9] then this is an error. Our caller is not allowed to
    // call us with overlapping ranges.
    if (end > ip->first) {
      // This is the [4, 9) error case from the example.
      auto message = stringf("Looking right: new range [%o,%o) would overlap with existing range [%o, %o)",
          begin, end, ip->first, ip->second);
      throw std::runtime_error(message);
    }
    if (end == ip->first) {
      // This is the [5, 9) "abutting" case in the example.
      // Extend the range we are adding to include this range.
      end = ip->second;
      // Then extract the node. Ultimately we will either discard this node or change its key
      // (the begin part of the range) and reinsert it.
      node = ranges_.extract(ip);
      // Now our input is [3, 9) and we've got "node" in a temporary variable whose heap storage
      // we might reuse if we can.
    }
  }

  // At this point our input range is either the original or modified [begin, end), and we might
  // have node storage that we can reuse.

  // Now we look to the left to see if the input range can be combined with the previous range.
  if (prevIp.has_value()) {
    ip = *prevIp;  // convenient to move to this temporary

    // Example: our input is [3, 5)
    // If the range to the left is [0, 2) then they are not connected.
    // If the range to the left is [0, 3) then they abut, and we can just reset the right
    // side of the left range to [0, 5].
    // If the range to the left is [0, 4) then this is an error due to overlap.
    if (ip->second > begin) {
      // This is the [0, 4) error case from the example.
      auto message = stringf(
          "Looking left: new range [%o,%o) would overlap with existing range [%o, %o)",
          begin, end, ip->first, ip->second);
      throw std::runtime_error(message);
    }

    if (ip->second == begin) {
      // This is the [0, 3) "abutting" case in the example.
      // Extend the range we are adding to include this range.
      // In our example we just reset the range to be [0, 5).
      ip->second = end;
      size_ += end - begin;
      return;
    }
  }

  // If we get here, we were not able to merge with the left node. So we have an interval that
  // needs to be inserted and we may or may not be able to reuse heap storage previous extracted
  // from the right.
  if (node.empty()) {
    // We were not able to reuse any nodes
    ranges_.insert(std::make_pair(begin, end));
  } else {
    // We were able to reuse at least one node (if we merged on both sides, then we can reuse
    // one node and discard one node).
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

void MyRowSequence::forEachChunk(const std::function<void(uint64_t beginKey, uint64_t endKey)> &f)
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

void SequentialRowSequence::forEachChunk(const std::function<void(uint64_t, uint64_t)> &f) const {
  f(begin_, end_);
}
}  // namespace
}  // namespace deephaven::client::container
