/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::stringf;

namespace deephaven::client::container {
std::shared_ptr<RowSequence> RowSequence::createEmpty() {
  return RowSequenceBuilder().build();
}

std::shared_ptr<RowSequence> RowSequence::createSequential(uint64_t begin, uint64_t end) {
  RowSequenceBuilder builder;
  builder.addRange(begin, end);
  return builder.build();
}

RowSequence::~RowSequence() = default;

std::ostream &operator<<(std::ostream &s, const RowSequence &o) {
  s << '[';
  auto iter = o.getRowSequenceIterator();
  const char *sep = "";
  uint64_t item;
  while (iter->tryGetNext(&item)) {
    s << sep << item;
    sep = ", ";
  }
  s << ']';
  return s;
}

namespace {
class MyRowSequence final : public RowSequence {
  // begin->end
  typedef std::map<uint64_t, uint64_t> ranges_t;
public:
  MyRowSequence(std::shared_ptr<ranges_t> ranges, ranges_t::const_iterator beginp,
      size_t entryOffset, size_t size);
  ~MyRowSequence() final = default;
  std::shared_ptr<RowSequenceIterator> getRowSequenceIterator() const final;

  std::shared_ptr<RowSequence> take(size_t size) const final;
  std::shared_ptr<RowSequence> drop(size_t size) const final;

  void forEachChunk(const std::function<void(uint64_t firstKey, uint64_t lastKey)> &f) const final;

  size_t size() const final {
    return size_;
  }

private:
  std::shared_ptr<ranges_t> ranges_;
  ranges_t::const_iterator beginp_;
  size_t entryOffset_ = 0;
  size_t size_ = 0;
};

class MyRowSequenceIterator final : public RowSequenceIterator {
  typedef std::map<uint64_t, uint64_t> ranges_t;
public:
  MyRowSequenceIterator(std::shared_ptr<ranges_t> ranges, ranges_t::const_iterator currentp,
      size_t currentOffset, size_t size);
  ~MyRowSequenceIterator() final = default;
  bool tryGetNext(uint64_t *result) final;

private:
  std::shared_ptr<ranges_t> ranges_;
  ranges_t::const_iterator current_;
  size_t currentOffset_ = 0;
  size_t size_ = 0;
};
} // namespace

RowSequenceIterator::~RowSequenceIterator() = default;
RowSequenceBuilder::RowSequenceBuilder() = default;
RowSequenceBuilder::~RowSequenceBuilder() = default;

void RowSequenceBuilder::addRange(uint64_t begin, uint64_t end) {
  if (begin > end) {
    throw std::runtime_error(stringf("Malformed range [%o,%o)", begin, end));
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

std::shared_ptr<RowSequenceIterator> MyRowSequence::getRowSequenceIterator() const {
  return std::make_shared<MyRowSequenceIterator>(ranges_, beginp_, entryOffset_, size_);
}

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

MyRowSequenceIterator::MyRowSequenceIterator(std::shared_ptr<ranges_t> ranges,
    ranges_t::const_iterator current, size_t currentOffset, size_t size) :
    ranges_(std::move(ranges)), current_(current), currentOffset_(currentOffset), size_(size) {}

bool MyRowSequenceIterator::tryGetNext(uint64_t *result) {
  while (size_ != 0) {
    auto entrySize = current_->second - current_->first;
    if (currentOffset_ < entrySize) {
      *result = current_->first + currentOffset_;
      ++currentOffset_;
      --size_;
      return true;
    }
    currentOffset_ = 0;
    ++current_;
  }
  return false;
}
}  // namespace
}  // namespace deephaven::client::container

