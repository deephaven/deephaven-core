/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/container/row_sequence.h"

#include <optional>
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"

namespace deephaven::dhcore::container {
namespace {
class SequentialRowSequence final : public RowSequence {
public:
  static std::shared_ptr<SequentialRowSequence> Create(uint64_t begin, uint64_t end);

  SequentialRowSequence(uint64_t begin, uint64_t end) : begin_(begin), end_(end) {}

  [[nodiscard]]
  std::shared_ptr<RowSequence> Take(size_t size) const final;
  [[nodiscard]]
  std::shared_ptr<RowSequence> Drop(size_t size) const final;
  void ForEachInterval(const std::function<void(uint64_t, uint64_t)> &f) const final;

  [[nodiscard]]
  size_t Size() const final {
    return end_ - begin_;
  }

private:
  uint64_t begin_ = 0;
  uint64_t end_ = 0;
};
}  // namespace

std::shared_ptr<RowSequence> RowSequence::CreateEmpty() {
  return SequentialRowSequence::Create(0, 0);
}

std::shared_ptr<RowSequence> RowSequence::CreateSequential(uint64_t begin, uint64_t end) {
  return SequentialRowSequence::Create(begin, end);
}

RowSequence::~RowSequence() = default;

RowSequenceIterator RowSequence::GetRowSequenceIterator() const {
  return RowSequenceIterator(Drop(0));
}

std::ostream &operator<<(std::ostream &s, const RowSequence &o) {
  s << '[';
  const char *sep = "";
  o.ForEachInterval([&](uint64_t start, uint64_t end) {
    s << sep;
    sep = ", ";
    s << '[' << start << ',' << end << ')';
  });
  s << ']';
  return s;
}

RowSequenceIterator::RowSequenceIterator(std::shared_ptr<RowSequence> row_sequence) :
  residual_(std::move(row_sequence)) {}
RowSequenceIterator::RowSequenceIterator(RowSequenceIterator &&other) noexcept = default;
RowSequenceIterator::~RowSequenceIterator() = default;

bool RowSequenceIterator::TryGetNext(uint64_t *result) {
  while (true) {
    if (rangeIndex_ == ranges_.size()) {
      rangeIndex_ = 0;
      RefillRanges();
      if (ranges_.empty()) {
        return false;
      }
      continue;
    }

    const auto &range = ranges_[rangeIndex_];
    auto range_size = range.second - range.first;
    if (offset_ == range_size) {
      ++rangeIndex_;
      offset_ = 0;
      continue;
    }

    *result = range.first + offset_;
    ++offset_;
    return true;
  }
}

void RowSequenceIterator::RefillRanges() {
  auto this_chunk = residual_->Take(kChunkSize);
  residual_ = residual_->Drop(kChunkSize);
  ranges_.clear();
  auto add_range = [this](uint64_t begin_key, uint64_t end_key) {
    ranges_.emplace_back(begin_key, end_key);
  };
  this_chunk->ForEachInterval(add_range);
}

namespace {
class MyRowSequence final : public RowSequence {
  // begin->end
  using ranges_t = std::map<uint64_t, uint64_t>;
public:
  MyRowSequence(std::shared_ptr<ranges_t> ranges, ranges_t::const_iterator beginp,
      size_t entry_offset, size_t size);
  ~MyRowSequence() final = default;

  [[nodiscard]]
  std::shared_ptr<RowSequence> Take(size_t size) const final;
  [[nodiscard]]
  std::shared_ptr<RowSequence> Drop(size_t size) const final;

  void ForEachInterval(const std::function<void(uint64_t begin_key, uint64_t end_key)> &f) const final;

  [[nodiscard]]
  size_t Size() const final {
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

void RowSequenceBuilder::AddInterval(uint64_t begin, uint64_t end) {
  if (begin > end) {
    auto message = fmt::format("Malformed range [{},{})", begin, end);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
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
    auto prev_ip = std::prev(ip);
    if (begin <= prev_ip->second) {
      // Remove that interval from the map (but, reuse its storage later) and extend the range
      // we are inserting to include the start of the interval.
      begin = prev_ip->first;
      size_ -= prev_ip->second - prev_ip->first;
      node = ranges_.extract(prev_ip);
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

std::shared_ptr<RowSequence> RowSequenceBuilder::Build() {
  auto sp = std::make_shared<ranges_t>(std::move(ranges_));
  auto begin = sp->begin();
  return std::make_shared<MyRowSequence>(std::move(sp), begin, 0, size_);
}

namespace {
MyRowSequence::MyRowSequence(std::shared_ptr<ranges_t> ranges, ranges_t::const_iterator beginp,
    size_t entry_offset, size_t size) : ranges_(std::move(ranges)), beginp_(beginp),
    entryOffset_(entry_offset), size_(size) {}

std::shared_ptr<RowSequence> MyRowSequence::Take(size_t size) const {
  auto new_size = std::min(size, size_);
  return std::make_shared<MyRowSequence>(ranges_, beginp_, entryOffset_, new_size);
}

std::shared_ptr<RowSequence> MyRowSequence::Drop(size_t size) const {
  auto current = beginp_;
  auto current_offset = entryOffset_;
  auto size_to_drop = std::min(size, size_);
  auto new_size = size_ - size_to_drop;
  while (size_to_drop != 0) {
    auto entry_size = current->second - current->first;
    if (current_offset == entry_size) {
      ++current;
      current_offset = 0;
      continue;
    }
    auto entry_remaining = entry_size - current_offset;
    auto amount_to_consume = std::min(entry_remaining, size_to_drop);
    current_offset += amount_to_consume;
    size_to_drop -= amount_to_consume;
  }
  return std::make_shared<MyRowSequence>(ranges_, current, current_offset, new_size);
}

void MyRowSequence::ForEachInterval(const std::function<void(uint64_t begin_key, uint64_t end_key)> &f)
    const {
  // The code is similar to "drop"
  auto current = beginp_;
  auto current_offset = entryOffset_;
  auto remaining = size_;
  while (remaining != 0) {
    auto entry_size = current->second - current->first;
    if (current_offset == entry_size) {
      ++current;
      current_offset = 0;
      continue;
    }
    auto entry_remaining = entry_size - current_offset;
    auto amount_to_consume = std::min(entry_remaining, remaining);
    auto begin = current->first + current_offset;
    auto end = begin + amount_to_consume;
    current_offset += amount_to_consume;
    remaining -= amount_to_consume;
    f(begin, end);
  }
}

std::shared_ptr<SequentialRowSequence> SequentialRowSequence::Create(uint64_t begin, uint64_t end) {
  return std::make_shared<SequentialRowSequence>(begin, end);
}

std::shared_ptr<RowSequence> SequentialRowSequence::Take(size_t size) const {
  auto size_to_use = std::min(size, this->Size());
  return Create(begin_, begin_ + size_to_use);
}

std::shared_ptr<RowSequence> SequentialRowSequence::Drop(size_t size) const {
  auto size_to_use = std::min(size, this->Size());
  return Create(begin_ + size_to_use, end_);
}

void SequentialRowSequence::ForEachInterval(const std::function<void(uint64_t, uint64_t)> &f) const {
  if (begin_ == end_) {
    return;
  }
  f(begin_, end_);
}
}  // namespace
}  // namespace deephaven::client::container
