#include "deephaven/client/highlevel/sad/sad_row_sequence.h"

namespace deephaven::client::highlevel::sad {
namespace {
/**
 * Holds a slice of a set::set<int64_t>
 */
class MySadRowSequence final : public SadRowSequence {
  typedef std::set<int64_t> data_t;
public:
  MySadRowSequence(std::shared_ptr<data_t> data, data_t::const_iterator begin,
      data_t::const_iterator end, size_t size);
  ~MySadRowSequence() final = default;
  std::shared_ptr<SadRowSequenceIterator> getRowSequenceIterator() const final;
  std::shared_ptr<SadRowSequenceIterator> getRowSequenceReverseIterator() const final;
  size_t size() const final {
    return size_;
  }

private:
  std::shared_ptr<data_t> data_;
  data_t::const_iterator begin_;
  data_t::const_iterator end_;
  size_t size_ = 0;
};

class MySadRowSequenceIterator final : public SadRowSequenceIterator {
  typedef std::set<int64_t> data_t;
public:
  MySadRowSequenceIterator(std::shared_ptr<data_t> data, data_t::const_iterator begin,
      data_t::const_iterator end, size_t size, bool forward);
  ~MySadRowSequenceIterator() final = default;
  std::shared_ptr<SadRowSequence> getNextRowSequenceWithLength(size_t size) final;
  bool tryGetNext(int64_t *result) final;

private:
  std::shared_ptr<data_t> data_;
  data_t::const_iterator begin_;
  data_t::const_iterator end_;
  size_t size_ = 0;
  bool forward_ = true;
};
} // namespace

std::shared_ptr<SadRowSequence> SadRowSequence::createSequential(int64_t begin, int64_t end) {
  // Inefficient hack for now. The efficient thing to do would be to make a special implementation
  // that just iterates over the range.
  SadRowSequenceBuilder builder;
  if (begin != end) {
    // Sad: decide on whether you want half-open or fully-closed intervals.
    builder.addRange(begin, end - 1);
  }
  return builder.build();
}

SadRowSequence::~SadRowSequence() = default;

std::ostream &operator<<(std::ostream &s, const SadRowSequence &o) {
  s << '[';
  auto iter = o.getRowSequenceIterator();
  const char *sep = "";
  int64_t item;
  while (iter->tryGetNext(&item)) {
    s << sep << item;
    sep = ", ";
  }
  s << ']';
  return s;
}

SadRowSequenceIterator::~SadRowSequenceIterator() = default;

SadRowSequenceBuilder::SadRowSequenceBuilder() : data_(std::make_shared<std::set<int64_t>>()) {}
SadRowSequenceBuilder::~SadRowSequenceBuilder() = default;

void SadRowSequenceBuilder::addRange(int64_t first, int64_t last) {
  if (first > last) {
    return;
  }
  while (true) {
    data_->insert(first);
    if (first == last) {
      return;
    }
    ++first;
  }
}

std::shared_ptr<SadRowSequence> SadRowSequenceBuilder::build() {
  auto begin = data_->begin();
  auto end = data_->end();
  auto size = data_->size();
  return std::make_shared<MySadRowSequence>(std::move(data_), begin, end, size);
}

namespace {
MySadRowSequence::MySadRowSequence(std::shared_ptr<data_t> data, data_t::const_iterator begin,
    data_t::const_iterator end, size_t size) : data_(std::move(data)), begin_(begin),
    end_(end), size_(size) {}

std::shared_ptr<SadRowSequenceIterator> MySadRowSequence::getRowSequenceIterator() const {
  return std::make_shared<MySadRowSequenceIterator>(data_, begin_, end_, size_, true);
}

std::shared_ptr<SadRowSequenceIterator> MySadRowSequence::getRowSequenceReverseIterator() const {
  return std::make_shared<MySadRowSequenceIterator>(data_, begin_, end_, size_, false);
}

MySadRowSequenceIterator::MySadRowSequenceIterator(
    std::shared_ptr<data_t> data, data_t::const_iterator begin,
    data_t::const_iterator end, size_t size, bool forward) : data_(std::move(data)), begin_(begin),
    end_(end), size_(size), forward_(forward) {}

bool MySadRowSequenceIterator::tryGetNext(int64_t *result) {
  if (begin_ == end_) {
    return false;
  }
  if (forward_) {
    *result = *begin_++;
  } else {
    *result = *--end_;
  }
  --size_;
  return true;
}

std::shared_ptr<SadRowSequence>
MySadRowSequenceIterator::getNextRowSequenceWithLength(size_t size) {
  auto sizeToUse = std::min(size, size_);
  data_t::const_iterator newBegin, newEnd;
  if (forward_) {
    newBegin = begin_;
    std::advance(begin_, sizeToUse);
    newEnd = begin_;
  } else {
    newEnd = end_;
    std::advance(end_, -(ssize_t)sizeToUse);
    newBegin = end_;
  }
  size_ -= sizeToUse;
  return std::make_shared<MySadRowSequence>(data_, newBegin, newEnd, sizeToUse);
}

}  // namespace
}  // namespace deephaven::client::highlevel::sad
