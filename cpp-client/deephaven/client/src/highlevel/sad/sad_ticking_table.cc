#include "deephaven/client/highlevel/sad/sad_ticking_table.h"

#include <optional>
#include <utility>

#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven::client::highlevel::sad {
namespace {
void mapShifter(int64_t start, int64_t endInclusive, int64_t dest, std::map<int64_t, int64_t> *zm);
void applyShiftData(const SadRowSequence &startIndex, const SadRowSequence &endInclusiveIndex,
    const SadRowSequence &destIndex,
    const std::function<void(int64_t, int64_t, int64_t)> &processShift);

class MyRowSequence final : public SadRowSequence {
  typedef std::map<int64_t, int64_t> data_t;
public:
  MyRowSequence(std::shared_ptr<data_t> data, data_t::const_iterator begin,
      data_t::const_iterator end, size_t size);
  ~MyRowSequence() final = default;
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

class MyRowSequenceIterator final : public SadRowSequenceIterator {
  typedef std::map<int64_t, int64_t> data_t;
public:
  MyRowSequenceIterator(std::shared_ptr<data_t> data,
      data_t::const_iterator begin, data_t::const_iterator end, size_t size, bool forward);
  ~MyRowSequenceIterator() final;

  std::shared_ptr<SadRowSequence> getNextRowSequenceWithLength(size_t size) final;
  bool tryGetNext(int64_t *result) final;

private:
  std::shared_ptr<data_t> data_;
  data_t::const_iterator begin_;
  data_t::const_iterator end_;
  size_t size_ = 0;
  bool forward_ = false;
};
}  // namespace

std::shared_ptr<SadTickingTable> SadTickingTable::create(std::vector<std::shared_ptr<SadColumnSource>> columns) {
  return std::make_shared<SadTickingTable>(Private(), std::move(columns));
}
SadTickingTable::SadTickingTable(Private, std::vector<std::shared_ptr<SadColumnSource>> columns) :
    columns_(std::move(columns)) {
  redirection_ = std::make_shared<std::map<int64_t, int64_t>>();
}
SadTickingTable::~SadTickingTable() = default;

std::shared_ptr<SadUnwrappedTable> SadTickingTable::add(const SadRowSequence &addedRows) {
  auto rowKeys = SadLongChunk::create(addedRows.size());
  auto iter = addedRows.getRowSequenceIterator();
  int64_t row;
  size_t destIndex = 0;
  while (iter->tryGetNext(&row)) {
    int64_t nextRedirectedRow;
    if (!slotsToReuse_.empty()) {
      nextRedirectedRow = slotsToReuse_.back();
      slotsToReuse_.pop_back();
    } else {
      nextRedirectedRow = (int64_t)redirection_->size();
    }
    auto result = redirection_->insert(std::make_pair(row, nextRedirectedRow));
    if (!result.second) {
      auto message = stringf("Row %o already exists", row);
      throw std::runtime_error(message);
    }
    rowKeys->data()[destIndex] = nextRedirectedRow;
    ++destIndex;
  }
  return SadUnwrappedTable::create(std::move(rowKeys), destIndex, columns_);
}

void SadTickingTable::erase(const SadRowSequence &removedRows) {
  auto iter = removedRows.getRowSequenceIterator();
  int64_t row;
  while (iter->tryGetNext(&row)) {
    auto ip = redirection_->find(row);
    if (ip == redirection_->end()) {
      auto message = stringf("Can't find row %o", row);
      throw std::runtime_error(message);
    }
    slotsToReuse_.push_back(ip->second);
    redirection_->erase(ip);
  }
}

void SadTickingTable::shift(const SadRowSequence &startIndex, const SadRowSequence &endInclusiveIndex,
    const SadRowSequence &destIndex) {
  auto processShift = [this](int64_t s, int64_t ei, int64_t dest) {
    mapShifter(s, ei, dest, redirection_.get());
  };
  applyShiftData(startIndex, endInclusiveIndex, destIndex, processShift);
}

std::shared_ptr<SadRowSequence> SadTickingTable::getRowSequence() const {
  return std::make_shared<MyRowSequence>(redirection_, redirection_->begin(), redirection_->end(),
      redirection_->size());
}

std::shared_ptr<SadUnwrappedTable>
SadTickingTable::unwrap(const std::shared_ptr<SadRowSequence> &rows,
    const std::vector<size_t> &cols) const {
  auto rowKeys = SadLongChunk::create(rows->size());
  auto iter = rows->getRowSequenceIterator();
  size_t destIndex = 0;
  int64_t rowKey;
  while (iter->tryGetNext(&rowKey)) {
    auto ip = redirection_->find(rowKey);
    if (ip == redirection_->end()) {
      auto message = stringf("Can't find rowkey %o", rowKey);
      throw std::runtime_error(message);
    }
    rowKeys->data()[destIndex++] = ip->second;
  }

  std::vector<std::shared_ptr<SadColumnSource>> columns;
  columns.reserve(cols.size());
  for (auto colIndex : cols) {
    if (colIndex >= columns_.size()) {
      auto message = stringf("No such columnindex %o", colIndex);
      throw std::runtime_error(message);
    }
    columns.push_back(columns_[colIndex]);
  }
  return SadUnwrappedTable::create(std::move(rowKeys), destIndex, std::move(columns));
}

std::shared_ptr<SadColumnSource> SadTickingTable::getColumn(size_t columnIndex) const {
  throw std::runtime_error("SadTickingTable: getColumn [redirected]: not implemented yet");
}

namespace {
void applyShiftData(const SadRowSequence &startIndex, const SadRowSequence &endInclusiveIndex,
    const SadRowSequence &destIndex,
    const std::function<void(int64_t, int64_t, int64_t)> &processShift) {
  if (startIndex.empty()) {
    return;
  }

  // Loop twice: once in the forward direction (applying negative shifts), and once in the reverse direction
  // (applying positive shifts).
  for (auto direction = 0; direction < 2; ++direction) {
    std::shared_ptr<SadRowSequenceIterator> startIter, endIter, destIter;
    if (direction == 0) {
      startIter = startIndex.getRowSequenceIterator();
      endIter = endInclusiveIndex.getRowSequenceIterator();
      destIter = destIndex.getRowSequenceIterator();
    } else {
      startIter = startIndex.getRowSequenceReverseIterator();
      endIter = endInclusiveIndex.getRowSequenceReverseIterator();
      destIter = destIndex.getRowSequenceReverseIterator();
    }
    int64_t start, end, dest;
    while (startIter->tryGetNext(&start)) {
      if (!endIter->tryGetNext(&end) || !destIter->tryGetNext(&dest)) {
        throw std::runtime_error("Sequences not of same size");
      }
      if (direction == 0) {
        // If forward, only process negative shifts.
        if (dest >= 0) {
          continue;
        }
      } else {
        // If reverse, only process positive shifts.
        if (dest <= 0) {
          continue;
        }
      }
      const char *dirText = direction == 0 ? "(positive)" : "(negative)";
      streamf(std::cerr, "Processing %o shift src [%o..%o] dest %o\n", dirText, start, end, dest);
      processShift(start, end, dest);
    }
  }
}

void mapShifter(int64_t start, int64_t endInclusive, int64_t dest, std::map<int64_t, int64_t> *zm) {
  auto delta = dest - start;
  if (delta < 0) {
    auto currentp = zm->lower_bound(start);
    while (true) {
      if (currentp == zm->end() || currentp->first > endInclusive) {
        return;
      }
      auto nextp = std::next(currentp);
      auto node = zm->extract(currentp);
      auto newKey = node.key() + delta;
      streamf(std::cerr, "Working forwards, moving key from %o to %o\n", node.key(), newKey);
      node.key() = newKey;
      zm->insert(std::move(node));
      currentp = nextp;
      ++dest;
    }
  }

  // delta >= 0 so move in the reverse direction
  auto currentp = zm->upper_bound(endInclusive);
  if (currentp == zm->begin()) {
    return;
  }
  --currentp;
  while (true) {
    if (currentp->first < start) {
      return;
    }
    std::optional<std::map<int64_t, int64_t>::iterator> nextp;
    if (currentp != zm->begin()) {
      nextp = std::prev(currentp);
    }
    auto node = zm->extract(currentp);
    auto newKey = node.key() + delta;
    streamf(std::cerr, "Working backwards, moving key from %o to %o\n", node.key(), newKey);
    node.key() = newKey;
    zm->insert(std::move(node));
    if (!nextp.has_value()) {
      return;
    }
    currentp = *nextp;
    --dest;
  }
}

MyRowSequence::MyRowSequence(std::shared_ptr<data_t> data,
    data_t::const_iterator begin, data_t::const_iterator end, size_t size)
    : data_(std::move(data)), begin_(begin), end_(end), size_(size) {}

    std::shared_ptr<SadRowSequenceIterator>
    MyRowSequence::getRowSequenceIterator() const {
  return std::make_shared<MyRowSequenceIterator>(data_, begin_, end_, size_, true);
}

std::shared_ptr<SadRowSequenceIterator>
MyRowSequence::getRowSequenceReverseIterator() const {
  return std::make_shared<MyRowSequenceIterator>(data_, begin_, end_, size_, false);
}

MyRowSequenceIterator::MyRowSequenceIterator(std::shared_ptr<data_t> data,
    data_t::const_iterator begin, data_t::const_iterator end, size_t size, bool forward)
    : data_(std::move(data)), begin_(begin), end_(end), size_(size), forward_(forward) {}
    MyRowSequenceIterator::~MyRowSequenceIterator() = default;

bool MyRowSequenceIterator::tryGetNext(int64_t *result) {
  if (begin_ == end_) {
    return false;
  }
  if (forward_) {
    *result = begin_->first;
    ++begin_;
  } else {
    --end_;
    *result = end_->first;
  }
  --size_;
  return true;
}

std::shared_ptr<SadRowSequence>
MyRowSequenceIterator::getNextRowSequenceWithLength(size_t size) {
  // TODO(kosak): iterates whole set. ugh.
  auto remaining = std::distance(begin_, end_);
  auto sizeToUse = std::min<ssize_t>((ssize_t)size, remaining);
  data_t::const_iterator newBegin, newEnd;
  if (forward_) {
    newBegin = begin_;
    std::advance(begin_, sizeToUse);
    newEnd = begin_;
  } else {
    newEnd = end_;
    std::advance(end_, -sizeToUse);
    newBegin = end_;
  }
  size_ -= sizeToUse;
  return std::make_shared<MyRowSequence>(data_, newBegin, newEnd, sizeToUse);
}
}  // namespace
}  // namespace deephaven::client::highlevel::sad
