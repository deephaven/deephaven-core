/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/clienttable/client_table.h"

#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/utility/utility.h"

#include <optional>

using deephaven::dhcore::chunk::AnyChunk;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::ChunkMaker;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceIterator;
using deephaven::dhcore::utility::makeReservedVector;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::SimpleOstringstream;
using deephaven::dhcore::utility::stringf;

namespace deephaven::dhcore::clienttable {
namespace {
void printTableData(std::ostream &stream, const ClientTable &table,
    const std::vector<size_t> &whichCols,
    const std::vector<std::shared_ptr<RowSequence>> &rowSequences,
    bool wantHeaders, bool wantRowNumbers, bool highlightCells);
}  // namespace

std::optional<size_t> ClientTable::getColumnIndex(std::string_view name, bool strict) const {
  return schema()->getColumnIndex(name, strict);
}

std::shared_ptr<ColumnSource> ClientTable::getColumn(std::string_view name, bool strict) const {
  auto index = getColumnIndex(name, strict);
  if (!index.has_value()) {
    return {};
  }
  return getColumn(*index);
}

internal::TableStreamAdaptor ClientTable::stream(bool wantHeaders, bool wantRowNumbers) const {
  std::vector<std::shared_ptr<RowSequence>> rowSequences{getRowSequence()};
  return {*this, std::move(rowSequences), wantHeaders, wantRowNumbers, false};
}

internal::TableStreamAdaptor ClientTable::stream(bool wantHeaders, bool wantRowNumbers,
    std::shared_ptr<RowSequence> rowSequence) const {
  std::vector<std::shared_ptr<RowSequence>> rowSequences{std::move(rowSequence)};
  return {*this, std::move(rowSequences), wantHeaders, wantRowNumbers, false};
}

internal::TableStreamAdaptor ClientTable::stream(bool wantHeaders, bool wantRowNumbers,
    std::vector<std::shared_ptr<RowSequence>> rowSequences) const {
  return {*this, std::move(rowSequences), wantHeaders, wantRowNumbers, true};
}

std::string ClientTable::toString(bool wantHeaders, bool wantRowNumbers) const {
  SimpleOstringstream oss;
  oss << stream(wantHeaders, wantRowNumbers);
  return std::move(oss.str());
}

std::string ClientTable::toString(bool wantHeaders, bool wantRowNumbers,
    std::shared_ptr<RowSequence> rowSequence) const {
  SimpleOstringstream oss;
  oss << stream(wantHeaders, wantRowNumbers, std::move(rowSequence));
  return std::move(oss.str());
}

std::string ClientTable::toString(bool wantHeaders, bool wantRowNumbers,
    std::vector<std::shared_ptr<RowSequence>> rowSequences) const {
  SimpleOstringstream oss;
  oss << stream(wantHeaders, wantRowNumbers, std::move(rowSequences));
  return std::move(oss.str());
}

namespace internal {
std::ostream &operator<<(std::ostream &s, const TableStreamAdaptor &o) {
  const auto &t = o.table_;
  auto numCols = t.numColumns();
  auto whichCols = makeReservedVector<size_t>(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    whichCols.push_back(i);
  }
  printTableData(s, t, whichCols, o.rowSequences_, o.wantHeaders_, o.wantRowNumbers_, o.highlightCells_);
  return s;
}
}  // namespace internal

namespace {
class ArrayRowSequence final : public RowSequence {
public:
  static std::shared_ptr<ArrayRowSequence> create(std::shared_ptr<uint64_t[]> data,
      const uint64_t *begin, const uint64_t *end);

  ArrayRowSequence(std::shared_ptr<uint64_t[]> data, const uint64_t *begin, const uint64_t *end);
  ~ArrayRowSequence() final;

  std::shared_ptr<RowSequence> take(size_t size) const final;
  std::shared_ptr<RowSequence> drop(size_t size) const final;
  void forEachInterval(const std::function<void(uint64_t, uint64_t)> &f) const final;

  size_t size() const final {
    return end_ - begin_;
  }

private:
  std::shared_ptr<uint64_t[]> data_;
  const uint64_t *begin_ = nullptr;
  const uint64_t *end_ = nullptr;
};

class ElementStreamer final {
public:
  ElementStreamer(std::ostream &s, size_t index, bool nullFlag, bool highlight) :
      s_(s), index_(index), nullFlag_(nullFlag), highlight_(highlight) {}

  template<typename T>
  void operator()(const T &chunk) const {
    if (highlight_) {
      s_ << '*';
    }

    if (nullFlag_) {
      s_ << "null";
    } else {
      render(chunk.data()[index_]);
    }

    if (highlight_) {
      s_ << '*';
    }
  }

private:
  template<typename T>
  void render(const T &item) const {
    s_ << item;
  }

  void render(const bool &item) const {
    s_ << (item ? "true" : "false");
  }

  std::ostream &s_;
  size_t index_ = 0;
  bool nullFlag_ = false;
  bool highlight_ = false;
};

struct RowSequenceState {
  explicit RowSequenceState(RowSequenceIterator iterator, size_t chunkSize);
  RowSequenceState(RowSequenceState &&other) noexcept;
  ~RowSequenceState();

  RowSequenceIterator iterator_;
  std::optional<uint64_t> currentValue_;
  std::unique_ptr<bool[]> isPresent_;
};

class RowMerger {
public:
  RowMerger(std::vector<RowSequenceIterator> iterators, size_t chunkSize);
  ~RowMerger();

  std::shared_ptr<RowSequence> getNextChunk();

  bool isCellPresent(size_t colIndex, size_t chunkOffset) const;

private:
  size_t chunkSize_ = 0;
  std::vector<RowSequenceState> rowSequenceStates_;
  /**
   * This is a shared ponter because we share it with the ArrayRowSequence that we return
   * from getNextChunk. size = chunkSize_
   */
  std::shared_ptr<uint64_t[]> build_;
};

void printTableData(std::ostream &stream, const ClientTable &table,
    const std::vector<size_t> &whichCols,
    const std::vector<std::shared_ptr<RowSequence>> &rowSequences,
    bool wantHeaders, bool wantRowNumbers, bool highlightCells) {
  if (wantHeaders) {
    const char *separator = "";
    if (wantRowNumbers) {
      stream << "[Row]";
      separator = "\t";
    }
    for (auto colIndex : whichCols) {
      stream << separator << table.schema()->names()[colIndex];
      separator = "\t";
    }
    stream << std::endl;
  }

  if (whichCols.empty() || rowSequences.empty()) {
    return;
  }

  const size_t chunkSize = 8192;

  auto numCols = whichCols.size();
  auto dataChunks = makeReservedVector<AnyChunk>(numCols);
  auto nullFlagChunks = makeReservedVector<BooleanChunk>(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    const auto &c = table.getColumn(i);
    auto dataChunk = ChunkMaker::createChunkFor(*c, chunkSize);
    auto nullFlagChunk = BooleanChunk::create(chunkSize);
    dataChunks.push_back(std::move(dataChunk));
    nullFlagChunks.push_back(std::move(nullFlagChunk));
  }

  auto iterators = makeReservedVector<RowSequenceIterator>(rowSequences.size());
  for (const auto &rs : rowSequences) {
    iterators.push_back(rs->getRowSequenceIterator());
  }

  RowMerger merger(std::move(iterators), chunkSize);

  while (true) {
    auto chunkOfRows = merger.getNextChunk();
    auto thisSize = chunkOfRows->size();
    if (thisSize == 0) {
      break;
    }

    for (size_t i = 0; i < numCols; ++i) {
      const auto colNum = whichCols[i];
      const auto &c = table.getColumn(colNum);
      auto &dataChunk = dataChunks[colNum].unwrap();
      auto &nullFlagChunk = nullFlagChunks[colNum];
      c->fillChunk(*chunkOfRows, &dataChunk, &nullFlagChunk);
    }

    // To print out the optional row number
    auto rowsIter = chunkOfRows->getRowSequenceIterator();

    for (size_t chunkOffset = 0; chunkOffset < thisSize; ++chunkOffset) {
      const char *separator = "";
      if (wantRowNumbers) {
        uint64_t rowNum;
        if (!rowsIter.tryGetNext(&rowNum)) {
          throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Impossible: no more rows"));
        }
        stream << '[' << rowNum << "] ";
        separator = "\t";
      }

      for (size_t i = 0; i < numCols; ++i) {
        stream << separator;
        separator = "\t";
        auto nullFlag = nullFlagChunks[i].data()[chunkOffset];
        auto highlight = highlightCells && merger.isCellPresent(i, chunkOffset);
        ElementStreamer es(stream, chunkOffset, nullFlag, highlight);
        dataChunks[i].visit(es);
      }

      stream << std::endl;
    }
  }
}

std::shared_ptr<ArrayRowSequence>
ArrayRowSequence::create(std::shared_ptr<uint64_t[]> data, const uint64_t *begin,
    const uint64_t *end) {
  return std::make_shared<ArrayRowSequence>(std::move(data), begin, end);
}

ArrayRowSequence::ArrayRowSequence(std::shared_ptr<uint64_t[]> data, const uint64_t *begin,
    const uint64_t *end) : data_(std::move(data)), begin_(begin), end_(end) {}

ArrayRowSequence::~ArrayRowSequence() = default;

std::shared_ptr<RowSequence> ArrayRowSequence::take(size_t size) const {
  auto sizeToUse = std::min(size, this->size());
  return create(data_, begin_, begin_ + sizeToUse);
}

std::shared_ptr<RowSequence> ArrayRowSequence::drop(size_t size) const {
  auto sizeToUse = std::min(size, this->size());
  return create(data_, begin_ + sizeToUse, end_);
}

void ArrayRowSequence::forEachInterval(const std::function<void(uint64_t, uint64_t)> &f) const {
  const auto *rangeStart = begin_;
  while (rangeStart != end_) {
    auto beginKey = *rangeStart;
    const auto *rangeEnd = rangeStart + 1;
    auto endKey = beginKey + 1;
    while (rangeEnd != end_ && *rangeEnd == endKey) {
      ++rangeEnd;
      ++endKey;
    }
    f(beginKey, endKey);
    rangeStart = rangeEnd;
  }
}

RowSequenceState::RowSequenceState(RowSequenceIterator iterator, size_t chunkSize) :
  iterator_(std::move(iterator)), isPresent_(std::make_unique<bool[]>(chunkSize)) {
  uint64_t value;
  if (iterator_.tryGetNext(&value)) {
    currentValue_ = value;
  }
}
RowSequenceState::RowSequenceState(RowSequenceState &&other) noexcept = default;
RowSequenceState::~RowSequenceState() = default;

RowMerger::RowMerger(std::vector<RowSequenceIterator> iterators, size_t chunkSize) :
  chunkSize_(chunkSize) {

  rowSequenceStates_ = makeReservedVector<RowSequenceState>(iterators.size());
  for (auto &iter : iterators) {
    rowSequenceStates_.emplace_back(std::move(iter), chunkSize);
  }
  build_ = std::shared_ptr<uint64_t[]>(new uint64_t[chunkSize]);
}

RowMerger::~RowMerger() = default;

std::shared_ptr<RowSequence> RowMerger::getNextChunk() {
  size_t destIndex;
  uint64_t *buildp = build_.get();
  for (destIndex = 0; destIndex < chunkSize_; ++destIndex) {
    // Simplistic priority queue. If performance becomes an issue, this should be rewritten as a
    // legit priority queue.

    // First calculate the minimum value among the current values (if one exists)
    std::optional<uint64_t> minValue;
    for (const auto &rss : rowSequenceStates_) {
      const auto &cv = rss.currentValue_;
      if (!cv.has_value()) {
        continue;
      }
      if (!minValue.has_value() || *cv < *minValue) {
        minValue = *cv;
      }
    }

    // If no values found, we are done.
    if (!minValue.has_value()) {
      break;
    }

    // Store the minimum value, calculate the isPresent flag, and advance the iterators that match
    // the minimum value.
    buildp[destIndex] = *minValue;

    // Advance the iterators that match the minimum value.
    for (auto &rss : rowSequenceStates_) {
      auto &cv = rss.currentValue_;
      if (!cv.has_value() || *cv != *minValue) {
        rss.isPresent_[destIndex] = false;
        continue;
      }
      rss.isPresent_[destIndex] = true;

      // Bump to next if you can
      uint64_t value;
      if (rss.iterator_.tryGetNext(&value)) {
        cv = value;
      } else {
        cv.reset();
      }
    }
  }

  return ArrayRowSequence::create(build_, build_.get(), build_.get() + destIndex);
}

bool RowMerger::isCellPresent(size_t colIndex, size_t chunkOffset) const {
  auto colIndexToUse = colIndex < rowSequenceStates_.size() ? colIndex : 0;
  return rowSequenceStates_[colIndexToUse].isPresent_[chunkOffset];
}
}  // namespace
}  // namespace deephaven::dhcore::clienttable
