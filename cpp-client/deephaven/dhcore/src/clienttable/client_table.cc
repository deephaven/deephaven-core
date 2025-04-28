/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/clienttable/client_table.h"

#include <cstddef>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/container/container.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::chunk::AnyChunk;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::ChunkMaker;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::container::ContainerBase;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceIterator;
using deephaven::dhcore::utility::ElementRenderer;
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::SimpleOstringstream;

namespace deephaven::dhcore::clienttable {
namespace {
void PrintTableData(std::ostream &stream, const ClientTable &table,
    const std::vector<size_t> &which_cols,
    const std::vector<std::shared_ptr<RowSequence>> &row_sequences,
    bool want_headers, bool want_row_numbers, bool highlight_cells);
}  // namespace

std::optional<size_t> ClientTable::GetColumnIndex(std::string_view name, bool strict) const {
  return Schema()->GetColumnIndex(name, strict);
}

std::shared_ptr<ColumnSource> ClientTable::GetColumn(std::string_view name, bool strict) const {
  auto index = GetColumnIndex(name, strict);
  if (!index.has_value()) {
    return {};
  }
  return GetColumn(*index);
}

internal::TableStreamAdaptor ClientTable::Stream(bool want_headers, bool want_row_numbers) const {
  std::vector<std::shared_ptr<RowSequence>> row_sequences{GetRowSequence()};
  return {*this, std::move(row_sequences), want_headers, want_row_numbers, false};
}

internal::TableStreamAdaptor ClientTable::Stream(bool want_headers, bool want_row_numbers,
    std::shared_ptr<RowSequence> row_sequence) const {
  std::vector<std::shared_ptr<RowSequence>> row_sequences{std::move(row_sequence)};
  return {*this, std::move(row_sequences), want_headers, want_row_numbers, false};
}

internal::TableStreamAdaptor ClientTable::Stream(bool want_headers, bool want_row_numbers,
    std::vector<std::shared_ptr<RowSequence>> row_sequences) const {
  return {*this, std::move(row_sequences), want_headers, want_row_numbers, true};
}

std::string ClientTable::ToString(bool want_headers, bool want_row_numbers) const {
  SimpleOstringstream oss;
  oss << Stream(want_headers, want_row_numbers);
  return std::move(oss.str());
}

std::string ClientTable::ToString(bool want_headers, bool want_row_numbers,
    std::shared_ptr<RowSequence> row_sequence) const {
  SimpleOstringstream oss;
  oss << Stream(want_headers, want_row_numbers, std::move(row_sequence));
  return std::move(oss.str());
}

std::string ClientTable::ToString(bool want_headers, bool want_row_numbers,
    std::vector<std::shared_ptr<RowSequence>> row_sequences) const {
  SimpleOstringstream oss;
  oss << Stream(want_headers, want_row_numbers, std::move(row_sequences));
  return std::move(oss.str());
}

namespace internal {
std::ostream &operator<<(std::ostream &s, const TableStreamAdaptor &o) {
  const auto &t = o.table_;
  auto num_cols = t.NumColumns();
  auto which_cols = MakeReservedVector<size_t>(num_cols);
  for (size_t i = 0; i < num_cols; ++i) {
    which_cols.push_back(i);
  }
  PrintTableData(s, t, which_cols, o.row_sequences_, o.want_headers_, o.want_row_numbers_, o.highlight_cells_);
  return s;
}
}  // namespace internal

namespace {
class ArrayRowSequence final : public RowSequence {
public:
  [[nodiscard]]
  static std::shared_ptr<ArrayRowSequence> Create(std::shared_ptr<uint64_t[]> data,
      const uint64_t *begin, const uint64_t *end);

  ArrayRowSequence(std::shared_ptr<uint64_t[]> data, const uint64_t *begin, const uint64_t *end);
  ~ArrayRowSequence() final;

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
  std::shared_ptr<uint64_t[]> data_;
  const uint64_t *begin_ = nullptr;
  const uint64_t *end_ = nullptr;
};

class ChunkElementStreamer final {
public:
  ChunkElementStreamer(std::ostream &s, size_t index, bool null_flag, bool highlight) :
      s_(s), index_(index), null_flag_(null_flag), highlight_(highlight) {}

  template<typename T>
  void operator()(const T &chunk) const {
    if (highlight_) {
      s_ << '*';
    }

    if (null_flag_) {
      s_ << "null";
    } else {
      renderer_.Render(s_, chunk.data()[index_]);
    }

    if (highlight_) {
      s_ << '*';
    }
  }

private:
  std::ostream &s_;
  size_t index_ = 0;
  bool null_flag_ = false;
  bool highlight_ = false;
  ElementRenderer renderer_ = {};
};

struct RowSequenceState {
  explicit RowSequenceState(RowSequenceIterator iterator, size_t chunk_size);
  RowSequenceState(RowSequenceState &&other) noexcept;
  ~RowSequenceState();

  RowSequenceIterator iterator_;
  std::optional<uint64_t> currentValue_;
  std::unique_ptr<bool[]> isPresent_;
};

class RowMerger {
public:
  RowMerger(std::vector<RowSequenceIterator> iterators, size_t chunk_size);
  ~RowMerger();

  [[nodiscard]]
  std::shared_ptr<RowSequence> GetNextChunk();

  [[nodiscard]]
  bool IsCellPresent(size_t col_index, size_t chunk_offset) const;

private:
  size_t chunkSize_ = 0;
  std::vector<RowSequenceState> rowSequenceStates_;
  /**
   * This is a shared ponter because we share it with the ArrayRowSequence that we return
   * from GetNextChunk. Size = chunkSize_
   */
  std::shared_ptr<uint64_t[]> build_;
};

void PrintTableData(std::ostream &stream, const ClientTable &table,
    const std::vector<size_t> &which_cols,
    const std::vector<std::shared_ptr<RowSequence>> &row_sequences,
    bool want_headers, bool want_row_numbers, bool highlight_cells) {
  if (want_headers) {
    const char *separator = "";
    if (want_row_numbers) {
      stream << "[Row]";
      separator = "\t";
    }
    for (auto col_index : which_cols) {
      stream << separator << table.Schema()->Names()[col_index];
      separator = "\t";
    }
    stream << std::endl;
  }

  if (which_cols.empty() || row_sequences.empty()) {
    return;
  }

  constexpr const size_t kChunkSize = 8192;

  auto num_cols = which_cols.size();
  auto data_chunks = MakeReservedVector<AnyChunk>(num_cols);
  auto null_flag_chunks = MakeReservedVector<BooleanChunk>(num_cols);
  for (size_t i = 0; i < num_cols; ++i) {
    const auto &c = table.GetColumn(i);
    auto data_chunk = ChunkMaker::CreateChunkFor(*c, kChunkSize);
    auto null_flag_chunk = BooleanChunk::Create(kChunkSize);
    data_chunks.push_back(std::move(data_chunk));
    null_flag_chunks.push_back(std::move(null_flag_chunk));
  }

  auto iterators = MakeReservedVector<RowSequenceIterator>(row_sequences.size());
  for (const auto &rs : row_sequences) {
    iterators.push_back(rs->GetRowSequenceIterator());
  }

  RowMerger merger(std::move(iterators), kChunkSize);

  while (true) {
    auto chunk_of_rows = merger.GetNextChunk();
    auto this_size = chunk_of_rows->Size();
    if (this_size == 0) {
      break;
    }

    for (size_t i = 0; i < num_cols; ++i) {
      const auto col_num = which_cols[i];
      const auto &c = table.GetColumn(col_num);
      auto &data_chunk = data_chunks[col_num].Unwrap();
      auto &null_flag_chunk = null_flag_chunks[col_num];
      c->FillChunk(*chunk_of_rows, &data_chunk, &null_flag_chunk);
    }

    // To print out the optional row number
    auto rows_iter = chunk_of_rows->GetRowSequenceIterator();

    for (size_t chunk_offset = 0; chunk_offset < this_size; ++chunk_offset) {
      const char *separator = "";
      if (want_row_numbers) {
        uint64_t row_num;
        if (!rows_iter.TryGetNext(&row_num)) {
          throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Impossible: no more rows"));
        }
        stream << '[' << row_num << "] ";
        separator = "\t";
      }

      for (size_t i = 0; i < num_cols; ++i) {
        stream << separator;
        separator = "\t";
        auto null_flag = null_flag_chunks[i].data()[chunk_offset];
        auto highlight = highlight_cells && merger.IsCellPresent(i, chunk_offset);
        ChunkElementStreamer es(stream, chunk_offset, null_flag, highlight);
        data_chunks[i].Visit(es);
      }

      stream << '\n';
    }
  }
}

std::shared_ptr<ArrayRowSequence>
ArrayRowSequence::Create(std::shared_ptr<uint64_t[]> data, const uint64_t *begin,
    const uint64_t *end) {
  return std::make_shared<ArrayRowSequence>(std::move(data), begin, end);
}

ArrayRowSequence::ArrayRowSequence(std::shared_ptr<uint64_t[]> data, const uint64_t *begin,
    const uint64_t *end) : data_(std::move(data)), begin_(begin), end_(end) {}

ArrayRowSequence::~ArrayRowSequence() = default;

std::shared_ptr<RowSequence> ArrayRowSequence::Take(size_t size) const {
  auto size_to_use = std::min(size, this->Size());
  return Create(data_, begin_, begin_ + size_to_use);
}

std::shared_ptr<RowSequence> ArrayRowSequence::Drop(size_t size) const {
  auto size_to_use = std::min(size, this->Size());
  return Create(data_, begin_ + size_to_use, end_);
}

void ArrayRowSequence::ForEachInterval(const std::function<void(uint64_t, uint64_t)> &f) const {
  const auto *range_start = begin_;
  while (range_start != end_) {
    auto begin_key = *range_start;
    const auto *range_end = range_start + 1;
    auto end_key = begin_key + 1;
    while (range_end != end_ && *range_end == end_key) {
      ++range_end;
      ++end_key;
    }
    f(begin_key, end_key);
    range_start = range_end;
  }
}

RowSequenceState::RowSequenceState(RowSequenceIterator iterator, size_t chunk_size) :
  iterator_(std::move(iterator)), isPresent_(std::make_unique<bool[]>(chunk_size)) {
  uint64_t value;
  if (iterator_.TryGetNext(&value)) {
    currentValue_ = value;
  }
}
RowSequenceState::RowSequenceState(RowSequenceState &&other) noexcept = default;
RowSequenceState::~RowSequenceState() = default;

RowMerger::RowMerger(std::vector<RowSequenceIterator> iterators, size_t chunk_size) :
  chunkSize_(chunk_size) {

  rowSequenceStates_ = MakeReservedVector<RowSequenceState>(iterators.size());
  for (auto &iter : iterators) {
    rowSequenceStates_.emplace_back(std::move(iter), chunk_size);
  }
  build_ = std::shared_ptr<uint64_t[]>(new uint64_t[chunk_size]);
}

RowMerger::~RowMerger() = default;

std::shared_ptr<RowSequence> RowMerger::GetNextChunk() {
  size_t dest_index;
  uint64_t *buildp = build_.get();
  for (dest_index = 0; dest_index < chunkSize_; ++dest_index) {
    // Simplistic priority queue. If performance becomes an issue, this should be rewritten as a
    // legit priority queue.

    // First calculate the minimum value among the current values (if one exists)
    std::optional<uint64_t> min_value;
    for (const auto &rss : rowSequenceStates_) {
      const auto &cv = rss.currentValue_;
      if (!cv.has_value()) {
        continue;
      }
      if (!min_value.has_value() || *cv < *min_value) {
        min_value = *cv;
      }
    }

    // If no values found, we are done.
    if (!min_value.has_value()) {
      break;
    }

    // Store the minimum value, calculate the isPresent flag, and advance the iterators that match
    // the minimum value.
    buildp[dest_index] = *min_value;

    // Advance the iterators that match the minimum value.
    for (auto &rss : rowSequenceStates_) {
      auto &cv = rss.currentValue_;
      if (!cv.has_value() || *cv != *min_value) {
        rss.isPresent_[dest_index] = false;
        continue;
      }
      rss.isPresent_[dest_index] = true;

      // Bump to next if you can
      uint64_t value;
      if (rss.iterator_.TryGetNext(&value)) {
        cv = value;
      } else {
        cv.reset();
      }
    }
  }

  return ArrayRowSequence::Create(build_, build_.get(), build_.get() + dest_index);
}

bool RowMerger::IsCellPresent(size_t col_index, size_t chunk_offset) const {
  auto col_index_to_use = col_index < rowSequenceStates_.size() ? col_index : 0;
  return rowSequenceStates_[col_index_to_use].isPresent_[chunk_offset];
}
}  // namespace
}  // namespace deephaven::dhcore::clienttable
