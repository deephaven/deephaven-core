/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/classic_table_state.h"

#include <memory>
#include "deephaven/client/arrowutil/arrow_visitors.h"
#include "deephaven/client/chunk/chunk_filler.h"
#include "deephaven/client/chunk/chunk_maker.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/column/array_column_source.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/subscription/shift_processor.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::arrowutil::ArrowTypeVisitor;
using deephaven::client::arrowutil::isNumericType;
using deephaven::client::chunk::BooleanChunk;
using deephaven::client::chunk::ChunkFiller;
using deephaven::client::chunk::ChunkMaker;
using deephaven::client::chunk::UInt64Chunk;
using deephaven::client::column::ColumnSource;
using deephaven::client::column::GenericArrayColumnSource;
using deephaven::client::column::MutableColumnSource;
using deephaven::client::column::NumericArrayColumnSource;
using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;
using deephaven::client::table::Schema;
using deephaven::client::table::Table;
using deephaven::client::utility::ColumnDefinitions;
using deephaven::client::utility::makeReservedVector;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven::client::subscription {
namespace {
void mapShifter(uint64_t begin, uint64_t end, uint64_t dest, std::map<uint64_t, uint64_t> *map);

std::vector<std::shared_ptr<MutableColumnSource>>
makeColumnSources(const ColumnDefinitions &colDefs);

class TableView final : public Table {
public:
  TableView(std::shared_ptr<Schema> schema, std::vector<std::shared_ptr<MutableColumnSource>> columns,
      std::shared_ptr<std::map<uint64_t, uint64_t>> redirection);
  ~TableView() final;

  std::shared_ptr<RowSequence> getRowSequence() const final;

  std::shared_ptr<ColumnSource> getColumn(size_t columnIndex) const final {
    // This is actually wrong. It needs to return a redirected column.
    // return columns_[columnIndex];
    throw std::runtime_error("TODO(kosak): need to make a redirected column.");
  }

  size_t numRows() const final {
    return redirection_->size();
  }

  size_t numColumns() const final {
    return columns_.size();
  }

  const Schema &schema() const final {
    return *schema_;
  }

private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<MutableColumnSource>> columns_;
  std::shared_ptr<std::map<uint64_t, uint64_t>> redirection_;
};

class UnwrappedTableView final : public Table {
public:
  UnwrappedTableView(std::shared_ptr<Schema> schema,
      std::vector<std::shared_ptr<MutableColumnSource>> columns,
      size_t numRows);
  ~UnwrappedTableView() final;

  std::shared_ptr<RowSequence> getRowSequence() const final;

  std::shared_ptr<ColumnSource> getColumn(size_t columnIndex) const final {
    return columns_[columnIndex];
  }

  size_t numRows() const final {
    return numRows_;
  }

  size_t numColumns() const override {
    return columns_.size();
  }

  const Schema &schema() const final {
    return *schema_;
  }

private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<MutableColumnSource>> columns_;
  size_t numRows_ = 0;
};
}

ClassicTableState::ClassicTableState(const ColumnDefinitions &colDefs) :
    redirection_(std::make_shared<std::map<uint64_t, uint64_t>>()) {
  columns_ = makeColumnSources(colDefs);
}

ClassicTableState::~ClassicTableState() = default;

UInt64Chunk ClassicTableState::erase(const RowSequence &rowsToRemoveKeySpace) {
  auto nrows = rowsToRemoveKeySpace.size();
  auto result = UInt64Chunk::create(nrows);
  auto *destp = result.data();
  auto removeRange = [this, &destp](uint64_t beginKey, uint64_t endKey) {
    auto beginp = redirection_->find(beginKey);
    if (beginp == redirection_->end()) {
      throw std::runtime_error(stringf("Can't find beginKey %o", beginKey));
    }

    auto currentp = beginp;
    for (auto current = beginKey; current != endKey; ++current) {
      if (currentp->first != current) {
        throw std::runtime_error(stringf("Can't find key %o", current));
      }
      *destp = currentp->second;
      ++destp;
      ++currentp;
    }
    redirection_->erase(beginp, currentp);
  };
  rowsToRemoveKeySpace.forEachChunk(removeRange);
  return result;
}

UInt64Chunk ClassicTableState::addKeys(const RowSequence &addedRowsKeySpace) {
  // In order to give back an ordered row sequence (because at the moment we don't have an
  // unordered row sequence), we sort the keys we're going to reuse.
  auto nrows = addedRowsKeySpace.size();
  auto numKeysToReuse = std::min(nrows, slotsToReuse_.size());
  auto reuseBegin = slotsToReuse_.end() - static_cast<ssize_t>(numKeysToReuse);
  auto reuseEnd = slotsToReuse_.end();
  std::sort(reuseBegin, reuseEnd);

  auto reuseCurrent = reuseBegin;

  auto result = UInt64Chunk::create(nrows);
  auto *destp = result.data();
  auto addRange = [this, &reuseCurrent, reuseEnd, &destp](uint64_t beginKey, uint64_t endKey) {
    for (auto current = beginKey; current != endKey; ++current) {
      uint64_t keyPositionSpace;
      if (reuseCurrent != reuseEnd) {
        keyPositionSpace = *reuseCurrent++;
      } else {
        keyPositionSpace = redirection_->size();
      }
      auto result = redirection_->insert(std::make_pair(current, keyPositionSpace));
      if (!result.second) {
        throw std::runtime_error(stringf("Can't add because key %o already exists", current));
      }
      *destp++ = keyPositionSpace;
    }
  };
  addedRowsKeySpace.forEachChunk(addRange);
  slotsToReuse_.erase(reuseBegin, reuseEnd);
  return result;
}

void ClassicTableState::addData(const std::vector<std::shared_ptr<arrow::Array>> &data,
    const UInt64Chunk &rowsToAddIndexSpace) {
  auto ncols = data.size();
  auto nrows = rowsToAddIndexSpace.size();
  auto sequentialRows = RowSequence::createSequential(0, nrows);
  auto nullFlags = BooleanChunk::create(nrows);
  for (size_t i = 0; i < ncols; ++i) {
    const auto &src = *data[i];
    auto *dest = columns_[i].get();
    auto anyChunk = ChunkMaker::createChunkFor(*dest, nrows);
    auto &dataChunk = anyChunk.unwrap();
    ChunkFiller::fillChunk(src, *sequentialRows, &dataChunk, &nullFlags);
    dest->fillFromChunkUnordered(dataChunk, &nullFlags, rowsToAddIndexSpace);
  }
}

void ClassicTableState::applyShifts(const RowSequence &firstIndex, const RowSequence &lastIndex,
    const RowSequence &destIndex) {
  auto *redir = redirection_.get();
  auto processShift = [redir](uint64_t first, uint64_t last, uint64_t dest) {
    uint64_t begin = first;
    uint64_t end = ((uint64_t) last) + 1;
    uint64_t destBegin = dest;
    mapShifter(begin, end, destBegin, redir);
  };
  ShiftProcessor::applyShiftData(firstIndex, lastIndex, destIndex, processShift);
}

std::shared_ptr<Table> ClassicTableState::snapshot() const {
  return std::make_shared<TableView>(schema_, columns_, redirection_);
}

std::shared_ptr<Table> ClassicTableState::snapshotUnwrapped() const {
  return std::make_shared<UnwrappedTableView>(schema_, columns_, redirection_->size());
}

std::vector<UInt64Chunk> ClassicTableState::modifyKeys(
    const std::vector<std::shared_ptr<RowSequence>> &rowsToModifyKeySpace) {
  auto nrows = rowsToModifyKeySpace.size();
  auto sequentialRows = RowSequence::createSequential(0, nrows);

  auto ncols = rowsToModifyKeySpace.size();
  auto result = makeReservedVector<UInt64Chunk>(ncols);
  for (size_t i = 0; i < ncols; ++i) {
    auto rowSequence = modifyKeysHelper(*rowsToModifyKeySpace[i]);
    result.push_back(std::move(rowSequence));
  }
  return result;
}

UInt64Chunk ClassicTableState::modifyKeysHelper(
    const RowSequence &rowsToModifyKeySpace) {
  auto nrows = rowsToModifyKeySpace.size();
  auto result = UInt64Chunk::create(nrows);
  auto *destp = result.data();
  auto modifyRange = [this, &destp](uint64_t beginKey, uint64_t endKey) {
    auto beginp = redirection_->find(beginKey);
    if (beginp == redirection_->end()) {
      throw std::runtime_error(stringf("Can't find beginKey %o", beginKey));
    }

    auto currentp = beginp;
    for (auto current = beginKey; current != endKey; ++current) {
      if (currentp->first != current) {
        throw std::runtime_error(stringf("Can't find key %o", current));
      }
      *destp = currentp->second;
      ++currentp;
      ++destp;
    }
  };
  rowsToModifyKeySpace.forEachChunk(modifyRange);
  if (destp != result.data() + nrows) {
    throw std::runtime_error("destp != result->data() + nrows");
  }
  return result;
}

void ClassicTableState::modifyData(const std::vector<std::shared_ptr<arrow::Array>> &src,
    const std::vector<UInt64Chunk> &rowsToModifyIndexSpace) {
  auto ncols = rowsToModifyIndexSpace.size();
  for (size_t i = 0; i < ncols; ++i) {
    const auto &rows = rowsToModifyIndexSpace[i];
    const auto &srcArray = *src[i];
    auto *destCol = columns_[i].get();
    auto nrows = rows.size();
    auto sequentialRows = RowSequence::createSequential(0, nrows);
    auto anyChunk = ChunkMaker::createChunkFor(*destCol, nrows);
    auto &chunk = anyChunk.unwrap();
    auto nullFlags = BooleanChunk::create(nrows);
    ChunkFiller::fillChunk(srcArray, *sequentialRows, &chunk, &nullFlags);
    destCol->fillFromChunkUnordered(chunk, &nullFlags, rows);
  }
}

namespace {
void mapShifter(uint64_t begin, uint64_t end, uint64_t dest, std::map<uint64_t, uint64_t> *map) {
  if (dest < begin) {
    // dest < begin, so shift down, moving forwards.
    auto delta = begin - dest;
    auto currentp = map->lower_bound(begin);
    // currentp points to map->end(), or to the first key >= begin
    while (true) {
      if (currentp == map->end() || currentp->first >= end) {
        return;
      }
      auto nextp = std::next(currentp);
      auto node = map->extract(currentp);
      auto newKey = node.key() - delta;
//      streamf(std::cerr, "Shifting down, working forwards, moving key from %o to %o\n", node.key(),
//          newKey);
      node.key() = newKey;
      map->insert(std::move(node));
      currentp = nextp;
    }
    return;
  }

  // dest >= begin, so shift up, moving backwards.
  auto delta = dest - begin;
  auto currentp = map->lower_bound(end);
  // currentp points to map->begin(), or to the first key >= end
  if (currentp == map->begin()) {
    return;
  }
  --currentp;
  // now currentp points to the last key < end
  while (true) {
    if (currentp->first < begin) {
      return;
    }
    // using map->end() as a sentinel, sort of viewing it as "wrapping around".
    auto nextp = currentp != map->begin() ? std::prev(currentp) : map->end();
    auto node = map->extract(currentp);
    auto newKey = node.key() + delta;
//    streamf(std::cerr, "Shifting up, working backwards, moving key from %o to %o\n", node.key(),
//        newKey);
    node.key() = newKey;
    map->insert(std::move(node));
    if (nextp == map->end()) {
      return;
    }
    currentp = nextp;
  }
}

struct ColumnSourceMaker final {
  template<typename T>
  void operator()() {
    if constexpr(isNumericType<T>()) {
      columnSource_ = NumericArrayColumnSource<T>::create();
    } else {
      columnSource_ = GenericArrayColumnSource<T>::create();
    }
  }

  std::shared_ptr<MutableColumnSource> columnSource_;
};

std::vector<std::shared_ptr<MutableColumnSource>> makeColumnSources(const ColumnDefinitions &colDefs) {
  std::vector<std::shared_ptr<MutableColumnSource>> result;
  for (const auto &[name, arrowType] : colDefs.vec()) {
    ArrowTypeVisitor<ColumnSourceMaker> v;
    okOrThrow(DEEPHAVEN_EXPR_MSG(arrowType->Accept(&v)));
    result.push_back(std::move(v.inner().columnSource_));
  }

  return result;
}

TableView::TableView(std::shared_ptr<Schema> schema,
    std::vector<std::shared_ptr<MutableColumnSource>> columns,
    std::shared_ptr<std::map<uint64_t, uint64_t>> redirection) : schema_(std::move(schema)),
    columns_(std::move(columns)), redirection_(std::move(redirection)) {}
TableView::~TableView() = default;

std::shared_ptr<RowSequence> TableView::getRowSequence() const {
  throw std::runtime_error("TODO(kosak)");
}

UnwrappedTableView::UnwrappedTableView(std::shared_ptr<Schema> schema,
    std::vector<std::shared_ptr<MutableColumnSource>> columns, size_t numRows) :
    schema_(std::move(schema)), columns_(std::move(columns)), numRows_(numRows) {}
UnwrappedTableView::~UnwrappedTableView() = default;

std::shared_ptr<RowSequence> UnwrappedTableView::getRowSequence() const {
  throw std::runtime_error("TODO(kosak)");
}
}  // namespace
}  // namespace deephaven::client::subscription
