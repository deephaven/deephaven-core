/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/ticking/immer_table_state.h"

#include <optional>
#include <utility>

#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/immerutil/abstract_flex_vector.h"
#include "deephaven/dhcore/ticking/shift_processor.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::ElementTypeId;
using deephaven::dhcore::visitElementTypeId;
using deephaven::dhcore::chunk::AnyChunk;
using deephaven::dhcore::chunk::Chunk;
using deephaven::dhcore::chunk::ChunkVisitor;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::column::ColumnSourceVisitor;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;
using deephaven::dhcore::container::RowSequenceIterator;
using deephaven::dhcore::subscription::ShiftProcessor;
using deephaven::dhcore::immerutil::AbstractFlexVectorBase;
using deephaven::dhcore::immerutil::GenericAbstractFlexVector;
using deephaven::dhcore::immerutil::NumericAbstractFlexVector;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::utility::makeReservedVector;
using deephaven::dhcore::utility::streamf;
using deephaven::dhcore::utility::stringf;

namespace deephaven::dhcore::ticking {
namespace {
class MyTable final : public ClientTable {
public:
  explicit MyTable(std::shared_ptr<Schema> schema,
      std::vector<std::shared_ptr<ColumnSource>> sources, size_t numRows);
  ~MyTable() final;

  std::shared_ptr<RowSequence> getRowSequence() const final;

  std::shared_ptr<ColumnSource> getColumn(size_t columnIndex) const final {
    return sources_[columnIndex];
  }

  size_t numRows() const final {
    return numRows_;
  }

  size_t numColumns() const final {
    return sources_.size();
  }

  std::shared_ptr<Schema> schema() const final {
    return schema_;
  }

private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<ColumnSource>> sources_;
  size_t numRows_ = 0;
};

std::vector<std::unique_ptr<AbstractFlexVectorBase>> makeEmptyFlexVectorsFromSchema(const Schema &schema);
std::unique_ptr<AbstractFlexVectorBase> makeFlexVectorFromColumnSource(const ColumnSource &source, size_t begin,
    size_t end);
void assertAllSame(size_t val0, size_t val1, size_t val2);
void assertLeq(size_t lhs, size_t rhs, const char *format);
}  // namespace

ImmerTableState::ImmerTableState(std::shared_ptr<Schema> schema) : schema_(std::move(schema)) {
  flexVectors_ = makeEmptyFlexVectorsFromSchema(*schema_);
}

ImmerTableState::~ImmerTableState() = default;

std::shared_ptr<RowSequence> ImmerTableState::addKeys(const RowSequence &rowsToAddKeySpace) {
  return spaceMapper_.addKeys(rowsToAddKeySpace);
}

void ImmerTableState::addData(const std::vector<std::shared_ptr<ColumnSource>> &sources,
    const std::vector<size_t> &begins, const std::vector<size_t> &ends, const RowSequence &rowsToAddIndexSpace) {
  auto ncols = sources.size();
  auto nrows = rowsToAddIndexSpace.size();
  assertAllSame(sources.size(), begins.size(), ends.size());
  assertLeq(ncols, flexVectors_.size(), "More columns provided than was expected (%o vs %o)");
  for (size_t i = 0; i != ncols; ++i) {
    assertLeq(nrows, ends[i] - begins[i], "Sources contain insufficient data (%o vs %o)");
  }
  auto addedData = makeReservedVector<std::unique_ptr<AbstractFlexVectorBase>>(ncols);
  for (size_t i = 0; i != ncols; ++i) {
    addedData.push_back(makeFlexVectorFromColumnSource(*sources[i], begins[i], begins[i] + nrows));
  }

  auto addChunk = [this, &addedData](uint64_t beginIndex, uint64_t endIndex) {
    auto size = endIndex - beginIndex;

    for (size_t i = 0; i < flexVectors_.size(); ++i) {
      auto &fv = flexVectors_[i];
      auto &ad = addedData[i];

      auto fvTemp = std::move(fv);
      // Give "fv" its original values up to 'beginIndex'; leave fvTemp with the rest.
      fv = fvTemp->take(beginIndex);
      fvTemp->inPlaceDrop(beginIndex);

      // Append the next 'size' values from 'addedData' to 'fv' and drop them from 'addedData'.
      fv->inPlaceAppend(ad->take(size));
      ad->inPlaceDrop(size);

      // Append the residual items back from 'fvTemp'.
      fv->inPlaceAppend(std::move(fvTemp));
    }
  };
  rowsToAddIndexSpace.forEachInterval(addChunk);
}

std::shared_ptr<RowSequence> ImmerTableState::erase(const RowSequence &rowsToRemoveKeySpace) {
  auto result = spaceMapper_.convertKeysToIndices(rowsToRemoveKeySpace);

  auto eraseChunk = [this](uint64_t beginKey, uint64_t endKey) {
    auto size = endKey - beginKey;
    auto beginIndex = spaceMapper_.eraseRange(beginKey, endKey);
    auto endIndex = beginIndex + size;

    for (auto &fv : flexVectors_) {
      auto fvTemp = std::move(fv);
      fv = fvTemp->take(beginIndex);
      fvTemp->inPlaceDrop(endIndex);
      fv->inPlaceAppend(std::move(fvTemp));
    }
  };
  rowsToRemoveKeySpace.forEachInterval(eraseChunk);
  return result;
}

std::shared_ptr<RowSequence> ImmerTableState::convertKeysToIndices(
    const RowSequence &rowsToModifyKeySpace) const {
  return spaceMapper_.convertKeysToIndices(rowsToModifyKeySpace);
}

void ImmerTableState::modifyData(size_t colNum, const ColumnSource &src, size_t begin, size_t end,
    const RowSequence &rowsToModifyIndexSpace) {
  auto nrows = rowsToModifyIndexSpace.size();
  auto sourceSize = end - begin;
  assertLeq(nrows, sourceSize, "Insufficient data in source");
  auto modifiedData = makeFlexVectorFromColumnSource(src, begin, begin + nrows);

  auto &fv = flexVectors_[colNum];
  auto modifyChunk = [&fv, &modifiedData](uint64_t beginIndex, uint64_t endIndex) {
    auto size = endIndex - beginIndex;
    auto fvTemp = std::move(fv);

    // Give 'fv' its original values up to 'beginIndex'; but drop values up to 'endIndex'
    fv = fvTemp->take(beginIndex);
    fvTemp->inPlaceDrop(endIndex);

    // Take 'size' values from 'modifiedData' and drop them from 'modifiedData'
    fv->inPlaceAppend(modifiedData->take(size));
    modifiedData->inPlaceDrop(size);

    // Append the residual items back from 'fvTemp'.
    fv->inPlaceAppend(std::move(fvTemp));
  };
  rowsToModifyIndexSpace.forEachInterval(modifyChunk);
}

void ImmerTableState::applyShifts(const RowSequence &firstIndex, const RowSequence &lastIndex,
    const RowSequence &destIndex) {
  auto processShift = [this](int64_t first, int64_t last, int64_t dest) {
    uint64_t begin = first;
    uint64_t end = ((uint64_t)last) + 1;
    uint64_t destBegin = dest;
    spaceMapper_.applyShift(begin, end, destBegin);
  };
  ShiftProcessor::applyShiftData(firstIndex, lastIndex, destIndex, processShift);
}

std::shared_ptr<ClientTable> ImmerTableState::snapshot() const {
  auto columnSources = makeReservedVector<std::shared_ptr<ColumnSource>>(flexVectors_.size());
  for (const auto &fv : flexVectors_) {
    columnSources.push_back(fv->makeColumnSource());
  }
  return std::make_shared<MyTable>(schema_, std::move(columnSources), spaceMapper_.cardinality());
}

namespace {
MyTable::MyTable(std::shared_ptr<Schema> schema, std::vector<std::shared_ptr<ColumnSource>> sources,
    size_t numRows) : schema_(std::move(schema)), sources_(std::move(sources)), numRows_(numRows) {}
MyTable::~MyTable() = default;

std::shared_ptr<RowSequence> MyTable::getRowSequence() const {
  // Need a utility for this
  RowSequenceBuilder rb;
  rb.addInterval(0, numRows_);
  return rb.build();
}

struct FlexVectorFromTypeMaker final {
  template<typename T>
  void operator()() {
    if constexpr(DeephavenTraits<T>::isNumeric) {
      result_ = std::make_unique<NumericAbstractFlexVector<T>>();
    } else {
      result_ = std::make_unique<GenericAbstractFlexVector<T>>();
    }
  }

  std::unique_ptr<AbstractFlexVectorBase> result_;
};

std::vector<std::unique_ptr<AbstractFlexVectorBase>> makeEmptyFlexVectorsFromSchema(const Schema &schema) {
  auto ncols = schema.numCols();
  auto result = makeReservedVector<std::unique_ptr<AbstractFlexVectorBase>>(ncols);
  for (auto typeId : schema.types()) {
    FlexVectorFromTypeMaker fvm;
    visitElementTypeId(typeId, &fvm);
    result.push_back(std::move(fvm.result_));
  }
  return result;
}

struct FlexVectorFromSourceMaker final : public ColumnSourceVisitor {
  void visit(const column::CharColumnSource &source) final {
    result_ = std::make_unique<NumericAbstractFlexVector<char16_t>>();
  }

  void visit(const column::Int8ColumnSource &source) final {
    result_ = std::make_unique<NumericAbstractFlexVector<int8_t>>();
  }

  void visit(const column::Int16ColumnSource &source) final {
    result_ = std::make_unique<NumericAbstractFlexVector<int16_t>>();
  }

  void visit(const column::Int32ColumnSource &source) final {
    result_ = std::make_unique<NumericAbstractFlexVector<int32_t>>();
  }

  void visit(const column::Int64ColumnSource &source) final {
    result_ = std::make_unique<NumericAbstractFlexVector<int64_t>>();
  }

  void visit(const column::FloatColumnSource &source) final {
    result_ = std::make_unique<NumericAbstractFlexVector<float>>();
  }

  void visit(const column::DoubleColumnSource &source) final {
    result_ = std::make_unique<NumericAbstractFlexVector<double>>();
  }

  void visit(const column::BooleanColumnSource &source) final {
    result_ = std::make_unique<GenericAbstractFlexVector<bool>>();
  }

  void visit(const column::StringColumnSource &source) final {
    result_ = std::make_unique<GenericAbstractFlexVector<std::string>>();
  }

  void visit(const column::DateTimeColumnSource &source) final {
    result_ = std::make_unique<GenericAbstractFlexVector<DateTime>>();
  }

  std::unique_ptr<AbstractFlexVectorBase> result_;
};

std::unique_ptr<AbstractFlexVectorBase> makeFlexVectorFromColumnSource(const ColumnSource &source, size_t begin,
    size_t end) {
  FlexVectorFromSourceMaker v;
  source.acceptVisitor(&v);
  v.result_->inPlaceAppendSource(source, begin, end);
  return std::move(v.result_);
}

void assertAllSame(size_t val0, size_t val1, size_t val2) {
  if (val0 != val1 || val0 != val2) {
    auto message = stringf("Sizes differ: %o vs %o vs %o", val0, val1, val2);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
}

void assertLeq(size_t lhs, size_t rhs, const char *format) {
  if (lhs <= rhs) {
    return;
  }
  auto message = stringf(format, lhs, rhs);
  throw std::runtime_error(message);
}
}  // namespace
}  // namespace deephaven::dhcore::ticking
