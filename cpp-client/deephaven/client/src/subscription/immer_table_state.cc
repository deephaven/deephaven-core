/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/immer_table_state.h"

#include <optional>
#include <utility>

#include "deephaven/client/arrowutil/arrow_visitors.h"
#include "deephaven/client/chunk/chunk.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/immerutil/abstract_flex_vector.h"
#include "deephaven/client/subscription/shift_processor.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::arrowutil::ArrowTypeVisitor;
using deephaven::client::arrowutil::ArrowArrayTypeVisitor;
using deephaven::client::arrowutil::isNumericType;
using deephaven::client::chunk::Int64Chunk;
using deephaven::client::column::ColumnSource;
using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;
using deephaven::client::container::RowSequenceIterator;
using deephaven::client::subscription::ShiftProcessor;
using deephaven::client::immerutil::AbstractFlexVectorBase;
using deephaven::client::immerutil::GenericAbstractFlexVector;
using deephaven::client::immerutil::NumericAbstractFlexVector;
using deephaven::client::table::Schema;
using deephaven::client::table::Table;
using deephaven::client::utility::ColumnDefinitions;
using deephaven::client::utility::makeReservedVector;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven::client::subscription {
namespace {
class MyTable final : public Table {
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

  const Schema &schema() const final {
    return *schema_;
  }

private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<ColumnSource>> sources_;
  size_t numRows_ = 0;
};

std::vector<std::unique_ptr<AbstractFlexVectorBase>> makeFlexVectorsFromColDefs(
    const ColumnDefinitions &colDefs);
std::vector<std::unique_ptr<AbstractFlexVectorBase>> makeFlexVectorsFromArrays(
    const std::vector<std::shared_ptr<arrow::Array>> &arrays);
}  // namespace

ImmerTableState::ImmerTableState(std::shared_ptr<ColumnDefinitions> colDefs) :
    colDefs_(std::move(colDefs)) {
  schema_ = std::make_shared<Schema>(colDefs_->vec());
  flexVectors_ = makeFlexVectorsFromColDefs(*colDefs_);
}

ImmerTableState::~ImmerTableState() = default;

std::shared_ptr<RowSequence> ImmerTableState::addKeys(const RowSequence &rowsToAddKeySpace) {
  return spaceMapper_.addKeys(rowsToAddKeySpace);
}

void ImmerTableState::addData(const std::vector<std::shared_ptr<arrow::Array>> &data,
    const RowSequence &rowsToAddIndexSpace) {
  auto ncols = data.size();
  if (ncols != flexVectors_.size()) {
    throw std::runtime_error(stringf("ncols != flexVectors_.size() (%o != %o)",
        ncols, flexVectors_.size()));
  }
  auto addedData = makeFlexVectorsFromArrays(data);

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
  rowsToAddIndexSpace.forEachChunk(addChunk);
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
  rowsToRemoveKeySpace.forEachChunk(eraseChunk);
  return result;
}

std::vector<std::shared_ptr<RowSequence>> ImmerTableState::modifyKeys(
    const std::vector<std::shared_ptr<RowSequence>> &rowsToModifyKeySpace) {
  auto ncols = rowsToModifyKeySpace.size();
  auto result = makeReservedVector<std::shared_ptr<RowSequence>>(ncols);
  for (const auto &rs : rowsToModifyKeySpace) {
    result.push_back(spaceMapper_.convertKeysToIndices(*rs));
  }
  return result;
}

void ImmerTableState::modifyData(const std::vector<std::shared_ptr<arrow::Array>> &data,
    const std::vector<std::shared_ptr<RowSequence>> &rowsToModifyIndexSpace) {
  auto ncols = data.size();
  if (ncols != rowsToModifyIndexSpace.size()) {
    throw std::runtime_error(stringf("data.size() != rowsToModifyIndexSpace.size() (%o != %o)",
        ncols, rowsToModifyIndexSpace.size()));
  }
  auto modifiedData = makeFlexVectorsFromArrays(data);

  for (size_t i = 0; i < ncols; ++i) {
    modifyColumn(i, std::move(modifiedData[i]), *rowsToModifyIndexSpace[i]);
  }
}

void ImmerTableState::modifyColumn(size_t colNum,
    std::unique_ptr<AbstractFlexVectorBase> modifiedData,
    const RowSequence &rowsToModifyIndexSpace) {
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
  rowsToModifyIndexSpace.forEachChunk(modifyChunk);
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

std::shared_ptr<Table> ImmerTableState::snapshot() const {
  auto columnSources = makeReservedVector<std::shared_ptr<ColumnSource>>(flexVectors_.size());
  for (const auto &fv : flexVectors_) {
    columnSources.push_back(fv->makeColumnSource());
  }
  return std::make_shared<MyTable>(schema_, std::move(columnSources), spaceMapper_.size());
}

namespace {

MyTable::MyTable(std::shared_ptr<Schema> schema, std::vector<std::shared_ptr<ColumnSource>> sources,
    size_t numRows) : schema_(std::move(schema)), sources_(std::move(sources)), numRows_(numRows) {}
MyTable::~MyTable() = default;

std::shared_ptr<RowSequence> MyTable::getRowSequence() const {
  // Need a utility for this
  RowSequenceBuilder rb;
  rb.addRange(0, numRows_);
  return rb.build();
}

struct FlexVectorMaker final {
  template<typename T>
  void operator()() {
    if constexpr(isNumericType<T>()) {
      result_ = std::make_unique<NumericAbstractFlexVector<T>>();
    } else {
      result_ = std::make_unique<GenericAbstractFlexVector<T>>();
    }
  }

  std::unique_ptr<AbstractFlexVectorBase> result_;
};

std::vector<std::unique_ptr<AbstractFlexVectorBase>> makeFlexVectorsFromColDefs(
    const ColumnDefinitions &colDefs) {
  auto ncols = colDefs.vec().size();
  auto result = makeReservedVector<std::unique_ptr<AbstractFlexVectorBase>>(ncols);
  for (const auto &colDef : colDefs.vec()) {
    ArrowTypeVisitor<FlexVectorMaker> v;
    okOrThrow(DEEPHAVEN_EXPR_MSG(colDef.second->Accept(&v)));
    result.push_back(std::move(v.inner().result_));
  }
  return result;
}

std::vector<std::unique_ptr<AbstractFlexVectorBase>> makeFlexVectorsFromArrays(
    const std::vector<std::shared_ptr<arrow::Array>> &arrays) {
  auto ncols = arrays.size();
  auto result = makeReservedVector<std::unique_ptr<AbstractFlexVectorBase>>(ncols);
  for (const auto &a : arrays) {
    ArrowArrayTypeVisitor<FlexVectorMaker> v;
    okOrThrow(DEEPHAVEN_EXPR_MSG(a->Accept(&v)));
    v.inner().result_->inPlaceAppendArrow(*a);
    result.push_back(std::move(v.inner().result_));
  }
  return result;
}
}  // namespace
}  // namespace deephaven::client::subscription
