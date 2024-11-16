/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
#include "deephaven/third_party/fmt/format.h"

using deephaven::dhcore::ElementTypeId;
using deephaven::dhcore::VisitElementTypeId;
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
using deephaven::dhcore::utility::MakeReservedVector;

namespace deephaven::dhcore::ticking {
namespace {
class MyTable final : public ClientTable {
  // Because we have a name clash with a member function called Schema()
  using SchemaType = deephaven::dhcore::clienttable::Schema;

public:
  explicit MyTable(std::shared_ptr<SchemaType> schema,
      std::vector<std::shared_ptr<ColumnSource>> sources, size_t num_rows);
  ~MyTable() final;

  [[nodiscard]]
  std::shared_ptr<RowSequence> GetRowSequence() const final;

  [[nodiscard]]
  std::shared_ptr<ColumnSource> GetColumn(size_t column_index) const final {
    if (column_index >= sources_.size()) {
      auto message = fmt::format("Requested column index {} >= num columns {}", column_index,
          sources_.size());
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
    return sources_[column_index];
  }

  [[nodiscard]]
  size_t NumRows() const final {
    return numRows_;
  }

  [[nodiscard]]
  size_t NumColumns() const final {
    return sources_.size();
  }

  [[nodiscard]]
  std::shared_ptr<SchemaType> Schema() const final {
    return schema_;
  }

private:
  std::shared_ptr<SchemaType> schema_;
  std::vector<std::shared_ptr<ColumnSource>> sources_;
  size_t numRows_ = 0;
};

std::vector<std::unique_ptr<AbstractFlexVectorBase>> MakeEmptyFlexVectorsFromSchema(const Schema &schema);
std::unique_ptr<AbstractFlexVectorBase> MakeFlexVectorFromColumnSource(const ColumnSource &source, size_t begin,
    size_t end);
void AssertAllSame(size_t val0, size_t val1, size_t val2);
void AssertLeq(size_t lhs, size_t rhs, const char *format);
}  // namespace

ImmerTableState::ImmerTableState(std::shared_ptr<Schema> schema) : schema_(std::move(schema)) {
  flexVectors_ = MakeEmptyFlexVectorsFromSchema(*schema_);
}

ImmerTableState::~ImmerTableState() = default;

std::shared_ptr<RowSequence> ImmerTableState::AddKeys(const RowSequence &rows_to_add_key_space) {
  return spaceMapper_.AddKeys(rows_to_add_key_space);
}

void ImmerTableState::AddData(const std::vector<std::shared_ptr<ColumnSource>> &sources,
    const std::vector<size_t> &begins, const std::vector<size_t> &ends,
    const RowSequence &rows_to_add_index_space) {
  auto ncols = sources.size();
  auto nrows = rows_to_add_index_space.Size();
  AssertAllSame(sources.size(), begins.size(), ends.size());
  AssertLeq(ncols, flexVectors_.size(), "More columns provided than was expected ({} vs {})");
  for (size_t i = 0; i != ncols; ++i) {
      AssertLeq(nrows, ends[i] - begins[i], "Sources contain insufficient data ({} vs {})");
  }
  auto added_data = MakeReservedVector<std::unique_ptr<AbstractFlexVectorBase>>(ncols);
  for (size_t i = 0; i != ncols; ++i) {
    added_data.push_back(MakeFlexVectorFromColumnSource(*sources[i], begins[i], begins[i] + nrows));
  }

  auto add_chunk = [this, &added_data](uint64_t begin_index, uint64_t end_index) {
    auto size = end_index - begin_index;

    for (size_t i = 0; i < flexVectors_.size(); ++i) {
      auto &fv = flexVectors_[i];
      auto &ad = added_data[i];

      auto fv_temp = std::move(fv);
      // Give "fv" its original values up to 'beginIndex'; leave fvTemp with the rest.
      fv = fv_temp->Take(begin_index);
      fv_temp->InPlaceDrop(begin_index);

      // Append the next 'size' values from 'addedData' to 'fv' and drop them from 'addedData'.
      fv->InPlaceAppend(ad->Take(size));
      ad->InPlaceDrop(size);

      // Append the residual items back from 'fvTemp'.
      fv->InPlaceAppend(std::move(fv_temp));
    }
  };
  rows_to_add_index_space.ForEachInterval(add_chunk);
}

std::shared_ptr<RowSequence> ImmerTableState::Erase(const RowSequence &rows_to_erase_key_space) {
  auto result = spaceMapper_.ConvertKeysToIndices(rows_to_erase_key_space);

  auto erase_chunk = [this](uint64_t begin_key, uint64_t end_key) {
    auto size = end_key - begin_key;
    auto begin_index = spaceMapper_.EraseRange(begin_key, end_key);
    auto end_index = begin_index + size;

    for (auto &fv : flexVectors_) {
      auto fv_temp = std::move(fv);
      fv = fv_temp->Take(begin_index);
      fv_temp->InPlaceDrop(end_index);
      fv->InPlaceAppend(std::move(fv_temp));
    }
  };
  rows_to_erase_key_space.ForEachInterval(erase_chunk);
  return result;
}

std::shared_ptr<RowSequence> ImmerTableState::ConvertKeysToIndices(
    const RowSequence &keys_row_space) const {
  return spaceMapper_.ConvertKeysToIndices(keys_row_space);
}

void ImmerTableState::ModifyData(size_t col_num, const ColumnSource &src, size_t begin, size_t end,
    const RowSequence &rows_to_modify_index_space) {
  auto nrows = rows_to_modify_index_space.Size();
  auto source_size = end - begin;
    AssertLeq(nrows, source_size, "Insufficient data in source ({} vs {})");
  auto modified_data = MakeFlexVectorFromColumnSource(src, begin, begin + nrows);

  auto &fv = flexVectors_[col_num];
  auto modify_chunk = [&fv, &modified_data](uint64_t begin_index, uint64_t end_index) {
    auto size = end_index - begin_index;
    auto fv_temp = std::move(fv);

    // Give 'fv' its original values up to 'beginIndex'; but drop values up to 'endIndex'
    fv = fv_temp->Take(begin_index);
    fv_temp->InPlaceDrop(end_index);

    // Take 'size' values from 'modifiedData' and drop them from 'modifiedData'
    fv->InPlaceAppend(modified_data->Take(size));
    modified_data->InPlaceDrop(size);

    // Append the residual items back from 'fvTemp'.
    fv->InPlaceAppend(std::move(fv_temp));
  };
  rows_to_modify_index_space.ForEachInterval(modify_chunk);
}

void ImmerTableState::ApplyShifts(const RowSequence &first_index, const RowSequence &last_index,
    const RowSequence &dest_index) {
  auto process_shift = [this](int64_t first, int64_t last, int64_t dest) {
    uint64_t begin = first;
    uint64_t end = static_cast<uint64_t>(last) + 1;
    uint64_t dest_begin = dest;
    spaceMapper_.ApplyShift(begin, end, dest_begin);
  };
  ShiftProcessor::ApplyShiftData(first_index, last_index, dest_index, process_shift);
}

std::shared_ptr<ClientTable> ImmerTableState::Snapshot() const {
  auto column_sources = MakeReservedVector<std::shared_ptr<ColumnSource>>(flexVectors_.size());
  for (const auto &fv : flexVectors_) {
    column_sources.push_back(fv->MakeColumnSource());
  }
  return std::make_shared<MyTable>(schema_, std::move(column_sources), spaceMapper_.Cardinality());
}

namespace {
MyTable::MyTable(std::shared_ptr<SchemaType> schema, std::vector<std::shared_ptr<ColumnSource>> sources,
    size_t num_rows) : schema_(std::move(schema)), sources_(std::move(sources)), numRows_(num_rows) {}
MyTable::~MyTable() = default;

std::shared_ptr<RowSequence> MyTable::GetRowSequence() const {
  // Need a utility for this
  RowSequenceBuilder rb;
  rb.AddInterval(0, numRows_);
  return rb.Build();
}

struct FlexVectorFromTypeMaker final {
  template<typename T>
  void operator()() {
    if constexpr(DeephavenTraits<T>::kIsNumeric) {
      result_ = std::make_unique<NumericAbstractFlexVector<T>>();
    } else {
      result_ = std::make_unique<GenericAbstractFlexVector<T>>();
    }
  }

  std::unique_ptr<AbstractFlexVectorBase> result_;
};

std::vector<std::unique_ptr<AbstractFlexVectorBase>> MakeEmptyFlexVectorsFromSchema(const Schema &schema) {
  auto ncols = schema.NumCols();
  auto result = MakeReservedVector<std::unique_ptr<AbstractFlexVectorBase>>(ncols);
  for (auto type_id : schema.Types()) {
    FlexVectorFromTypeMaker fvm;
    VisitElementTypeId(type_id, &fvm);
    result.push_back(std::move(fvm.result_));
  }
  return result;
}

struct FlexVectorFromSourceMaker final : public ColumnSourceVisitor {
  void Visit(const column::CharColumnSource &/*source*/) final {
    result_ = std::make_unique<NumericAbstractFlexVector<char16_t>>();
  }

  void Visit(const column::Int8ColumnSource &/*source*/) final {
    result_ = std::make_unique<NumericAbstractFlexVector<int8_t>>();
  }

  void Visit(const column::Int16ColumnSource &/*source*/) final {
    result_ = std::make_unique<NumericAbstractFlexVector<int16_t>>();
  }

  void Visit(const column::Int32ColumnSource &/*source*/) final {
    result_ = std::make_unique<NumericAbstractFlexVector<int32_t>>();
  }

  void Visit(const column::Int64ColumnSource &/*source*/) final {
    result_ = std::make_unique<NumericAbstractFlexVector<int64_t>>();
  }

  void Visit(const column::FloatColumnSource &/*source*/) final {
    result_ = std::make_unique<NumericAbstractFlexVector<float>>();
  }

  void Visit(const column::DoubleColumnSource &/*source*/) final {
    result_ = std::make_unique<NumericAbstractFlexVector<double>>();
  }

  void Visit(const column::BooleanColumnSource &/*source*/) final {
    result_ = std::make_unique<GenericAbstractFlexVector<bool>>();
  }

  void Visit(const column::StringColumnSource &/*source*/) final {
    result_ = std::make_unique<GenericAbstractFlexVector<std::string>>();
  }

  void Visit(const column::DateTimeColumnSource &/*source*/) final {
    result_ = std::make_unique<GenericAbstractFlexVector<DateTime>>();
  }

  void Visit(const column::LocalDateColumnSource &/*source*/) final {
    result_ = std::make_unique<GenericAbstractFlexVector<LocalDate>>();
  }

  void Visit(const column::LocalTimeColumnSource &/*source*/) final {
    result_ = std::make_unique<GenericAbstractFlexVector<LocalTime>>();
  }

  std::unique_ptr<AbstractFlexVectorBase> result_;
};

std::unique_ptr<AbstractFlexVectorBase> MakeFlexVectorFromColumnSource(const ColumnSource &source,
    size_t begin, size_t end) {
  FlexVectorFromSourceMaker v;
  source.AcceptVisitor(&v);
  v.result_->InPlaceAppendSource(source, begin, end);
  return std::move(v.result_);
}

void AssertAllSame(size_t val0, size_t val1, size_t val2) {
  if (val0 != val1 || val0 != val2) {
    auto message = fmt::format("Sizes differ: {} vs {} vs {}", val0, val1, val2);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
}

void AssertLeq(size_t lhs, size_t rhs, const char *format) {
  if (lhs <= rhs) {
    return;
  }
  auto message = fmt::format(format, lhs, rhs);
  throw std::runtime_error(message);
}
}  // namespace
}  // namespace deephaven::dhcore::ticking
