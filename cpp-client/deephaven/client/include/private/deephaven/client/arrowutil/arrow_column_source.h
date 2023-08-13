/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <string>
#include <cstdint>
#include <arrow/array.h>
#include "deephaven/client/arrowutil/arrow_value_converter.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/dhcore/types.h"

namespace deephaven::client::arrowutil {
namespace internal {
template<typename ArrayType, typename ElementType>
class NumericBackingStore {
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;

public:
  explicit NumericBackingStore(const ArrayType *arrow_array) : array_(arrow_array) {}

  void Get(size_t begin_index, size_t end_index, ElementType *dest, bool *optional_null_flags) const {
    ColumnSourceImpls::AssertRangeValid(begin_index, end_index, array_->length());
    for (auto i = begin_index; i != end_index; ++i) {
      auto element = (*array_)[i];
      ElementType value;
      bool is_null;
      if (element.has_value()) {
        value = *element;
          is_null = false;
      } else {
        value = deephaven::dhcore::DeephavenTraits<ElementType>::kNullValue;
          is_null = true;
      }
      *dest++ = value;
      if (optional_null_flags != nullptr) {
        *optional_null_flags++ = is_null;
      }
    }
  }

private:
  const ArrayType *array_ = nullptr;
};

template<typename ArrayType, typename ElementType>
class GenericBackingStore {
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
  using DateTime = deephaven::dhcore::DateTime;
public:
  explicit GenericBackingStore(const ArrayType *arrow_array) : array_(arrow_array) {}

  void Get(size_t begin_index, size_t end_index, ElementType *dest, bool *optional_null_flags) const {
    ColumnSourceImpls::AssertRangeValid(begin_index, end_index, array_->length());
    for (auto i = begin_index; i != end_index; ++i) {
      auto element = (*array_)[i];
      bool is_null;
      if (element.has_value()) {
        ArrowValueConverter::Convert(*element, dest);
        is_null = false;
      } else {
        // placeholder
        *dest = ElementType();
        is_null = true;
      }
      if (optional_null_flags != nullptr) {
        *optional_null_flags++ = is_null;
      }
    }
  }

private:
  const ArrayType *array_ = nullptr;
};
}  // namespace internal

template<typename ArrayType, typename ElementType>
class NumericArrowColumnSource final : public deephaven::dhcore::column::NumericColumnSource<ElementType> {
  struct Private {};
  /**
   * Alias.
   */
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  /**
   * Alias.
   */
  using Chunk = deephaven::dhcore::chunk::Chunk;
  /**
   * Alias.
   */
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  /**
   * Alias.
   */
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
  /**
   * Alias.
   */
  using RowSequence = deephaven::dhcore::container::RowSequence;
  /**
   * Alias.
   */
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;

public:
  static std::shared_ptr<NumericArrowColumnSource> Create(std::shared_ptr<arrow::Array> storage,
      const ArrayType *arrow_array) {
    return std::make_shared<NumericArrowColumnSource>(Private(), std::move(storage), arrow_array);
  }

  explicit NumericArrowColumnSource(Private, std::shared_ptr<arrow::Array> storage, const ArrayType *arrow_array) :
      storage_(std::move(storage)), backingStore_(arrow_array) {}

  void FillChunk(const RowSequence &rows, Chunk *dest_data, BooleanChunk *optional_dest_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<ElementType>::type_t chunkType_t;
    ColumnSourceImpls::FillChunk<chunkType_t>(rows, dest_data, optional_dest_null_flags, backingStore_);
  }

  void FillChunkUnordered(const UInt64Chunk &rows, Chunk *dest_data, BooleanChunk *optional_dest_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<ElementType>::type_t chunkType_t;
    ColumnSourceImpls::FillChunkUnordered<chunkType_t>(rows, dest_data, optional_dest_null_flags,
                                                       backingStore_);
  }

  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

private:
  std::shared_ptr<arrow::Array> storage_;
  internal::NumericBackingStore<ArrayType, ElementType> backingStore_;
};

template<typename ArrayType, typename ElementType>
class GenericArrowColumnSource final : public deephaven::dhcore::column::GenericColumnSource<ElementType> {
  struct Private {};
  /**
   * Alias.
   */
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  /**
   * Alias.
   */
  using Chunk = deephaven::dhcore::chunk::Chunk;
  /**
   * Alias.
   */
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  /**
   * Alias.
   */
  using RowSequence = deephaven::dhcore::container::RowSequence;
  /**
   * Alias.
   */
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
  /**
   * Alias.
   */
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;

public:
  static std::shared_ptr<GenericArrowColumnSource> Create(std::shared_ptr<arrow::Array> storage,
      const ArrayType *arrow_array) {
    return std::make_shared<GenericArrowColumnSource>(Private(), std::move(storage), arrow_array);
  }

  explicit GenericArrowColumnSource(Private, std::shared_ptr<arrow::Array> storage, const ArrayType *arrow_array) :
      storage_(std::move(storage)), backingStore_(arrow_array) {}

  void FillChunk(const RowSequence &rows, Chunk *dest_data, BooleanChunk *optional_dest_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<ElementType>::type_t chunkType_t;
    ColumnSourceImpls::FillChunk<chunkType_t>(rows, dest_data, optional_dest_null_flags, backingStore_);
  }
  void FillChunkUnordered(const UInt64Chunk &rows, Chunk *dest_data, BooleanChunk *optional_dest_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<ElementType>::type_t chunkType_t;
    ColumnSourceImpls::FillChunkUnordered<chunkType_t>(rows, dest_data, optional_dest_null_flags,
                                                       backingStore_);
  }
  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

private:
  std::shared_ptr<arrow::Array> storage_;
  internal::GenericBackingStore<ArrayType, ElementType> backingStore_;
};

using ArrowInt8ColumnSource = NumericArrowColumnSource<arrow::Int8Array, int8_t>;
using ArrowInt16ColumnSource = NumericArrowColumnSource<arrow::Int16Array, int16_t>;
using ArrowInt32ColumnSource = NumericArrowColumnSource<arrow::Int32Array, int32_t>;
using ArrowInt64ColumnSource = NumericArrowColumnSource<arrow::Int64Array, int64_t>;
using ArrowFloatColumnSource = NumericArrowColumnSource<arrow::FloatArray, float>;
using ArrowDoubleColumnSource = NumericArrowColumnSource<arrow::DoubleArray, double>;

using ArrowBooleanColumnSource = GenericArrowColumnSource<arrow::BooleanArray, bool>;
using ArrowStringColumnSource = GenericArrowColumnSource<arrow::StringArray, std::string>;
using ArrowDateTimeColumnSource = GenericArrowColumnSource<arrow::TimestampArray, deephaven::dhcore::DateTime>;
}  // namespace deephaven::client::arrowutil
