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
template<typename ARRAY_TYPE, typename ELEMENT_TYPE>
class NumericBackingStore {
  typedef deephaven::dhcore::column::ColumnSourceImpls ColumnSourceImpls;

public:
  explicit NumericBackingStore(const ARRAY_TYPE *arrowArray) : array_(arrowArray) {}

  void get(size_t beginIndex, size_t endIndex, ELEMENT_TYPE *dest, bool *optionalNullFlags) const {
    ColumnSourceImpls::assertRangeValid(beginIndex, endIndex, array_->length());
    for (auto i = beginIndex; i != endIndex; ++i) {
      auto element = (*array_)[i];
      ELEMENT_TYPE value;
      bool isNull;
      if (element.has_value()) {
        value = *element;
        isNull = false;
      } else {
        value = deephaven::dhcore::DeephavenTraits<ELEMENT_TYPE>::NULL_VALUE;
        isNull = true;
      }
      *dest++ = value;
      if (optionalNullFlags != nullptr) {
        *optionalNullFlags++ = isNull;
      }
    }
  }

private:
  const ARRAY_TYPE *array_ = nullptr;
};

template<typename ARRAY_TYPE, typename ELEMENT_TYPE>
class GenericBackingStore {
  typedef deephaven::dhcore::column::ColumnSourceImpls ColumnSourceImpls;
  typedef deephaven::dhcore::DateTime DateTime;
public:
  explicit GenericBackingStore(const ARRAY_TYPE *arrowArray) : array_(arrowArray) {}

  void get(size_t beginIndex, size_t endIndex, ELEMENT_TYPE *dest, bool *optionalNullFlags) const {
    ColumnSourceImpls::assertRangeValid(beginIndex, endIndex, array_->length());
    for (auto i = beginIndex; i != endIndex; ++i) {
      auto element = (*array_)[i];
      bool isNull;
      if (element.has_value()) {
        ArrowValueConverter::convert(*element, dest);
        isNull = false;
      } else {
        // placeholder
        *dest = ELEMENT_TYPE();
        isNull = true;
      }
      if (optionalNullFlags != nullptr) {
        *optionalNullFlags++ = isNull;
      }
    }
  }

private:
  const ARRAY_TYPE *array_ = nullptr;
};
}  // namespace internal

template<typename ARRAY_TYPE, typename ELEMENT_TYPE>
class NumericArrowColumnSource final : public deephaven::dhcore::column::NumericColumnSource<ELEMENT_TYPE> {
  struct Private {};
  /**
   * Alias.
   */
  typedef deephaven::dhcore::chunk::BooleanChunk BooleanChunk;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::chunk::Chunk Chunk;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::chunk::UInt64Chunk UInt64Chunk;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::column::ColumnSourceImpls ColumnSourceImpls;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::container::RowSequence RowSequence;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::column::ColumnSourceVisitor ColumnSourceVisitor;

public:
  static std::shared_ptr<NumericArrowColumnSource> create(const ARRAY_TYPE *arrowArray) {
    return std::make_shared<NumericArrowColumnSource>(Private(), arrowArray);
  }

  explicit NumericArrowColumnSource(Private, const ARRAY_TYPE *arrowArray) : backingStore_(arrowArray) {}

  void fillChunk(const RowSequence &rows, Chunk *destData, BooleanChunk *optionalDestNullFlags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<ELEMENT_TYPE>::type_t chunkType_t;
    ColumnSourceImpls::fillChunk<chunkType_t>(rows, destData, optionalDestNullFlags, backingStore_);
  }

  void fillChunkUnordered(const UInt64Chunk &rows, Chunk *destData, BooleanChunk *optionalDestNullFlags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<ELEMENT_TYPE>::type_t chunkType_t;
    ColumnSourceImpls::fillChunkUnordered<chunkType_t>(rows, destData, optionalDestNullFlags, backingStore_);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

private:
  internal::NumericBackingStore<ARRAY_TYPE, ELEMENT_TYPE> backingStore_;
};

template<typename ARRAY_TYPE, typename ELEMENT_TYPE>
class GenericArrowColumnSource final : public deephaven::dhcore::column::GenericColumnSource<ELEMENT_TYPE> {
  struct Private {};
  /**
   * Alias.
   */
  typedef deephaven::dhcore::chunk::BooleanChunk BooleanChunk;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::chunk::Chunk Chunk;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::chunk::UInt64Chunk UInt64Chunk;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::container::RowSequence RowSequence;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::column::ColumnSourceImpls ColumnSourceImpls;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::column::ColumnSourceVisitor ColumnSourceVisitor;

public:
  static std::shared_ptr<GenericArrowColumnSource> create(const ARRAY_TYPE *arrowArray) {
    return std::make_shared<GenericArrowColumnSource>(Private(), arrowArray);
  }

  explicit GenericArrowColumnSource(Private, const ARRAY_TYPE *arrowArray) : backingStore_(arrowArray) {}

  void fillChunk(const RowSequence &rows, Chunk *destData, BooleanChunk *optionalDestNullFlags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<ELEMENT_TYPE>::type_t chunkType_t;
    ColumnSourceImpls::fillChunk<chunkType_t>(rows, destData, optionalDestNullFlags, backingStore_);
  }
  void fillChunkUnordered(const UInt64Chunk &rows, Chunk *destData, BooleanChunk *optionalDestNullFlags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<ELEMENT_TYPE>::type_t chunkType_t;
    ColumnSourceImpls::fillChunkUnordered<chunkType_t>(rows, destData, optionalDestNullFlags, backingStore_);
  }
  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

private:
  internal::GenericBackingStore<ARRAY_TYPE, ELEMENT_TYPE> backingStore_;
};

typedef NumericArrowColumnSource<arrow::Int8Array, int8_t> ArrowInt8ColumnSource;
typedef NumericArrowColumnSource<arrow::Int16Array, int16_t> ArrowInt16ColumnSource;
typedef NumericArrowColumnSource<arrow::Int32Array, int32_t> ArrowInt32ColumnSource;
typedef NumericArrowColumnSource<arrow::Int64Array, int64_t> ArrowInt64ColumnSource;
typedef NumericArrowColumnSource<arrow::FloatArray, float> ArrowFloatColumnSource;
typedef NumericArrowColumnSource<arrow::DoubleArray, double> ArrowDoubleColumnSource;

typedef GenericArrowColumnSource<arrow::BooleanArray, bool> ArrowBooleanColumnSource;
typedef GenericArrowColumnSource<arrow::StringArray, std::string> ArrowStringColumnSource;
typedef GenericArrowColumnSource<arrow::TimestampArray, deephaven::dhcore::DateTime> ArrowDateTimeColumnSource;
}  // namespace deephaven::client::arrowutil
