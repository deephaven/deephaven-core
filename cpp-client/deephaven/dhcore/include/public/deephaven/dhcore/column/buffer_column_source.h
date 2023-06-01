/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <numeric>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/dhcore/types.h"

namespace deephaven::dhcore::column {
namespace internal {
class BufferBackingStoreBase {
protected:
  explicit BufferBackingStoreBase(size_t size) : size_(size) {}
  size_t size_ = 0;
};

/**
 * This is the backing store used for the numeric types, which have the property that "null" is
 * represented by a special Deephaven constant.
 */
template<typename T>
class NumericBufferBackingStore : public BufferBackingStoreBase {
  typedef deephaven::dhcore::column::ColumnSourceImpls ColumnSourceImpls;
public:
  NumericBufferBackingStore(const T *start, size_t size) : BufferBackingStoreBase(size), start_(start) {
  }

  void get(size_t beginIndex, size_t endIndex, T *dest, bool *optionalNullFlags) const {
    ColumnSourceImpls::assertRangeValid(beginIndex, endIndex, size_);
    if (beginIndex == endIndex) {
      return;
    }

    const auto *srcBegin = start_ + beginIndex;
    const auto *srcEnd = start_ + endIndex;
    std::copy(srcBegin, srcEnd, dest);

    if (optionalNullFlags != nullptr) {
      for (const auto *sp = srcBegin; sp != srcEnd; ++sp) {
        auto isNull = *sp == deephaven::dhcore::DeephavenTraits<T>::NULL_VALUE;
        *optionalNullFlags++ = isNull;
      }
    }
  }

private:
  const T *start_ = nullptr;
};
}  // namespace internal

template<typename T>
class NumericBufferColumnSource final : public deephaven::dhcore::column::NumericColumnSource<T>,
    std::enable_shared_from_this<NumericBufferColumnSource<T>> {
  struct Private {
  };
  typedef deephaven::dhcore::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::dhcore::chunk::Chunk Chunk;
  typedef deephaven::dhcore::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::dhcore::column::ColumnSourceImpls ColumnSourceImpls;
  typedef deephaven::dhcore::column::ColumnSourceVisitor ColumnSourceVisitor;
  typedef deephaven::dhcore::container::RowSequence RowSequence;

public:
  static std::shared_ptr<NumericBufferColumnSource> create(const T *start, size_t size) {
    return std::make_shared<NumericBufferColumnSource<T>>(Private(), start, size);
  }

  static std::shared_ptr<NumericBufferColumnSource> createUntyped(const void *start, size_t size) {
    const auto *typedStart = static_cast<const T*>(start);
    return create(typedStart, size);
  }

  NumericBufferColumnSource(Private, const T* start, size_t size) : data_(start, size) {}
  ~NumericBufferColumnSource() = default;

  void fillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optionalNullFlags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::fillChunk<chunkType_t>(rows, dest, optionalNullFlags, data_);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest, BooleanChunk *optionalNullFlags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::fillChunkUnordered<chunkType_t>(rowKeys, dest, optionalNullFlags, data_);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

private:
  internal::NumericBufferBackingStore<T> data_;
};

// Convenience typedefs
typedef NumericBufferColumnSource<char16_t> CharBufferColumnSource;
typedef NumericBufferColumnSource<int8_t> Int8BufferColumnSource;
typedef NumericBufferColumnSource<int16_t> Int16BufferColumnSource;
typedef NumericBufferColumnSource<int32_t> Int32BufferColumnSource;
typedef NumericBufferColumnSource<int64_t> Int64BufferColumnSource;
typedef NumericBufferColumnSource<float> FloatBufferColumnSource;
typedef NumericBufferColumnSource<double> DoubleBufferColumnSource;
}  // namespace deephaven::dhcore::column
