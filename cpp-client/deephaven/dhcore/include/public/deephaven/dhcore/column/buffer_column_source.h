/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
 * represented By a special Deephaven constant.
 */
template<typename T>
class NumericBufferBackingStore : public BufferBackingStoreBase {
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
public:
  NumericBufferBackingStore(const T *start, size_t size) : BufferBackingStoreBase(size), start_(start) {
  }

  void Get(size_t begin_index, size_t end_index, T *dest, bool *optional_null_flags) const {
    ColumnSourceImpls::AssertRangeValid(begin_index, end_index, size_);
    if (begin_index == end_index) {
      return;
    }

    const auto *src_begin = start_ + begin_index;
    const auto *src_end = start_ + end_index;
    std::copy(src_begin, src_end, dest);

    if (optional_null_flags != nullptr) {
      for (const auto *sp = src_begin; sp != src_end; ++sp) {
        auto is_null = *sp == deephaven::dhcore::DeephavenTraits<T>::kNullValue;
        *optional_null_flags++ = is_null;
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
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;
  using RowSequence = deephaven::dhcore::container::RowSequence;

public:
  static std::shared_ptr<NumericBufferColumnSource> Create(const T *start, size_t size) {
    return std::make_shared<NumericBufferColumnSource<T>>(Private(), start, size);
  }

  static std::shared_ptr<NumericBufferColumnSource> CreateUntyped(const void *start, size_t size) {
    const auto *typed_start = static_cast<const T*>(start);
    return Create(typed_start, size);
  }

  NumericBufferColumnSource(Private, const T* start, size_t size) : data_(start, size) {}
  ~NumericBufferColumnSource() = default;

  void FillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optional_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillChunk<chunkType_t>(rows, dest, optional_null_flags, data_);
  }

  void FillChunkUnordered(const UInt64Chunk &row_keys, Chunk *dest, BooleanChunk *optional_null_flags) const final {
    typedef typename
    deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillChunkUnordered<chunkType_t>(row_keys, dest, optional_null_flags, data_);
  }

  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

private:
  internal::NumericBufferBackingStore<T> data_;
};

// Convenience usings
using CharBufferColumnSource = NumericBufferColumnSource<char16_t>;
using Int8BufferColumnSource =NumericBufferColumnSource<int16_t>;
using Int32BufferColumnSource = NumericBufferColumnSource<int32_t>;
using Int64BufferColumnSource = NumericBufferColumnSource<int64_t>;
using FloatBufferColumnSource = NumericBufferColumnSource<float>;
using DoubleBufferColumnSource = NumericBufferColumnSource<double>;
}  // namespace deephaven::dhcore::column
