/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/dhcore/container/row_sequence.h"
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

// This struct exists for backward compatibility. We will remove it when we update Cython
// to understand ElementType.
template<typename T>
struct TypeToElementType {
};

template<>
struct TypeToElementType<char16_t> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kChar;
};

template<>
struct TypeToElementType<int8_t> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kInt8;
};

template<>
struct TypeToElementType<int16_t> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kInt16;
};

template<>
struct TypeToElementType<int32_t> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kInt32;
};

template<>
struct TypeToElementType<int64_t> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kInt64;
};

template<>
struct TypeToElementType<float> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kFloat;
};

template<>
struct TypeToElementType<double> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kDouble;
};

template<>
struct TypeToElementType<bool> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kBool;
};

template<>
struct TypeToElementType<std::string> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kString;
};

template<>
struct TypeToElementType<deephaven::dhcore::DateTime> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kTimestamp;
};

template<>
struct TypeToElementType<deephaven::dhcore::LocalDate> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kLocalDate;
};

template<>
struct TypeToElementType<deephaven::dhcore::LocalTime> {
  static constexpr const ElementTypeId::Enum kElementTypeId = ElementTypeId::kLocalTime;
};
}  // namespace internal

template<typename T>
class NumericBufferColumnSource final : public deephaven::dhcore::column::GenericColumnSource<T>,
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
  static std::shared_ptr<NumericBufferColumnSource> Create(const ElementType &element_type,
      const T *start, size_t size) {
    return std::make_shared<NumericBufferColumnSource<T>>(Private(), element_type, start, size);
  }

  static std::shared_ptr<NumericBufferColumnSource> CreateUntyped(const void *start, size_t size) {
    auto type_id = internal::TypeToElementType<T>::kElementTypeId;
    auto element_type= ElementType::Of(type_id);
    const auto *typed_start = static_cast<const T*>(start);
    return Create(element_type, typed_start, size);
  }

  static std::shared_ptr<NumericBufferColumnSource> CreateUntyped(const ElementType &element_type,
      const void *start, size_t size) {
    const auto *typed_start = static_cast<const T*>(start);
    return Create(element_type, typed_start, size);
  }

  NumericBufferColumnSource(Private, const ElementType &element_type,
      const T* start, size_t size) : element_type_(element_type), data_(start, size) {}
  ~NumericBufferColumnSource() = default;

  void FillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optional_null_flags) const final {
    using chunkType_t = typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t;
    ColumnSourceImpls::FillChunk<chunkType_t>(rows, dest, optional_null_flags, data_);
  }

  void FillChunkUnordered(const UInt64Chunk &row_keys, Chunk *dest, BooleanChunk *optional_null_flags) const final {
    using chunkType_t = typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t;
    ColumnSourceImpls::FillChunkUnordered<chunkType_t>(row_keys, dest, optional_null_flags, data_);
  }

  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

  [[nodiscard]]
  const ElementType &GetElementType() const final {
    return element_type_;
  }

private:
  ElementType element_type_;
  internal::NumericBufferBackingStore<T> data_;
};

// Convenience usings
using CharBufferColumnSource = NumericBufferColumnSource<char16_t>;
using Int8BufferColumnSource = NumericBufferColumnSource<int16_t>;
using Int32BufferColumnSource = NumericBufferColumnSource<int32_t>;
using Int64BufferColumnSource = NumericBufferColumnSource<int64_t>;
using FloatBufferColumnSource = NumericBufferColumnSource<float>;
using DoubleBufferColumnSource = NumericBufferColumnSource<double>;
}  // namespace deephaven::dhcore::column
