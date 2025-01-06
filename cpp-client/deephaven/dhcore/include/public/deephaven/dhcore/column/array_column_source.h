/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/dhcore/types.h"

namespace deephaven::dhcore::column {
namespace internal {
class BackingStoreBase {
protected:
  explicit BackingStoreBase(size_t capacity) : capacity_(capacity) {}

  template<typename T>
  void GrowIfNeeded(size_t requested_capacity, std::unique_ptr<T[]> *data,
      std::unique_ptr<bool[]> *optional_null_flags) {
    if (requested_capacity <= capacity_) {
      return;
    }
    auto old_capacity = capacity_;
    // sad
    if (capacity_ == 0) {
      capacity_ = 1;
    }
    while (capacity_ < requested_capacity) {
      capacity_ *= 2;
    }

    auto new_data = std::make_unique<T[]>(capacity_);
    auto new_null_flags =
        optional_null_flags != nullptr ? std::make_unique<bool[]>(capacity_) : nullptr;
    std::copy(
        std::make_move_iterator(data->get()),
        std::make_move_iterator(data->get() + old_capacity),
        new_data.get());
    *data = std::move(new_data);
    if (optional_null_flags != nullptr) {
      std::copy(optional_null_flags->get(),
          optional_null_flags->get() + old_capacity,
          new_null_flags.get());
      *optional_null_flags = std::move(new_null_flags);
    }
  }

  size_t capacity_ = 0;
};

/**
 * This is the backing store used for the numeric types, which have the property that "null" is
 * represented By a special Deephaven constant.
 */
template<typename T>
class NumericBackingStore : public BackingStoreBase {
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
public:
  NumericBackingStore() {
    data_ = std::make_unique<T[]>(0);
  }

  void Get(size_t begin_index, size_t end_index, T *dest, bool *optional_null_flags) const {
    ColumnSourceImpls::AssertRangeValid(begin_index, end_index, capacity_);
    for (auto i = begin_index; i != end_index; ++i) {
      const auto &value = data_[i];
      *dest++ = value;
      if (optional_null_flags != nullptr) {
        auto is_null = value == deephaven::dhcore::DeephavenTraits<T>::NULL_VALUE;
        *optional_null_flags++ = is_null;
      }
    }
  }

  void Set(size_t begin_index, size_t end_index, const T *src, const bool *optional_null_flags) const {
    ColumnSourceImpls::AssertRangeValid(begin_index, end_index, capacity_);
    for (auto i = begin_index; i != end_index; ++i) {
      if (optional_null_flags != nullptr && *optional_null_flags++) {
        data_[i] = deephaven::dhcore::DeephavenTraits<T>::NULL_VALUE;
        continue;
      }
      data_[i] = *src++;
    }
  }

  void EnsureCapacity(size_t requested_capacity) {
    GrowIfNeeded(requested_capacity, &data_, nullptr);
  }

private:
  std::unique_ptr<T[]> data_;
};

/**
 * This is the backing store used for other types like std::string, bool, and DateTime, which track
 * the "null" flag explicitly.
 */
template<typename T>
class GenericBackingStore : public BackingStoreBase {
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
public:
  GenericBackingStore(std::unique_ptr<T[]> data, std::unique_ptr<bool[]> is_null, size_t size) :
      BackingStoreBase(size), data_(std::move(data)), isNull_(std::move(is_null)) {}
  ~GenericBackingStore() = default;

  void Get(size_t begin_index, size_t end_index, T *dest, bool *optional_null_flags) const {
    ColumnSourceImpls::AssertRangeValid(begin_index, end_index, capacity_);
    for (auto i = begin_index; i != end_index; ++i) {
      *dest++ = data_[i];
      if (optional_null_flags != nullptr) {
        *optional_null_flags++ = isNull_[i];
      }
    }
  }

  void Set(size_t begin_index, size_t end_index, const T *src, const bool *optional_null_flags) const {
    ColumnSourceImpls::AssertRangeValid(begin_index, end_index, capacity_);
    for (auto i = begin_index; i != end_index; ++i) {
      data_[i] = *src++;
      isNull_[i] = optional_null_flags != nullptr && *optional_null_flags++;
    }
  }

  void EnsureCapacity(size_t requested_capacity) {
    GrowIfNeeded(requested_capacity, &data_, &isNull_);
  }

private:
  std::unique_ptr<T[]> data_;
  std::unique_ptr<bool[]> isNull_;
};
}  // namespace internal

template<typename T>
class NumericArrayColumnSource final : public deephaven::dhcore::column::MutableNumericColumnSource<T>,
    std::enable_shared_from_this<NumericArrayColumnSource<T>> {
  struct Private {
  };
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;
  using RowSequence = deephaven::dhcore::container::RowSequence;

public:
  static std::shared_ptr <NumericArrayColumnSource> Create() {
    return std::make_shared<NumericArrayColumnSource<T>>(Private());
  }

  explicit NumericArrayColumnSource(Private) {}

  ~NumericArrayColumnSource() final = default;

  void FillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optional_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillChunk<chunkType_t>(rows, dest, optional_null_flags, data_);
  }

  void FillChunkUnordered(const UInt64Chunk &row_keys, Chunk *dest, BooleanChunk *optional_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillChunkUnordered<chunkType_t>(row_keys, dest, optional_null_flags, data_);
  }

  void FillFromChunk(const Chunk &src, const BooleanChunk *optional_null_flags, const RowSequence &rows) final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillFromChunk<chunkType_t>(src, optional_null_flags, rows, &data_);
  }

  void FillFromChunkUnordered(const Chunk &src, const BooleanChunk *optional_null_flags,
      const UInt64Chunk &row_keys) final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillFromChunkUnordered<chunkType_t>(src, optional_null_flags,
        row_keys, &data_);
  }

  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

private:
  internal::NumericBackingStore<T> data_;
};

template<typename T>
class GenericArrayColumnSource final : public deephaven::dhcore::column::MutableGenericColumnSource<T>,
    std::enable_shared_from_this<GenericArrayColumnSource<T>> {
  struct Private {
  };
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;
  using RowSequence = deephaven::dhcore::container::RowSequence;

public:
  static std::shared_ptr<GenericArrayColumnSource> Create() {
    auto elements = std::make_unique<T[]>(0);
    auto nulls = std::make_unique<T[]>(0);
    return CreateFromArrays(std::move(elements), std::move(nulls), 0);
  }

  static std::shared_ptr<GenericArrayColumnSource> CreateFromArrays(std::unique_ptr<T[]> elements,
      std::unique_ptr<bool[]> nulls, size_t size) {
    return std::make_shared<GenericArrayColumnSource>(Private(), std::move(elements), std::move(nulls),
        size);
  }

  explicit GenericArrayColumnSource(Private, std::unique_ptr<T[]> elements,
      std::unique_ptr<bool[]> nulls, size_t size) : data_(std::move(elements), std::move(nulls), size) {}
  ~GenericArrayColumnSource() final = default;

  void FillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optional_dest_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillChunk<chunkType_t>(rows, dest, optional_dest_null_flags, data_);
  }

  void FillChunkUnordered(const UInt64Chunk &row_keys, Chunk *dest,
      BooleanChunk *optional_dest_null_flags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillChunkUnordered<chunkType_t>(row_keys, dest, optional_dest_null_flags, data_);
  }

  void FillFromChunk(const Chunk &src, const BooleanChunk *optional_src_null_flags,
      const RowSequence &rows) final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillFromChunk<chunkType_t>(src, optional_src_null_flags, rows, &data_);
  }

  void FillFromChunkUnordered(const Chunk &src, const BooleanChunk *optional_src_null_flags,
      const UInt64Chunk &row_keys) final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::FillFromChunkUnordered<chunkType_t>(src, optional_src_null_flags, row_keys,
        &data_);
  }

  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

private:
  internal::GenericBackingStore<T> data_;
};

using BooleanArrayColumnSource = GenericArrayColumnSource<bool>;
using StringArrayColumnSource = GenericArrayColumnSource<std::string>;
using DateTimeArrayColumnSource = GenericArrayColumnSource<DateTime>;
using LocalDateArrayColumnSource = GenericArrayColumnSource<LocalDate>;
using LocalTimeArrayColumnSource = GenericArrayColumnSource<LocalTime>;
}  // namespace deephaven::dhcore::column
