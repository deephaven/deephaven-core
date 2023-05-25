/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

#pragma once

#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/dhcore/types.h"

namespace deephaven::client::column {
namespace internal {
class BackingStoreBase {
protected:
  void assertIndexValid(size_t index) const;

  template<typename T>
  void growIfNeeded(size_t requestedCapacity, std::unique_ptr<T[]> *data,
      std::unique_ptr<bool[]> *optionalNullFlags) {
    if (requestedCapacity <= capacity_) {
      return;
    }
    auto oldCapacity = capacity_;
    // sad
    if (capacity_ == 0) {
      capacity_ = 1;
    }
    while (capacity_ < requestedCapacity) {
      capacity_ *= 2;
    }

    auto newData = std::make_unique<T[]>(capacity_);
    auto newNullFlags =
        optionalNullFlags != nullptr ? std::make_unique<bool[]>(capacity_) : nullptr;
    std::copy(
        std::make_move_iterator(data->get()),
        std::make_move_iterator(data->get() + oldCapacity),
        newData.get());
    *data = std::move(newData);
    if (optionalNullFlags != nullptr) {
      std::copy(optionalNullFlags->get(),
          optionalNullFlags->get() + oldCapacity,
          newNullFlags.get());
      *optionalNullFlags = std::move(newNullFlags);
    }
  }

  size_t capacity_ = 0;
};

/**
 * This is the backing store used for the numeric types, which have the property that "null" is
 * represented by a special Deephaven constant.
 */
template<typename T>
class NumericBackingStore : public BackingStoreBase {
public:
  NumericBackingStore() {
    data_ = std::make_unique<T[]>(0);
  }

  std::tuple<T, bool> get(size_t index) const {
    assertIndexValid(index);
    auto value = data_[index];
    auto isNull = value == deephaven::dhcore::DeephavenTraits<T>::NULL_VALUE;
    return std::make_tuple(value, isNull);
  }

  void set(size_t index, T value, bool forceNull) {
    assertIndexValid(index);
    if (forceNull) {
      value = deephaven::dhcore::DeephavenTraits<T>::NULL_VALUE;
    }
    data_[index] = value;
  }

  void ensureCapacity(size_t requestedCapacity) {
    growIfNeeded(requestedCapacity, &data_, nullptr);
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
public:
  GenericBackingStore() {
    data_ = std::make_unique<T[]>(0);
  }

  std::tuple<T, bool> get(size_t index) const {
    assertIndexValid(index);
    auto value = data_[index];
    auto isNull = isNull_[index];
    return std::make_tuple(std::move(value), isNull);
  }

  void set(size_t index, T value, bool forceNull) {
    assertIndexValid(index);
    data_[index] = std::move(value);
    isNull_[index] = forceNull;
  }

  void ensureCapacity(size_t requestedCapacity) {
    growIfNeeded(requestedCapacity, &data_, &isNull_);
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
  typedef deephaven::dhcore::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::dhcore::chunk::Chunk Chunk;
  typedef deephaven::dhcore::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::dhcore::column::ColumnSourceImpls ColumnSourceImpls;
  typedef deephaven::dhcore::column::ColumnSourceVisitor ColumnSourceVisitor;
  typedef deephaven::dhcore::container::RowSequence RowSequence;

public:
  static std::shared_ptr <NumericArrayColumnSource> create() {
    return std::make_shared<NumericArrayColumnSource<T>>(Private());
  }

  explicit NumericArrayColumnSource(Private) {}

  ~NumericArrayColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optionalNullFlags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::fillChunk<chunkType_t>(rows, dest, optionalNullFlags, data_);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest, BooleanChunk *optionalNullFlags) const final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::fillChunkUnordered<chunkType_t>(rowKeys, dest, optionalNullFlags, data_);
  }

  void fillFromChunk(const Chunk &src, const BooleanChunk *optionalNullFlags, const RowSequence &rows) final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::fillFromChunk<chunkType_t>(src, optionalNullFlags, rows, &data_);
  }

  void fillFromChunkUnordered(const Chunk &src, const BooleanChunk *optionalNullFlags,
      const UInt64Chunk &rowKeys) final {
    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    ColumnSourceImpls::fillFromChunkUnordered<chunkType_t>(src, optionalNullFlags,
        rowKeys, &data_);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

private:
  internal::NumericBackingStore<T> data_;
};

template<typename T>
class GenericArrayColumnSource final : public deephaven::dhcore::column::MutableGenericColumnSource<T>,
    std::enable_shared_from_this<GenericArrayColumnSource<T>> {
  struct Private {
  };
  typedef deephaven::dhcore::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::dhcore::chunk::Chunk Chunk;
  typedef deephaven::dhcore::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::dhcore::column::ColumnSourceImpls ColumnSourceImpls;
  typedef deephaven::dhcore::column::ColumnSourceVisitor ColumnSourceVisitor;
  typedef deephaven::dhcore::container::RowSequence RowSequence;

public:
  static std::shared_ptr<GenericArrayColumnSource> create() {
    return std::make_shared<GenericArrayColumnSource>(Private());
  }

  explicit GenericArrayColumnSource(Private) {}
  ~GenericArrayColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optionalDestNullFlags) const final {
    ColumnSourceImpls::fillChunk<BooleanChunk>(rows, dest, optionalDestNullFlags, data_);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest,
      BooleanChunk *optionalDestNullFlags) const final {
    ColumnSourceImpls::fillChunkUnordered<BooleanChunk>(rowKeys, dest, optionalDestNullFlags, data_);
  }

  void fillFromChunk(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const RowSequence &rows) final {
    ColumnSourceImpls::fillFromChunk<BooleanChunk>(src, optionalSrcNullFlags, rows, &data_);
  }

  void fillFromChunkUnordered(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const UInt64Chunk &rowKeys) final {
    ColumnSourceImpls::fillFromChunkUnordered<BooleanChunk>(src, optionalSrcNullFlags, rowKeys, &data_);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

private:
  internal::GenericBackingStore<T> data_;
};
}  // namespace deephaven::client::column
