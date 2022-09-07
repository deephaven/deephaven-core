/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

#pragma once

#include "deephaven/client/column/column_source.h"
#include "deephaven/client/types.h"

namespace deephaven::client::column {
namespace internal {
// A central place to put the implementations for the similar-but-not-identical
// fill{,From}Chunk{,Unordered} implementations for the various column source types.

struct ColumnSourceImpls {
  typedef deephaven::client::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;

  template<typename CHUNK_TYPE, typename BACKING_STORE>
  static void fillChunk(const RowSequence &rows,
      Chunk *dest, BooleanChunk *optionalNullFlags, const BACKING_STORE &backingStore) {
    using deephaven::client::utility::trueOrThrow;
    using deephaven::client::utility::verboseCast;

    auto *typedDest = verboseCast<CHUNK_TYPE *>(DEEPHAVEN_EXPR_MSG(dest));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rows.size() <= typedDest->size()));
    auto *destData = typedDest->data();
    auto *destNull = optionalNullFlags != nullptr ? optionalNullFlags->data() : nullptr;
    size_t destIndex = 0;
    auto applyChunk = [destData, destNull, &backingStore, &destIndex]
        (uint64_t begin, uint64_t end) {
      for (auto srcIndex = begin; srcIndex != end; ++srcIndex) {
        auto [value, isNull] = backingStore.get(srcIndex);
        destData[destIndex] = std::move(value);
        if (destNull != nullptr) {
          destNull[destIndex] = isNull;
        }
        ++destIndex;
      }
    };
    rows.forEachChunk(applyChunk);
  }

  template<typename CHUNK_TYPE, typename BACKING_STORE>
  static void fillChunkUnordered(
      const UInt64Chunk &rowKeys, Chunk *dest, BooleanChunk *optionalNullFlags,
      const BACKING_STORE &backingStore) {
    using deephaven::client::chunk::TypeToChunk;
    using deephaven::client::utility::trueOrThrow;
    using deephaven::client::utility::verboseCast;

    auto *typedDest = verboseCast<CHUNK_TYPE *>(DEEPHAVEN_EXPR_MSG(dest));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rowKeys.size() <= typedDest->size()));
    const uint64_t *keys = rowKeys.data();
    auto *destData = typedDest->data();
    auto *destNull = optionalNullFlags != nullptr ? optionalNullFlags->data() : nullptr;

    for (size_t destIndex = 0; destIndex < rowKeys.size(); ++destIndex) {
      auto srcIndex = keys[destIndex];
      auto [value, isNull] = backingStore.get(srcIndex);
      destData[destIndex] = std::move(value);
      if (destNull != nullptr) {
        destNull[destIndex] = isNull;
      }
    }
  }

  template<typename CHUNK_TYPE, typename BACKING_STORE>
  static void fillFromChunk(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const RowSequence &rows, BACKING_STORE *backingStore) {
    using deephaven::client::chunk::TypeToChunk;
    using deephaven::client::utility::trueOrThrow;
    using deephaven::client::utility::verboseCast;

    const auto *typedSrc = verboseCast<const CHUNK_TYPE *>(DEEPHAVEN_EXPR_MSG(&src));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rows.size() <= typedSrc->size()));

    const auto *srcData = typedSrc->data();
    const auto *nullData = optionalSrcNullFlags != nullptr ? optionalSrcNullFlags->data() : nullptr;
    size_t srcIndex = 0;
    auto applyChunk = [srcData, &srcIndex, nullData, backingStore](uint64_t begin, uint64_t end) {
      backingStore->ensureCapacity(end);
      for (auto destIndex = begin; destIndex != end; ++destIndex) {
        auto value = srcData[srcIndex];
        auto forceNull = nullData != nullptr && nullData[srcIndex];
        backingStore->set(destIndex, value, forceNull);
        ++srcIndex;
      }
    };
    rows.forEachChunk(applyChunk);
  }

  template<typename CHUNK_TYPE, typename BACKING_STORE>
  static void fillFromChunkUnordered(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const UInt64Chunk &rowKeys, BACKING_STORE *backingStore) {
    using deephaven::client::chunk::TypeToChunk;
    using deephaven::client::utility::trueOrThrow;
    using deephaven::client::utility::verboseCast;

    const auto *typedSrc = verboseCast<const CHUNK_TYPE *>(DEEPHAVEN_EXPR_MSG(&src));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rowKeys.size() <= typedSrc->size()));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(optionalSrcNullFlags == nullptr ||
        rowKeys.size() <= optionalSrcNullFlags->size()));

    const auto *keyData = rowKeys.data();
    const auto *srcData = typedSrc->data();
    const auto *nullData = optionalSrcNullFlags != nullptr ? optionalSrcNullFlags->data() : nullptr;
    for (size_t srcIndex = 0; srcIndex < typedSrc->size(); ++srcIndex) {
      auto destIndex = keyData[srcIndex];
      backingStore->ensureCapacity(destIndex + 1);
      auto value = srcData[srcIndex];
      auto forceNull = nullData != nullptr && nullData[srcIndex];
      backingStore->set(destIndex, value, forceNull);
    }
  }

};

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
    auto isNull = value == deephaven::client::DeephavenConstantsForType<T>::NULL_VALUE;
    return std::make_tuple(value, isNull);
  }

  void set(size_t index, T value, bool forceNull) {
    assertIndexValid(index);
    if (forceNull) {
      value = deephaven::client::DeephavenConstantsForType<T>::NULL_VALUE;
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
    return std::make_tuple(value, isNull);
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
class NumericArrayColumnSource final : public MutableNumericColumnSource<T>,
    std::enable_shared_from_this<NumericArrayColumnSource<T>> {
  struct Private {
  };
  typedef deephaven::client::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  static std::shared_ptr <NumericArrayColumnSource> create() {
    return std::make_shared<NumericArrayColumnSource<T>>(Private());
  }

  explicit NumericArrayColumnSource(Private) {}

  ~NumericArrayColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest,
      BooleanChunk *optionalNullFlags) const final {
    typedef typename deephaven::client::chunk::TypeToChunk<T>::type_t chunkType_t;
    internal::ColumnSourceImpls::fillChunk<chunkType_t>(rows, dest, optionalNullFlags, data_);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest,
      BooleanChunk *optionalNullFlags) const final {
    typedef typename deephaven::client::chunk::TypeToChunk<T>::type_t chunkType_t;
    internal::ColumnSourceImpls::fillChunkUnordered<chunkType_t>(rowKeys, dest, optionalNullFlags, data_);
  }

  void fillFromChunk(const Chunk &src, const BooleanChunk *optionalNullFlags,
      const RowSequence &rows) final {
    typedef typename deephaven::client::chunk::TypeToChunk<T>::type_t chunkType_t;
    internal::ColumnSourceImpls::fillFromChunk<chunkType_t>(src, optionalNullFlags, rows, &data_);
  }

  void fillFromChunkUnordered(const Chunk &src,
      const BooleanChunk *optionalNullFlags, const UInt64Chunk &rowKeys) final {
    typedef typename deephaven::client::chunk::TypeToChunk<T>::type_t chunkType_t;
    internal::ColumnSourceImpls::fillFromChunkUnordered<chunkType_t>(src, optionalNullFlags,
        rowKeys, &data_);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

private:
  internal::NumericBackingStore<T> data_;
};

template<typename T>
class GenericArrayColumnSource final : public MutableGenericColumnSource<T>,
    std::enable_shared_from_this<GenericArrayColumnSource<T>> {
  struct Private {
  };
  typedef deephaven::client::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  static std::shared_ptr<GenericArrayColumnSource> create() {
    return std::make_shared<GenericArrayColumnSource>(Private());
  }

  explicit GenericArrayColumnSource(Private) {}
  ~GenericArrayColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optionalDestNullFlags) const final {
    internal::ColumnSourceImpls::fillChunk<BooleanChunk>(rows, dest, optionalDestNullFlags, data_);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest,
      BooleanChunk *optionalDestNullFlags) const final {
    internal::ColumnSourceImpls::fillChunkUnordered<BooleanChunk>(rowKeys, dest,
        optionalDestNullFlags, data_);
  }

  void fillFromChunk(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const RowSequence &rows) final {
    internal::ColumnSourceImpls::fillFromChunk<BooleanChunk>(src, optionalSrcNullFlags, rows, &data_);
  }

  void fillFromChunkUnordered(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const UInt64Chunk &rowKeys) final {
    internal::ColumnSourceImpls::fillFromChunkUnordered<BooleanChunk>(src, optionalSrcNullFlags,
        rowKeys, &data_);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

private:
  internal::GenericBackingStore<bool> data_;
};
}  // namespace deephaven::client::column
