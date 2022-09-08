/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <immer/algorithm.hpp>
#include <immer/flex_vector.hpp>
#include "deephaven/client/arrowutil/arrow_visitors.h"
#include "deephaven/client/chunk/chunk.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/types.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::immerutil {
namespace internal {
struct ImmerColumnSourceImpls {
  typedef deephaven::client::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;

  /**
   * This helper function has two dimensions of optionality: the first (controlled by whether the
   * template is numeric) indicates whether (if it is numeric) "null-ness" comes from the inherent
   * Deephaven notion of null-ness (the special numeric constants), or (if it is not numeric),
   * it comes from a separate vector of null flags. The second dimension of optionality is
   * controlled by 'optionalDestNullFlags', which indicates whether the caller cares about nullness.
   * If this pointer is not null, then it points to a BooleanChunk which can hold all the null
   * flags. On the other hand if this pointer is null, then the caller doesn't care about null flags
   * and we don't have to do any special work to determine nullness.
   */

  template<typename T>
  static void fillChunk(const immer::flex_vector<T> &srcData,
      const immer::flex_vector<bool> *srcNullFlags,
      const RowSequence &rows, Chunk *destData, BooleanChunk *optionalDestNullFlags) {
    using deephaven::client::chunk::TypeToChunk;
    using deephaven::client::utility::trueOrThrow;
    using deephaven::client::utility::verboseCast;
    typedef typename TypeToChunk<T>::type_t chunkType_t;
    auto *typedDest = verboseCast<chunkType_t*>(DEEPHAVEN_EXPR_MSG(destData));

    constexpr bool typeIsNumeric = deephaven::client::arrowutil::isNumericType<T>();

    trueOrThrow(DEEPHAVEN_EXPR_MSG(rows.size() <= typedDest->size()));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(optionalDestNullFlags == nullptr ||
       rows.size() <= optionalDestNullFlags->size()));
    if (!typeIsNumeric) {
      trueOrThrow(DEEPHAVEN_EXPR_MSG(srcNullFlags != nullptr));
    } else {
      // avoid CLion warning about unused variable.
      (void)srcNullFlags;
    }
    auto *destDatap = typedDest->data();
    auto *destNullp = optionalDestNullFlags != nullptr ? optionalDestNullFlags->data() : nullptr;

    auto copyDataInner = [&destDatap, &destNullp](const T *dataBegin, const T *dataEnd) {
      for (const T *current = dataBegin; current != dataEnd; ++current) {
        auto value = *current;
        *destDatap++ = value;
        if constexpr(typeIsNumeric) {
          if (destNullp != nullptr) {
            *destNullp++ = value == deephaven::client::DeephavenConstantsForType<T>::NULL_VALUE;
          }
        }
      }
    };

    auto copyNullsInner = [&destNullp](const bool *nullBegin, const bool *nullEnd) {
      for (const bool *current = nullBegin; current != nullEnd; ++current) {
        *destNullp++ = *current;
      }
    };

    auto copyOuter = [&srcData, srcNullFlags, destNullp, &copyDataInner,
        &copyNullsInner](uint64_t srcBegin, uint64_t srcEnd) {
      auto srcBeginp = srcData.begin() + srcBegin;
      auto srcEndp = srcData.begin() + srcEnd;
      immer::for_each_chunk(srcBeginp, srcEndp, copyDataInner);

      if constexpr(!typeIsNumeric) {
        if (destNullp != nullptr) {
          auto nullsBeginp = srcNullFlags->begin() + srcBegin;
          auto nullsEndp = srcNullFlags->begin() + srcEnd;
          immer::for_each_chunk(nullsBeginp, nullsEndp, copyNullsInner);
        }
      }
    };
    rows.forEachChunk(copyOuter);
  }

  template<typename T>
  static void fillChunkUnordered(const immer::flex_vector<T> &srcData,
      const immer::flex_vector<bool> *srcNullFlags,
      const UInt64Chunk &rowKeys, Chunk *destData, BooleanChunk *optionalDestNullFlags) {
    using deephaven::client::chunk::TypeToChunk;
    using deephaven::client::utility::trueOrThrow;
    using deephaven::client::utility::verboseCast;

    typedef typename TypeToChunk<T>::type_t chunkType_t;

    constexpr bool typeIsNumeric = deephaven::client::arrowutil::isNumericType<T>();

    auto *typedDest = verboseCast<chunkType_t*>(DEEPHAVEN_EXPR_MSG(destData));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rowKeys.size() <= typedDest->size()));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(optionalDestNullFlags == nullptr ||
        rowKeys.size() <= optionalDestNullFlags->size()));
    if (!typeIsNumeric) {
      trueOrThrow(DEEPHAVEN_EXPR_MSG(srcNullFlags != nullptr));
    }
    auto *destp = typedDest->data();
    auto *destNullp = optionalDestNullFlags != nullptr ? optionalDestNullFlags->data() : nullptr;

    // Note: Uses random access with Immer, which is significantly more expensive than iterating
    // over contiguous Immer ranges.
    for (auto key : rowKeys) {
      auto value = srcData[key];
      *destp++ = value;
      if (destNullp != nullptr) {
        if constexpr(typeIsNumeric) {
          *destNullp++ = value == deephaven::client::DeephavenConstantsForType<T>::NULL_VALUE;
        } else {
          *destNullp++ = (*srcNullFlags)[key];
        }
      }
    }
  }
};
}  // namespace internal

class ImmerColumnSource : public virtual deephaven::client::column::ColumnSource {
};

template<typename T>
class NumericImmerColumnSource final : public ImmerColumnSource,
    public deephaven::client::column::NumericColumnSource<T>,
    std::enable_shared_from_this<NumericImmerColumnSource<T>> {
  struct Private {};

  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::column::ColumnSourceVisitor ColumnSourceVisitor;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  static std::shared_ptr<NumericImmerColumnSource> create(immer::flex_vector<T> data) {
    return std::make_shared<NumericImmerColumnSource>(Private(), std::move(data));
  }

  explicit NumericImmerColumnSource(Private, immer::flex_vector<T> data) : data_(std::move(data)) {}

  ~NumericImmerColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *destData, BooleanChunk *optionalDestNullFlags) const final {
    internal::ImmerColumnSourceImpls::fillChunk(data_, nullptr, rows, destData,
        optionalDestNullFlags);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *destData,
      BooleanChunk *optionalDestNullFlags) const final {
    internal::ImmerColumnSourceImpls::fillChunkUnordered(data_, nullptr, rowKeys, destData,
        optionalDestNullFlags);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

  std::any backdoor() const final {
    return &data_;
  }

private:
  immer::flex_vector<T> data_;
};

template<typename T>
class GenericImmerColumnSource final : public ImmerColumnSource,
    public deephaven::client::column::GenericColumnSource<T>,
    std::enable_shared_from_this<GenericImmerColumnSource<T>> {
  struct Private {};
  typedef deephaven::client::column::ColumnSourceVisitor ColumnSourceVisitor;
public:
  static std::shared_ptr<GenericImmerColumnSource> create(immer::flex_vector<T> data,
      immer::flex_vector<bool> nullFlags) {
    return std::make_shared<GenericImmerColumnSource>(Private(), std::move(data), std::move(nullFlags));
  }

  GenericImmerColumnSource(Private, immer::flex_vector<T> &&data, immer::flex_vector<bool> &&nullFlags) :
      data_(std::move(data)), nullFlags_(std::move(nullFlags)) {}
  ~GenericImmerColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optionalDestNullFlags) const final {
    internal::ImmerColumnSourceImpls::fillChunk(data_, &nullFlags_, rows, dest,
        optionalDestNullFlags);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest,
      BooleanChunk *optionalDestNullFlags) const final {
    internal::ImmerColumnSourceImpls::fillChunkUnordered(data_, &nullFlags_, rowKeys, dest,
        optionalDestNullFlags);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

  std::any backdoor() const final {
    return &data_;
  }

private:
  immer::flex_vector<T> data_;
  immer::flex_vector<bool> nullFlags_;
};
}  // namespace deephaven::client::immerutil
