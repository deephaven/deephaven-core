#pragma once

#include <type_traits>

#include "deephaven/dhcore/column/column_source.h"

namespace deephaven::dhcore::column {
// A central place to put the implementations for the similar-but-not-identical
// fill{,From}Chunk{,Unordered} implementations for the various column source types.
struct ColumnSourceImpls {
  typedef deephaven::dhcore::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::dhcore::chunk::Chunk Chunk;
  typedef deephaven::dhcore::container::RowSequence RowSequence;
  typedef deephaven::dhcore::chunk::UInt64Chunk UInt64Chunk;

  static void assertRangeValid(size_t begin, size_t end, size_t size);

  template<typename CHUNK_TYPE, typename BACKING_STORE>
  static void fillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optionalNullFlags,
      const BACKING_STORE &backingStore) {
    using deephaven::dhcore::utility::trueOrThrow;
    using deephaven::dhcore::utility::verboseCast;

    auto *typedDest = verboseCast<CHUNK_TYPE *>(DEEPHAVEN_EXPR_MSG(dest));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rows.size() <= typedDest->size()));
    auto *destData = typedDest->data();
    auto *destNull = optionalNullFlags != nullptr ? optionalNullFlags->data() : nullptr;
    auto applyChunk = [&destData, &destNull, &backingStore](uint64_t begin, uint64_t end) {
      backingStore.get(begin, end, destData, destNull);
      auto size = end - begin;
      destData += size;
      if (destNull != nullptr) {
        destNull += size;
      }
    };
    rows.forEachInterval(applyChunk);
  }

  template<typename CHUNK_TYPE, typename BACKING_STORE>
  static void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest, BooleanChunk *optionalNullFlags,
      const BACKING_STORE &backingStore) {
    using deephaven::dhcore::utility::trueOrThrow;
    using deephaven::dhcore::utility::verboseCast;

    auto *typedDest = verboseCast<CHUNK_TYPE *>(DEEPHAVEN_EXPR_MSG(dest));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rowKeys.size() <= typedDest->size()));
    const uint64_t *keys = rowKeys.data();
    auto *destData = typedDest->data();
    auto *destNull = optionalNullFlags != nullptr ? optionalNullFlags->data() : nullptr;

    for (size_t destIndex = 0; destIndex < rowKeys.size(); ++destIndex) {
      // This is terrible. For now.
      auto srcIndex = keys[destIndex];
      backingStore.get(srcIndex, srcIndex + 1, destData, destNull);
      ++destData;
      if (destNull != nullptr) {
        ++destNull;
      }
    }
  }

  template<typename CHUNK_TYPE, typename BACKING_STORE>
  static void fillFromChunk(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const RowSequence &rows, BACKING_STORE *backingStore) {
    using deephaven::dhcore::utility::trueOrThrow;
    using deephaven::dhcore::utility::verboseCast;

    const auto *typedSrc = verboseCast<const CHUNK_TYPE *>(DEEPHAVEN_EXPR_MSG(&src));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rows.size() <= typedSrc->size()));

    const auto *srcData = typedSrc->data();
    const auto *nullData = optionalSrcNullFlags != nullptr ? optionalSrcNullFlags->data() : nullptr;
    auto applyChunk = [&srcData, &nullData, backingStore](uint64_t begin, uint64_t end) {
      backingStore->ensureCapacity(end);
      backingStore->set(begin, end, srcData, nullData);
      auto size = end - begin;
      srcData += size;
      if (nullData != nullptr) {
        nullData += size;
      }
    };
    rows.forEachInterval(applyChunk);
  }

  template<typename CHUNK_TYPE, typename BACKING_STORE>
  static void fillFromChunkUnordered(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const UInt64Chunk &rowKeys, BACKING_STORE *backingStore) {
    using deephaven::dhcore::utility::trueOrThrow;
    using deephaven::dhcore::utility::verboseCast;

    const auto *typedSrc = verboseCast<const CHUNK_TYPE *>(DEEPHAVEN_EXPR_MSG(&src));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(rowKeys.size() <= typedSrc->size()));
    trueOrThrow(DEEPHAVEN_EXPR_MSG(optionalSrcNullFlags == nullptr ||
        rowKeys.size() <= optionalSrcNullFlags->size()));

    const auto *keyData = rowKeys.data();
    const auto *srcData = typedSrc->data();
    const auto *nullData = optionalSrcNullFlags != nullptr ? optionalSrcNullFlags->data() : nullptr;
    for (size_t srcIndex = 0; srcIndex < typedSrc->size(); ++srcIndex) {
      // This is terrible. For now.
      auto destIndex = keyData[srcIndex];
      backingStore->ensureCapacity(destIndex + 1);
      backingStore->set(destIndex, destIndex + 1, srcData, nullData);
      ++srcData;
      if (nullData != nullptr) {
        ++nullData;
      }
    }
  }
};
}  // namespace deephaven::client::column
