/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <type_traits>

#include "deephaven/dhcore/column/column_source.h"

namespace deephaven::dhcore::column {
// A central place to put the implementations for the similar-but-not-identical
// fill{,From}Chunk{,Unordered} implementations for the various column source types.
struct ColumnSourceImpls {
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using RowSequence = deephaven::dhcore::container::RowSequence;
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;

  static void AssertRangeValid(size_t begin, size_t end, size_t size);

  template<typename ChunkType, typename BackingStore>
  static void FillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optional_null_flags,
      const BackingStore &backing_store) {
    using deephaven::dhcore::utility::TrueOrThrow;
    using deephaven::dhcore::utility::VerboseCast;

    auto *typed_dest = VerboseCast<ChunkType *>(DEEPHAVEN_LOCATION_EXPR(dest));
    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= typed_dest->Size()));
    auto *dest_data = typed_dest->data();
    auto *dest_null = optional_null_flags != nullptr ? optional_null_flags->data() : nullptr;
    auto apply_chunk = [&dest_data, &dest_null, &backing_store](uint64_t begin, uint64_t end) {
      backing_store.Get(begin, end, dest_data, dest_null);
      auto size = end - begin;
      dest_data += size;
      if (dest_null != nullptr) {
        dest_null += size;
      }
    };
    rows.ForEachInterval(apply_chunk);
  }

  template<typename ChunkType, typename BackingStore>
  static void FillChunkUnordered(const UInt64Chunk &row_keys, Chunk *dest,
      BooleanChunk *optional_null_flags, const BackingStore &backing_store) {
    using deephaven::dhcore::utility::TrueOrThrow;
    using deephaven::dhcore::utility::VerboseCast;

    auto *typed_dest = VerboseCast<ChunkType *>(DEEPHAVEN_LOCATION_EXPR(dest));
    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(row_keys.Size() <= typed_dest->Size()));
    const uint64_t *keys = row_keys.data();
    auto *dest_data = typed_dest->data();
    auto *dest_null = optional_null_flags != nullptr ? optional_null_flags->data() : nullptr;

    for (size_t dest_index = 0; dest_index < row_keys.Size(); ++dest_index) {
      // This is terrible. For now.
      auto src_index = keys[dest_index];
      backing_store.Get(src_index, src_index + 1, dest_data, dest_null);
      ++dest_data;
      if (dest_null != nullptr) {
        ++dest_null;
      }
    }
  }

  template<typename ChunkType, typename BackingStore>
  static void FillFromChunk(const Chunk &src, const BooleanChunk *optional_src_null_flags,
      const RowSequence &rows, BackingStore *backing_store) {
    using deephaven::dhcore::utility::TrueOrThrow;
    using deephaven::dhcore::utility::VerboseCast;

    const auto *typed_src = VerboseCast<const ChunkType *>(DEEPHAVEN_LOCATION_EXPR(&src));
    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= typed_src->Size()));

    const auto *src_data = typed_src->data();
    const auto *null_data = optional_src_null_flags != nullptr ? optional_src_null_flags->data() : nullptr;
    auto apply_chunk = [&src_data, &null_data, backing_store](uint64_t begin, uint64_t end) {
      backing_store->EnsureCapacity(end);
      backing_store->Set(begin, end, src_data, null_data);
      auto size = end - begin;
      src_data += size;
      if (null_data != nullptr) {
        null_data += size;
      }
    };
    rows.ForEachInterval(apply_chunk);
  }

  template<typename ChunkType, typename BackingStore>
  static void FillFromChunkUnordered(const Chunk &src, const BooleanChunk *optional_src_null_flags,
      const UInt64Chunk &row_keys, BackingStore *backing_store) {
    using deephaven::dhcore::utility::TrueOrThrow;
    using deephaven::dhcore::utility::VerboseCast;

    const auto *typed_src = VerboseCast<const ChunkType *>(DEEPHAVEN_LOCATION_EXPR(&src));
    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(row_keys.Size() <= typed_src->Size()));
    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(optional_src_null_flags == nullptr ||
        row_keys.Size() <= optional_src_null_flags->Size()));

    const auto *key_data = row_keys.data();
    const auto *src_data = typed_src->data();
    const auto *null_data = optional_src_null_flags != nullptr ? optional_src_null_flags->data() : nullptr;
    for (size_t src_index = 0; src_index < typed_src->Size(); ++src_index) {
      // This is terrible. For now.
      auto dest_index = key_data[src_index];
      backing_store->EnsureCapacity(dest_index + 1);
      backing_store->Set(dest_index, dest_index + 1, src_data, null_data);
      ++src_data;
      if (null_data != nullptr) {
        ++null_data;
      }
    }
  }
};
}  // namespace deephaven::client::column
