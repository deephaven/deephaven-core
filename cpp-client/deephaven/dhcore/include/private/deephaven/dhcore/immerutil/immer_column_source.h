/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <immer/algorithm.hpp>
#include <immer/flex_vector.hpp>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::dhcore::immerutil {
namespace internal {
struct ImmerColumnSourceImpls {
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  using RowSequence = deephaven::dhcore::container::RowSequence;

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
  static void FillChunk(const immer::flex_vector<T> &src_data,
      const immer::flex_vector<bool> *src_null_flags,
      const RowSequence &rows, Chunk *dest_data, BooleanChunk *optional_dest_null_flags) {
    using deephaven::dhcore::chunk::TypeToChunk;
    using deephaven::dhcore::utility::TrueOrThrow;
    using deephaven::dhcore::utility::VerboseCast;
    typedef typename TypeToChunk<T>::type_t chunkType_t;
    auto *typed_dest = VerboseCast<chunkType_t *>(DEEPHAVEN_LOCATION_EXPR(dest_data));

    constexpr bool kTypeIsNumeric = deephaven::dhcore::DeephavenTraits<T>::kIsNumeric;

    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= src_data.size()));
    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= typed_dest->Size()));
    if (!kTypeIsNumeric) {
      TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(src_null_flags != nullptr));
      TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= src_null_flags->size()));
    }
    if (optional_dest_null_flags != nullptr) {
      TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= optional_dest_null_flags->Size()));
    }
    (void) src_null_flags;  // avoid CLion warning about unused variable.

    auto *dest_datap = typed_dest->data();
    // We have a nested loop here, represented by two lambdas. This code invokes
    // RowSequence::ForEachInterval which takes contiguous ranges from 'rows' and feeds them
    // to 'copy_data_outer'. Then 'copy_data_outer' turns that contiguous range into a
    // pair of [begin, end) Immer iterators. But then, rather than store into that iterator range
    // directly, those Immer iterators are passed to immer::for_each_chunk. This breaks down the
    // Immer range into subranges of plain data, and invokes the copy_data_inner lambda. Then,
    // 'copy_data_inner' just copies data in the normal C++ way.
    auto copy_data_inner = [&dest_datap](const T *src_beginp, const T *src_endp) {
      for (const T *current = src_beginp; current != src_endp; ++current) {
        *dest_datap++ = *current;
      }
    };

    auto copy_data_outer = [&src_data, &copy_data_inner](uint64_t src_begin, uint64_t src_end) {
      auto src_beginp = src_data.begin() + src_begin;
      auto src_endp = src_data.begin() + src_end;
      immer::for_each_chunk(src_beginp, src_endp, copy_data_inner);
    };

    rows.ForEachInterval(copy_data_outer);

    // If the caller has opted out of getting null flags, we are done.
    if (optional_dest_null_flags == nullptr) {
      return;
    }

    // Otherwise (if the caller wants null flags), we do a similar algorithm to copy null flags.
    // The one complication is that the column source only stores null flags explicitly for
    // non-numeric types. For numeric types, the column source uses the Deephaven convention
    // for nullness. To handle this, we have two different forms of the operation,
    // one which supports the numeric convention and one which supports the non-numeric convention.
    auto *dest_nullp = optional_dest_null_flags->data();

    if constexpr (kTypeIsNumeric) {
      auto copy_nulls_inner = [&dest_nullp](const T *data_begin, const T *data_end) {
        for (const T *current = data_begin; current != data_end; ++current) {
          auto is_null = *current == deephaven::dhcore::DeephavenTraits<T>::kNullValue;
          *dest_nullp++ = is_null;
        }
      };

      auto copy_nulls_outer = [&src_data, &copy_nulls_inner](uint64_t src_begin,
          uint64_t src_end) {
        auto src_beginp = src_data.begin() + src_begin;
        auto src_endp = src_data.begin() + src_end;
        immer::for_each_chunk(src_beginp, src_endp, copy_nulls_inner);
      };
      rows.ForEachInterval(copy_nulls_outer);
    } else {
      auto copy_nulls_inner = [&dest_nullp](const bool *null_begin, const bool *null_end) {
        for (const bool *current = null_begin; current != null_end; ++current) {
          *dest_nullp++ = *current;
        }
      };

      auto copy_nulls_outer = [src_null_flags, &copy_nulls_inner](uint64_t src_begin,
          uint64_t src_end) {
        auto nulls_begin = src_null_flags->begin() + src_begin;
        auto nulls_end = src_null_flags->begin() + src_end;
        immer::for_each_chunk(nulls_begin, nulls_end, copy_nulls_inner);
      };
      rows.ForEachInterval(copy_nulls_outer);
    }
  }

  template<typename T>
  static void FillChunkUnordered(const immer::flex_vector<T> &src_data,
      const immer::flex_vector<bool> *src_null_flags,
      const UInt64Chunk &rows, Chunk *dest_data, BooleanChunk *optional_dest_null_flags) {
    using deephaven::dhcore::chunk::TypeToChunk;
    using deephaven::dhcore::utility::TrueOrThrow;
    using deephaven::dhcore::utility::VerboseCast;

    typedef typename TypeToChunk<T>::type_t chunkType_t;

    constexpr bool kTypeIsNumeric = deephaven::dhcore::DeephavenTraits<T>::kIsNumeric;

    auto *typed_dest = VerboseCast<chunkType_t *>(DEEPHAVEN_LOCATION_EXPR(dest_data));
    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= src_data.size()));
    TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= typed_dest->Size()));
    if (!kTypeIsNumeric) {
      TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(src_null_flags != nullptr));
      TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= src_null_flags->size()));
    }
    if (optional_dest_null_flags != nullptr) {
      TrueOrThrow(DEEPHAVEN_LOCATION_EXPR(rows.Size() <= optional_dest_null_flags->Size()));
    }
    (void) src_null_flags;  // avoid CLion warning about unused variable.

    auto *destp = typed_dest->data();
    auto *dest_nullp = optional_dest_null_flags != nullptr ? optional_dest_null_flags->data() : nullptr;

    // Note: Uses random access with Immer, which is significantly more expensive than iterating
    // over contiguous Immer ranges.
    for (auto key : rows) {
      auto value = src_data[key];
      *destp++ = value;
      if (dest_nullp != nullptr) {
        if constexpr(kTypeIsNumeric) {
          *dest_nullp++ = value == deephaven::dhcore::DeephavenTraits<T>::kNullValue;
        } else {
          *dest_nullp++ = (*src_null_flags)[key];
        }
      }
    }
  }
};
}  // namespace internal

class ImmerColumnSource : public virtual deephaven::dhcore::column::ColumnSource {
};

template<typename T>
class NumericImmerColumnSource final : public ImmerColumnSource,
    public deephaven::dhcore::column::NumericColumnSource<T>,
    std::enable_shared_from_this<NumericImmerColumnSource<T>> {
  struct Private {};

  using Chunk = deephaven::dhcore::chunk::Chunk;
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;
  using RowSequence = deephaven::dhcore::container::RowSequence;

public:
  static std::shared_ptr<NumericImmerColumnSource> Create(immer::flex_vector<T> data) {
    return std::make_shared<NumericImmerColumnSource>(Private(), std::move(data));
  }

  explicit NumericImmerColumnSource(Private, immer::flex_vector<T> data) : data_(std::move(data)) {}

  ~NumericImmerColumnSource() final = default;

  void FillChunk(const RowSequence &rows, Chunk *dest_data, BooleanChunk *optional_dest_null_flags) const final {
    internal::ImmerColumnSourceImpls::FillChunk(data_, nullptr, rows, dest_data,
        optional_dest_null_flags);
  }

  void FillChunkUnordered(const UInt64Chunk &rows, Chunk *dest_data,
      BooleanChunk *optional_dest_null_flags) const final {
    internal::ImmerColumnSourceImpls::FillChunkUnordered(data_, nullptr, rows, dest_data,
        optional_dest_null_flags);
  }

  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

private:
  immer::flex_vector<T> data_;
};

template<typename T>
class GenericImmerColumnSource final : public ImmerColumnSource,
    public deephaven::dhcore::column::GenericColumnSource<T>,
    std::enable_shared_from_this<GenericImmerColumnSource<T>> {
  struct Private {};
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;
public:
  static std::shared_ptr<GenericImmerColumnSource> Create(immer::flex_vector<T> data,
      immer::flex_vector<bool> null_flags) {
    return std::make_shared<GenericImmerColumnSource>(Private(), std::move(data), std::move(null_flags));
  }

  GenericImmerColumnSource(Private, immer::flex_vector<T> &&data, immer::flex_vector<bool> &&null_flags) :
      data_(std::move(data)), null_flags_(std::move(null_flags)) {}
  ~GenericImmerColumnSource() final = default;

  void FillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optional_dest_null_flags) const final {
    internal::ImmerColumnSourceImpls::FillChunk(data_, &null_flags_, rows, dest,
        optional_dest_null_flags);
  }

  void FillChunkUnordered(const UInt64Chunk &rows, Chunk *dest,
      BooleanChunk *optional_dest_null_flags) const final {
    internal::ImmerColumnSourceImpls::FillChunkUnordered(data_, &null_flags_, rows, dest,
        optional_dest_null_flags);
  }

  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

private:
  immer::flex_vector<T> data_;
  immer::flex_vector<bool> null_flags_;
};
}  // namespace deephaven::dhcore::immerutil
