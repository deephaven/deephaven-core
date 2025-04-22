/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>
#include <arrow/array.h>
#include <arrow/type.h>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::arrowutil {
namespace internal {
// Because this is a template function and we want to share code, we encode the
// differences in behavior with this enum.
// kNormal is used for the "simple" Deephaven primitive types (e.g. char, float, short), which have
// special reserved values for nulls (e.g. DeephavenConstants.kNullChar), and copying can be
// done with simple pointer and assignment operations.
// kBooleanOrString uses Arrow iterators for copying (which yield std::optional<T>), and determines
// null-ness by determining whether the optional has a value.
// kTimestamp is its own special case, where nullness is determined by the underlying nanos
// being equal to Deephaven's NULL_LONG.
// kLocalDate and kLocalTime are similar to kTimestamp in nullness except they resolve to different
// data types.
enum class ArrowProcessingStyle { kNormal, kBooleanOrString, kTimestamp, kLocalDate, kLocalTime };

/**
 * When 'array' has dynamic type arrow::TimestampArray or arrow::Time64Array, we need to look at the
 * underlying time resolution of the arrow type and calculate a conversion factor from that unit
 * to nanoseconds. For example if the underlying time unit is arrow::TimeUnit::MILLI, then the
 * conversion factor would be 1_000_000, meaning that one needs to multiply incoming millisecond
 * values by one million to convert them to nanoseconds. This method converts the Arrow TimeUnit
 * type to that conversion factor. If the TimeUnit is not supported, it throws an exception.
 * @param unit The Arrow time unit.
 * @return For supported time types, the conversion factor to nanoseconds. Otherwise, throws
 *   exception.
 */
size_t ScaleFromUnit(arrow::TimeUnit::type unit);

template<ArrowProcessingStyle Style, typename TColumnSourceBase, typename TArrowArray, typename TChunk>
class GenericArrowColumnSource final : public TColumnSourceBase {
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;
  using DateTime = deephaven::dhcore::DateTime;
  using ElementType = deephaven::dhcore::ElementType;
  using LocalDate = deephaven::dhcore::LocalDate;
  using LocalTime = deephaven::dhcore::LocalTime;
  using RowSequence = deephaven::dhcore::container::RowSequence;
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;

public:
  static std::shared_ptr<GenericArrowColumnSource> OfArrowArrayVec(
      const ElementType &element_type,
      std::vector<std::shared_ptr<TArrowArray>> arrays) {
    return std::make_shared<GenericArrowColumnSource>(element_type, std::move(arrays));
  }

  GenericArrowColumnSource(const ElementType &element_type,
      std::vector<std::shared_ptr<TArrowArray>> arrays) :
      element_type_(element_type), arrays_(std::move(arrays)) {
  }

  ~GenericArrowColumnSource() final = default;

  void FillChunk(const RowSequence &rows, Chunk *dest_data,
      BooleanChunk *optional_dest_null_flags) const final {
    using deephaven::dhcore::DeephavenTraits;
    using deephaven::dhcore::utility::VerboseCast;

    if (rows.Empty()) {
      return;
    }
    if (arrays_.empty()) {
      const auto *message = "Ran out of source data before processing whole RowSequence";
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }

    // This algorithm is a little tricky because the source data and RowSequence are both
    // segmented, perhaps in different ways.
    auto *typed_dest = VerboseCast<TChunk *>(DEEPHAVEN_LOCATION_EXPR(dest_data));
    auto *destp = typed_dest->data();
    auto outerp = arrays_.begin();
    size_t src_segment_begin = 0;
    size_t src_segment_end = (*outerp)->length();

    auto *null_destp =
        optional_dest_null_flags != nullptr ? optional_dest_null_flags->data() : nullptr;

    rows.ForEachInterval([&](uint64_t requested_segment_begin, uint64_t requested_segment_end) {
      while (true) {
        if (requested_segment_begin == requested_segment_end) {
          return;
        }
        if (requested_segment_begin >= src_segment_end) {
          // src_segment needs to catch up
          ++outerp;
          if (outerp == arrays_.end()) {
            const auto *message = "Ran out of source data before processing whole RowSequence";
            throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
          }
          src_segment_begin = src_segment_end;
          src_segment_end = src_segment_begin + (*outerp)->length();
          continue;
        }
        if (requested_segment_begin < src_segment_begin) {
          throw "can't happen";
        }
        auto min_end = std::min(requested_segment_end, src_segment_end);
        // [relative_begin, relative_end) are the coordinates of the source data relative to the
        // start of the current data segment being pointed to.
        auto relative_begin = requested_segment_begin - src_segment_begin;
        auto relative_end = min_end - src_segment_begin;
        const auto &innerp = *outerp;

        static_assert(
            Style == ArrowProcessingStyle::kNormal ||
            Style == ArrowProcessingStyle::kBooleanOrString ||
            Style == ArrowProcessingStyle::kTimestamp ||
            Style == ArrowProcessingStyle::kLocalDate ||
            Style == ArrowProcessingStyle::kLocalTime,
            "Unexpected ArrowProcessingStyle");

        if constexpr (Style == ArrowProcessingStyle::kNormal) {
          // Process these types using pointer operations and the Deephaven Null convention
          const auto *src_beginp = innerp->raw_values() + relative_begin;
          const auto *src_endp = innerp->raw_values() + relative_end;
          std::copy(src_beginp, src_endp, destp);
          destp += src_endp - src_beginp;

          if (null_destp != nullptr) {
            for (const auto *current = src_beginp; current != src_endp; ++current) {
              *null_destp = *current == DeephavenTraits<typename TChunk::value_type>::kNullValue;
              ++null_destp;
            }
          }
        } else if constexpr (Style == ArrowProcessingStyle::kBooleanOrString) {
          // Process booleans and strings by using the Arrow iterator which yields optionals;
          // which also gives us access to the Arrow validity array.
          const auto src_beginp = innerp->begin() + relative_begin;
          const auto src_endp = innerp->begin() + relative_end;
          for (auto ip = src_beginp; ip != src_endp; ++ip) {
            const auto &optional_element = *ip;
            if (optional_element.has_value()) {
              *destp = *optional_element;
            } else {
              *destp = typename TChunk::value_type();
            }
            ++destp;

            if (null_destp != nullptr) {
              *null_destp = !optional_element.has_value();
              ++null_destp;
            }
          }
        } else if constexpr (Style == ArrowProcessingStyle::kTimestamp) {
          // Process these types using pointer operations and the Deephaven Null convention
          const auto *src_beginp = innerp->raw_values() + relative_begin;
          const auto *src_endp = innerp->raw_values() + relative_end;

          const auto *typed_type = VerboseCast<const arrow::TimestampType*>(
              DEEPHAVEN_LOCATION_EXPR(innerp->type().get()));
          auto time_nano_scale_factor = ScaleFromUnit(typed_type->unit());

          for (const auto *ip = src_beginp; ip != src_endp; ++ip) {
            auto is_null = *ip == DeephavenTraits<int64_t>::kNullValue;
            *destp = DateTime::FromNanos(is_null ? *ip : (*ip * time_nano_scale_factor));
            ++destp;

            if (null_destp != nullptr) {
              *null_destp = is_null;
              ++null_destp;
            }
          }
        } else if constexpr (Style == ArrowProcessingStyle::kLocalDate) {
          // Process these types using pointer operations and the Deephaven Null convention
          const auto *src_beginp = innerp->raw_values() + relative_begin;
          const auto *src_endp = innerp->raw_values() + relative_end;

          for (const auto *ip = src_beginp; ip != src_endp; ++ip) {
            *destp = LocalDate::FromMillis(*ip);
            ++destp;

            if (null_destp != nullptr) {
              *null_destp = *ip == DeephavenTraits<int64_t>::kNullValue;
              ++null_destp;
            }
          }
        } else if constexpr (Style == ArrowProcessingStyle::kLocalTime) {
          // Process these types using pointer operations and the Deephaven Null convention
          const auto *src_beginp = innerp->raw_values() + relative_begin;
          const auto *src_endp = innerp->raw_values() + relative_end;

          const auto *typed_type = VerboseCast<const arrow::Time64Type*>(
              DEEPHAVEN_LOCATION_EXPR(innerp->type().get()));
          auto time_nano_scale_factor = ScaleFromUnit(typed_type->unit());

          for (const auto *ip = src_beginp; ip != src_endp; ++ip) {
            auto is_null = *ip == DeephavenTraits<int64_t>::kNullValue;
            *destp = LocalTime::FromNanos(is_null ? *ip : (*ip * time_nano_scale_factor));
            ++destp;

            if (null_destp != nullptr) {
              *null_destp = is_null;
              ++null_destp;
            }
          }
        }
        requested_segment_begin = min_end;
      }
    });
  }

  void FillChunkUnordered(const UInt64Chunk &rows, Chunk *dest_data,
      BooleanChunk *optional_dest_null_flags) const final {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Not implemented"));
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
  std::vector<std::shared_ptr<TArrowArray>> arrays_;
};
}  // namespace internal

using Int8ArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kNormal,
    deephaven::dhcore::column::Int8ColumnSource,
    arrow::Int8Array,
    deephaven::dhcore::chunk::Int8Chunk>;

using Int16ArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kNormal,
    deephaven::dhcore::column::Int16ColumnSource,
    arrow::Int16Array,
    deephaven::dhcore::chunk::Int16Chunk>;

using Int32ArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kNormal,
    deephaven::dhcore::column::Int32ColumnSource,
    arrow::Int32Array,
    deephaven::dhcore::chunk::Int32Chunk>;

using Int64ArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kNormal,
    deephaven::dhcore::column::Int64ColumnSource,
    arrow::Int64Array,
    deephaven::dhcore::chunk::Int64Chunk>;

using FloatArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kNormal,
    deephaven::dhcore::column::FloatColumnSource,
    arrow::FloatArray,
    deephaven::dhcore::chunk::FloatChunk>;

using DoubleArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kNormal,
    deephaven::dhcore::column::DoubleColumnSource,
    arrow::DoubleArray,
    deephaven::dhcore::chunk::DoubleChunk>;

using CharArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kNormal,
    deephaven::dhcore::column::CharColumnSource,
    arrow::UInt16Array,
    deephaven::dhcore::chunk::CharChunk>;

using BooleanArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kBooleanOrString,
    deephaven::dhcore::column::BooleanColumnSource,
    arrow::BooleanArray,
    deephaven::dhcore::chunk::BooleanChunk>;

using StringArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kBooleanOrString,
    deephaven::dhcore::column::StringColumnSource,
    arrow::StringArray,
    deephaven::dhcore::chunk::StringChunk>;

using DateTimeArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kTimestamp,
    deephaven::dhcore::column::DateTimeColumnSource,
    arrow::TimestampArray,
    deephaven::dhcore::chunk::DateTimeChunk>;

using LocalDateArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kLocalDate,
    deephaven::dhcore::column::LocalDateColumnSource,
    arrow::Date64Array,
    deephaven::dhcore::chunk::LocalDateChunk>;

using LocalTimeArrowColumnSource = internal::GenericArrowColumnSource<
    internal::ArrowProcessingStyle::kLocalTime,
    deephaven::dhcore::column::LocalTimeColumnSource,
    arrow::Time64Array,
    deephaven::dhcore::chunk::LocalTimeChunk>;
}  // namespace deephaven::client::arrowutil
