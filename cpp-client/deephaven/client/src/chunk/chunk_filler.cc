/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/chunk/chunk_filler.h"

#include "deephaven/client/arrowutil/arrow_visitors.h"
#include "deephaven/client/arrowutil/arrow_value_converter.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/impl/util.h"
#include "deephaven/client/types.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::arrowutil::isNumericType;
using deephaven::client::arrowutil::ArrowValueConverter;
using deephaven::client::container::RowSequence;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::stringf;
using deephaven::client::utility::verboseCast;

namespace deephaven::client::chunk {
namespace {
struct Visitor final : arrow::ArrayVisitor {
  Visitor(const RowSequence &keys, Chunk *destData, BooleanChunk *optionalDestNullFlags) :
      keys_(keys), destData_(destData), optionalDestNullFlags_(optionalDestNullFlags) {}

  arrow::Status Visit(const arrow::Int8Array &array) final {
    return fillChunk<int8_t>(array);
  }

  arrow::Status Visit(const arrow::Int16Array &array) final {
    return fillChunk<int16_t>(array);
  }

  arrow::Status Visit(const arrow::Int32Array &array) final {
    return fillChunk<int32_t>(array);
  }

  arrow::Status Visit(const arrow::Int64Array &array) final {
    return fillChunk<int64_t>(array);
  }

  arrow::Status Visit(const arrow::FloatArray &array) final {
    return fillChunk<float>(array);
  }

  arrow::Status Visit(const arrow::DoubleArray &array) final {
    return fillChunk<double>(array);
  }

  arrow::Status Visit(const arrow::StringArray &array) final {
    return fillChunk<std::string>(array);
  }

  arrow::Status Visit(const arrow::BooleanArray &array) final {
    return fillChunk<bool>(array);
  }

  template<typename T, typename TArrowAray>
  arrow::Status fillChunk(const TArrowAray &array) {
    auto *typedDest = verboseCast<GenericChunk<T>*>(DEEPHAVEN_EXPR_MSG(destData_));
    auto *destDatap = typedDest->data();
    auto *destNullp = optionalDestNullFlags_ != nullptr ? optionalDestNullFlags_->data() : nullptr;
    checkSize(typedDest->size());
    size_t destIndex = 0;

    auto copyChunk =
        [&array, destDatap, destNullp, &destIndex](uint64_t begin, uint64_t end) {
      auto beginp = array.begin() + begin;
      auto endp = array.begin() + end;
      for (auto currentp = beginp; currentp != endp; ++currentp) {
        auto optValue = *currentp;
        bool isNull = !optValue.has_value();
        T value = T();
        if (isNull) {
          if constexpr(isNumericType<T>()) {
            value = deephaven::client::DeephavenConstantsForType<T>::NULL_VALUE;
          }
        } else {
          ArrowValueConverter::convert(*optValue, &value);
        }
        destDatap[destIndex] = std::move(value);
        if (destNullp != nullptr) {
          destNullp[destIndex] = isNull;
        }
        ++destIndex;
      }
    };
    keys_.forEachChunk(copyChunk);
    return arrow::Status::OK();
  }

  void checkSize(size_t destSize) {
    if (destSize < keys_.size()) {
      throw std::runtime_error(
          DEEPHAVEN_DEBUG_MSG(stringf("destSize < keys_.size() (%d < %d)", destSize, keys_.size())));
    }
  }

  const RowSequence &keys_;
  Chunk *const destData_ = nullptr;
  BooleanChunk *const optionalDestNullFlags_ = nullptr;
};
}  // namespace

void ChunkFiller::fillChunk(const arrow::Array &src, const RowSequence &keys, Chunk *destData,
    BooleanChunk *optionalDestNullFlags) {
  Visitor visitor(keys, destData, optionalDestNullFlags);
  okOrThrow(DEEPHAVEN_EXPR_MSG(src.Accept(&visitor)));
}
}  // namespace deephaven::client::chunk
