/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/chunk/chunk_filler.h"

#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/impl/util.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::stringf;
using deephaven::client::utility::verboseCast;
using deephaven::client::container::RowSequence;

namespace deephaven::client::chunk {
namespace {
struct Visitor final : arrow::ArrayVisitor {
  Visitor(const RowSequence &keys, Chunk *dest) : keys_(keys), dest_(dest) {}

  arrow::Status Visit(const arrow::Int32Array &array) final {
    return fillNumericChunk<int32_t>(array);
  }

  arrow::Status Visit(const arrow::Int64Array &array) final {
    return fillNumericChunk<int64_t>(array);
  }

  arrow::Status Visit(const arrow::UInt64Array &array) final {
    return fillNumericChunk<uint64_t>(array);
  }

  arrow::Status Visit(const arrow::DoubleArray &array) final {
    return fillNumericChunk<double>(array);
  }

  template<typename T, typename TArrowAray>
  arrow::Status fillNumericChunk(const TArrowAray &array) {
    auto *typedDest = verboseCast<GenericChunk<T>*>(dest_, DEEPHAVEN_PRETTY_FUNCTION);
    checkSize(typedDest->size());
    size_t destIndex = 0;
    auto copyChunk = [&destIndex, &array, typedDest](uint64_t begin, uint64_t end) {
      for (auto current = begin; current != end; ++current) {
        if (array.IsNull(current)) {
          throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Not handling nulls yet"));
        }
        auto val = array.Value((int64_t)current);
        typedDest->data()[destIndex] = val;
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
  Chunk *const dest_;
};
}  // namespace

void ChunkFiller::fillChunk(const arrow::Array &src, const RowSequence &keys, Chunk *dest) {
  Visitor visitor(keys, dest);
  okOrThrow(DEEPHAVEN_EXPR_MSG(src.Accept(&visitor)));
}
}  // namespace deephaven::client::chunk
