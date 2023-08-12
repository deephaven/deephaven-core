/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdlib>
#include "flatbuffers/flatbuffers.h"
#include "deephaven/dhcore/container/row_sequence.h"

namespace deephaven::dhcore::ticking {
class DataInput {
public:
  explicit DataInput(const flatbuffers::Vector<int8_t> &vec) : DataInput(vec.data(), vec.size()) {}

  DataInput(const void *start, size_t size) : data_(static_cast<const char *>(start)),
      end_(data_ + size) {}

  [[nodiscard]] int64_t ReadValue(int command);

  [[nodiscard]] int64_t ReadLong();
  [[nodiscard]] int32_t ReadInt();
  [[nodiscard]] int16_t ReadShort();
  [[nodiscard]] int8_t ReadByte();

private:
  const char *data_ = nullptr;
  const char *end_ = nullptr;
};

struct IndexDecoder {
  using RowSequence = deephaven::dhcore::container::RowSequence;

  [[nodiscard]] static std::shared_ptr<RowSequence> ReadExternalCompressedDelta(DataInput *in);
};
}  // namespace deephaven::dhcore::ticking
