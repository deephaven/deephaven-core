/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <vector>
#include <arrow/array.h>
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/chunk/chunk.h"

namespace deephaven::client::chunk {
class ChunkFiller {
  typedef deephaven::client::container::RowSequence RowSequence;
public:
  static void fillChunk(const arrow::Array &src, const RowSequence &keys, Chunk *destData,
      BooleanChunk *optionalDestNullFlags);
};
}  // namespace deephaven::client::chunk
