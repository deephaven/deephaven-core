/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <vector>
#include <arrow/array.h>
#include "deephaven/client/chunk/chunk.h"

namespace deephaven::client::column {
class ColumnSource;
}  // namespace deephaven::client::column

namespace deephaven::client::chunk {
class ChunkMaker {
  typedef deephaven::client::column::ColumnSource ColumnSource;
public:
  static AnyChunk createChunkFor(const ColumnSource &columnSource, size_t chunkSize);
};
}  // namespace deephaven::client::chunk
