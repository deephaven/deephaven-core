/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <vector>
#include "deephaven/dhcore/chunk/chunk.h"

namespace deephaven::dhcore::column {
class ColumnSource;
}  // namespace deephaven::dhcore::column

namespace deephaven::dhcore::chunk {
/**
 * Factory class for creating Chunk objects.
 */
class ChunkMaker {
  typedef deephaven::dhcore::column::ColumnSource ColumnSource;
public:
  /**
   * Create a Chunk compatible with the specified ColumnSource. For example if the underlying
   * element type of the ColumnSource is int32_t, this method will create an Int32Chunk.
   * @param columnSource The column source whose underlying element type will be inspected.
   * @param chunkSize The requested size of the chunk.
   * @return An AnyChunk, which is a variant value containing the requested chunk.
   */
  static AnyChunk createChunkFor(const ColumnSource &columnSource, size_t chunkSize);
};
}  // namespace deephaven::client::chunk
