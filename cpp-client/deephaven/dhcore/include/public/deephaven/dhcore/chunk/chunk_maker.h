/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
  using ColumnSource = deephaven::dhcore::column::ColumnSource;
public:
  /**
   * Create a Chunk compatible with the specified ColumnSource. For example if the underlying
   * element type of the ColumnSource is int32_t, this method will Create an Int32Chunk.
   * @param column_source The column source whose underlying element type will be inspected.
   * @param chunk_size The requested size of the chunk.
   * @return An AnyChunk, which is a variant value containing the requested chunk.
   */
  [[nodiscard]]
  static AnyChunk CreateChunkFor(const ColumnSource &column_source, size_t chunk_size);
};
}  // namespace deephaven::client::chunk
