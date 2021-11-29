package io.deephaven.engine.rowset.chunkattributes;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Indices;

/**
 * Attribute that specifies that a {@link Chunk} contains row keys, which may be ordered or unordered.
 */
public interface RowKeys extends Indices {
}
