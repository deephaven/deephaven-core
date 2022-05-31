package io.deephaven.engine.table.impl.chunkattributes;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.ChunkPositions;

/**
 * Attribute that specifies that a {@link Chunk} contains positions within another Chunk that represents a dictionary of
 * values.
 */
public interface DictionaryKeys extends ChunkPositions {
}
