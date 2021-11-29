package io.deephaven.engine.rowset.chunkattributes;

import io.deephaven.chunk.Chunk;

/**
 * Attribute that specifies that a {@link Chunk} contains unordered row keys, which may be in any order or be
 * duplicative.
 */
public interface UnorderedRowKeys extends RowKeys {
}
