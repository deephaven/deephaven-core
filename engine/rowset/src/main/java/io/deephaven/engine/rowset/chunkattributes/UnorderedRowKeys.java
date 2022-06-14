/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.chunkattributes;

import io.deephaven.chunk.Chunk;

/**
 * Attribute that specifies that a {@link Chunk} contains unordered row keys, which may be in any order or be
 * duplicative.
 */
public interface UnorderedRowKeys extends RowKeys {
}
