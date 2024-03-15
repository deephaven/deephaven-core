//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.chunkattributes;

import io.deephaven.chunk.Chunk;

/**
 * Attribute that specifies that a {@link Chunk} contains individual ordered row keys, which must be in strictly
 * ascending order.
 */
public interface OrderedRowKeys extends RowKeys {
}
