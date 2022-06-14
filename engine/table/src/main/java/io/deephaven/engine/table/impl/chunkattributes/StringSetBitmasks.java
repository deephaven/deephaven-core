/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.chunkattributes;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;

/**
 * Attribute that specifies that a {@link Chunk} contains {@code long} values which are StringSets endoded as a bitmask.
 */
public interface StringSetBitmasks extends Values {
}
