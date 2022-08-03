/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk.attributes;

import io.deephaven.chunk.Chunk;

/**
 * Attribute that specifies that a {@link Chunk} contains positions within some other Chunk.
 */
public interface ChunkPositions extends Indices {
}
