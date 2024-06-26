//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;

/**
 *
 */
public interface ChunkInputStreamGeneratorFactory {
    /**
     *
     * @param chunkType
     * @param type
     * @param componentType
     * @param chunk
     * @param rowOffset
     * @return
     * @param <T>
     */
    <T> ChunkInputStreamGenerator makeInputStreamGenerator(
            final ChunkType chunkType,
            final Class<T> type,
            final Class<?> componentType,
            final Chunk<Values> chunk,
            final long rowOffset);
}
