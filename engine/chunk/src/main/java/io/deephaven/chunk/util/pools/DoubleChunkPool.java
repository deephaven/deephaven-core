//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableDoubleChunk;
import io.deephaven.chunk.ResettableWritableDoubleChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Interface for pools of {@link WritableDoubleChunk}, {@link ResettableDoubleChunk}, and
 * {@link ResettableWritableDoubleChunk}.
 */
public interface DoubleChunkPool {

    /**
     * @return This DoubleChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect a
     *         {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableDoubleChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableDoubleChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableDoubleChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableDoubleChunk#close() closed}
     */
    <ATTR extends Any> WritableDoubleChunk<ATTR> takeWritableDoubleChunk(int capacity);

    /**
     * Take a {@link ResettableDoubleChunk}. The result belongs to the caller until {@link ResettableDoubleChunk#close()
     * closed}.
     *
     * @return A {@link ResettableDoubleChunk} that belongs to the caller until {@link ResettableDoubleChunk#close() closed}
     */
    <ATTR extends Any> ResettableDoubleChunk<ATTR> takeResettableDoubleChunk();

    /**
     * Take a {@link ResettableWritableDoubleChunk}. The result belongs to the caller until
     * {@link ResettableWritableDoubleChunk#close() closed}.
     *
     * @return A {@link ResettableWritableDoubleChunk} that belongs to the caller until
     *         {@link ResettableWritableDoubleChunk#close() closed}
     */
    <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk();
}
