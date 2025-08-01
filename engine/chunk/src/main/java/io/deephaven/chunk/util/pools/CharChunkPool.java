//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableCharChunk;
import io.deephaven.chunk.ResettableWritableCharChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Interface for pools of {@link WritableCharChunk}, {@link ResettableCharChunk}, and
 * {@link ResettableWritableCharChunk}.
 */
public interface CharChunkPool {

    /**
     * @return This CharChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect a
     *         {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableCharChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableCharChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableCharChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableCharChunk#close() closed}
     */
    <ATTR extends Any> WritableCharChunk<ATTR> takeWritableCharChunk(int capacity);

    /**
     * Take a {@link ResettableCharChunk}. The result belongs to the caller until {@link ResettableCharChunk#close()
     * closed}.
     *
     * @return A {@link ResettableCharChunk} that belongs to the caller until {@link ResettableCharChunk#close() closed}
     */
    <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk();

    /**
     * Take a {@link ResettableWritableCharChunk}. The result belongs to the caller until
     * {@link ResettableWritableCharChunk#close() closed}.
     *
     * @return A {@link ResettableWritableCharChunk} that belongs to the caller until
     *         {@link ResettableWritableCharChunk#close() closed}
     */
    <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk();
}
