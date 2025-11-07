//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;

/**
 * Interface for pools of {@link Chunk}s.
 */
public interface ChunkPool {

    /**
     * Take a {@link WritableChunk} of at least the specified {@code capacity}. The result belongs to the caller until
     * {@link WritableChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableChunk#close() closed}
     */
    <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(int capacity);

    /**
     * Take a {@link ResettableReadOnlyChunk}. The result belongs to the caller until
     * {@link ResettableReadOnlyChunk#close() closed}.
     *
     * @return A {@link ResettableReadOnlyChunk} that belongs to the caller until {@link ResettableReadOnlyChunk#close()
     *         closed}
     */
    <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk();

    /**
     * Take a {@link ResettableWritableChunk}. The result belongs to the caller until
     * {@link ResettableWritableChunk#close() closed}.
     *
     * @return A {@link ResettableWritableChunk} that belongs to the caller until {@link ResettableWritableChunk#close()
     *         closed}
     */
    <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk();
}
