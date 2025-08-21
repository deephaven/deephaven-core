//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableIntChunk;
import io.deephaven.chunk.ResettableWritableIntChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Interface for pools of {@link WritableIntChunk}, {@link ResettableIntChunk}, and
 * {@link ResettableWritableIntChunk}.
 */
public interface IntChunkPool {

    /**
     * @return This IntChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect a
     *         {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableIntChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableIntChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableIntChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableIntChunk#close() closed}
     */
    <ATTR extends Any> WritableIntChunk<ATTR> takeWritableIntChunk(int capacity);

    /**
     * Take a {@link ResettableIntChunk}. The result belongs to the caller until {@link ResettableIntChunk#close()
     * closed}.
     *
     * @return A {@link ResettableIntChunk} that belongs to the caller until {@link ResettableIntChunk#close() closed}
     */
    <ATTR extends Any> ResettableIntChunk<ATTR> takeResettableIntChunk();

    /**
     * Take a {@link ResettableWritableIntChunk}. The result belongs to the caller until
     * {@link ResettableWritableIntChunk#close() closed}.
     *
     * @return A {@link ResettableWritableIntChunk} that belongs to the caller until
     *         {@link ResettableWritableIntChunk#close() closed}
     */
    <ATTR extends Any> ResettableWritableIntChunk<ATTR> takeResettableWritableIntChunk();
}
