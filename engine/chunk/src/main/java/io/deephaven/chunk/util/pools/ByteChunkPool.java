//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableByteChunk;
import io.deephaven.chunk.ResettableWritableByteChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Interface for pools of {@link WritableByteChunk}, {@link ResettableByteChunk}, and
 * {@link ResettableWritableByteChunk}.
 */
public interface ByteChunkPool {

    /**
     * @return This ByteChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect a
     *         {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableByteChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableByteChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableByteChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableByteChunk#close() closed}
     */
    <ATTR extends Any> WritableByteChunk<ATTR> takeWritableByteChunk(int capacity);

    /**
     * Take a {@link ResettableByteChunk}. The result belongs to the caller until {@link ResettableByteChunk#close()
     * closed}.
     *
     * @return A {@link ResettableByteChunk} that belongs to the caller until {@link ResettableByteChunk#close() closed}
     */
    <ATTR extends Any> ResettableByteChunk<ATTR> takeResettableByteChunk();

    /**
     * Take a {@link ResettableWritableByteChunk}. The result belongs to the caller until
     * {@link ResettableWritableByteChunk#close() closed}.
     *
     * @return A {@link ResettableWritableByteChunk} that belongs to the caller until
     *         {@link ResettableWritableByteChunk#close() closed}
     */
    <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk();
}
