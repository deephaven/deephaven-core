//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableFloatChunk;
import io.deephaven.chunk.ResettableWritableFloatChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Interface for pools of {@link WritableFloatChunk}, {@link ResettableFloatChunk}, and
 * {@link ResettableWritableFloatChunk}.
 */
public interface FloatChunkPool {

    /**
     * @return This FloatChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect a
     *         {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableFloatChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableFloatChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableFloatChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableFloatChunk#close() closed}
     */
    <ATTR extends Any> WritableFloatChunk<ATTR> takeWritableFloatChunk(int capacity);

    /**
     * Take a {@link ResettableFloatChunk}. The result belongs to the caller until {@link ResettableFloatChunk#close()
     * closed}.
     *
     * @return A {@link ResettableFloatChunk} that belongs to the caller until {@link ResettableFloatChunk#close() closed}
     */
    <ATTR extends Any> ResettableFloatChunk<ATTR> takeResettableFloatChunk();

    /**
     * Take a {@link ResettableWritableFloatChunk}. The result belongs to the caller until
     * {@link ResettableWritableFloatChunk#close() closed}.
     *
     * @return A {@link ResettableWritableFloatChunk} that belongs to the caller until
     *         {@link ResettableWritableFloatChunk#close() closed}
     */
    <ATTR extends Any> ResettableWritableFloatChunk<ATTR> takeResettableWritableFloatChunk();
}
