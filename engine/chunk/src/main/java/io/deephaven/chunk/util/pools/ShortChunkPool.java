//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableShortChunk;
import io.deephaven.chunk.ResettableWritableShortChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Interface for pools of {@link WritableShortChunk}, {@link ResettableShortChunk}, and
 * {@link ResettableWritableShortChunk}.
 */
public interface ShortChunkPool {

    /**
     * @return This ShortChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect a
     *         {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableShortChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableShortChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableShortChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableShortChunk#close() closed}
     */
    <ATTR extends Any> WritableShortChunk<ATTR> takeWritableShortChunk(int capacity);

    /**
     * Take a {@link ResettableShortChunk}. The result belongs to the caller until {@link ResettableShortChunk#close()
     * closed}.
     *
     * @return A {@link ResettableShortChunk} that belongs to the caller until {@link ResettableShortChunk#close() closed}
     */
    <ATTR extends Any> ResettableShortChunk<ATTR> takeResettableShortChunk();

    /**
     * Take a {@link ResettableWritableShortChunk}. The result belongs to the caller until
     * {@link ResettableWritableShortChunk#close() closed}.
     *
     * @return A {@link ResettableWritableShortChunk} that belongs to the caller until
     *         {@link ResettableWritableShortChunk#close() closed}
     */
    <ATTR extends Any> ResettableWritableShortChunk<ATTR> takeResettableWritableShortChunk();
}
