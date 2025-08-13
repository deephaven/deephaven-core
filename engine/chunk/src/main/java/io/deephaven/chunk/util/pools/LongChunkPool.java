//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableLongChunk;
import io.deephaven.chunk.ResettableWritableLongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Interface for pools of {@link WritableLongChunk}, {@link ResettableLongChunk}, and
 * {@link ResettableWritableLongChunk}.
 */
public interface LongChunkPool {

    /**
     * @return This LongChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect a
     *         {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableLongChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableLongChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableLongChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableLongChunk#close() closed}
     */
    <ATTR extends Any> WritableLongChunk<ATTR> takeWritableLongChunk(int capacity);

    /**
     * Take a {@link ResettableLongChunk}. The result belongs to the caller until {@link ResettableLongChunk#close()
     * closed}.
     *
     * @return A {@link ResettableLongChunk} that belongs to the caller until {@link ResettableLongChunk#close() closed}
     */
    <ATTR extends Any> ResettableLongChunk<ATTR> takeResettableLongChunk();

    /**
     * Take a {@link ResettableWritableLongChunk}. The result belongs to the caller until
     * {@link ResettableWritableLongChunk#close() closed}.
     *
     * @return A {@link ResettableWritableLongChunk} that belongs to the caller until
     *         {@link ResettableWritableLongChunk#close() closed}
     */
    <ATTR extends Any> ResettableWritableLongChunk<ATTR> takeResettableWritableLongChunk();
}
