//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableBooleanChunk;
import io.deephaven.chunk.ResettableWritableBooleanChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Interface for pools of {@link WritableBooleanChunk}, {@link ResettableBooleanChunk}, and
 * {@link ResettableWritableBooleanChunk}.
 */
public interface BooleanChunkPool {

    /**
     * @return This BooleanChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect a
     *         {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableBooleanChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableBooleanChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableBooleanChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableBooleanChunk#close() closed}
     */
    <ATTR extends Any> WritableBooleanChunk<ATTR> takeWritableBooleanChunk(int capacity);

    /**
     * Take a {@link ResettableBooleanChunk}. The result belongs to the caller until {@link ResettableBooleanChunk#close()
     * closed}.
     *
     * @return A {@link ResettableBooleanChunk} that belongs to the caller until {@link ResettableBooleanChunk#close() closed}
     */
    <ATTR extends Any> ResettableBooleanChunk<ATTR> takeResettableBooleanChunk();

    /**
     * Take a {@link ResettableWritableBooleanChunk}. The result belongs to the caller until
     * {@link ResettableWritableBooleanChunk#close() closed}.
     *
     * @return A {@link ResettableWritableBooleanChunk} that belongs to the caller until
     *         {@link ResettableWritableBooleanChunk#close() closed}
     */
    <ATTR extends Any> ResettableWritableBooleanChunk<ATTR> takeResettableWritableBooleanChunk();
}
