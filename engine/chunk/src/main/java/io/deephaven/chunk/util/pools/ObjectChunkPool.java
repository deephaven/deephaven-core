//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableObjectChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;

public interface ObjectChunkPool {

    /**
     * @return This ObjectChunkPool as a {@link ChunkPool}. This is useful for passing this pool to methods that expect
     *         a {@link ChunkPool} but do not need to know the specific type.
     */
    ChunkPool asChunkPool();

    /**
     * Take a {@link WritableObjectChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link WritableObjectChunk#close() closed}.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableObjectChunk} of at least the specified {@code capacity} that belongs to the caller until
     *         {@link WritableObjectChunk#close() closed}
     */
    <TYPE, ATTR extends Any> WritableObjectChunk<TYPE, ATTR> takeWritableObjectChunk(int capacity);

    /**
     * Take a {@link ResettableObjectChunk} of at least the specified {@code capacity}. The result belongs to the caller
     * until {@link ResettableObjectChunk#close() closed}.
     *
     * @return A {@link ResettableObjectChunk} of at least the specified {@code capacity} that belongs to the caller
     *         until {@link ResettableObjectChunk#close() closed}
     */
    <TYPE, ATTR extends Any> ResettableObjectChunk<TYPE, ATTR> takeResettableObjectChunk();

    /**
     * Take a {@link ResettableWritableObjectChunk} of at least the specified {@code capacity}. The result belongs to
     * the caller until {@link ResettableWritableObjectChunk#close() closed}.
     *
     * @return A {@link ResettableWritableObjectChunk} of at least the specified {@code capacity} that belongs to the
     *         caller until {@link ResettableWritableObjectChunk#close() closed}
     */
    <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk();
}
