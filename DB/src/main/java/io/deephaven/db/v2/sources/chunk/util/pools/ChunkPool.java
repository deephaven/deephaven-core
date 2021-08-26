package io.deephaven.db.v2.sources.chunk.util.pools;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ResettableReadOnlyChunk;
import io.deephaven.db.v2.sources.chunk.ResettableWritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for pools of {@link Chunk}s.
 */
public interface ChunkPool {

    /**
     * Take a {@link WritableChunk} of at least the specified {@code capacity}. The result belongs
     * to the caller until released.
     *
     * @param capacity The minimum capacity for the result
     * @return A {@link WritableChunk} of at least the specified {@code capacity} that belongs to
     *         the caller until released
     */
    <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(int capacity);

    /**
     * Return a {@link WritableChunk} to the pool.
     *
     * @param writableChunk The chunk to give
     */
    <ATTR extends Any> void giveWritableChunk(@NotNull WritableChunk<ATTR> writableChunk);

    /**
     * Take a {@link ResettableReadOnlyChunk}. The result belongs to the caller until released.
     *
     * @return A {@link ResettableReadOnlyChunk} that belongs to the caller until released
     */
    <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk();

    /**
     * Return a {@link ResettableReadOnlyChunk} of to the pool.
     *
     * @param resettableChunk The chunk to give
     */
    <ATTR extends Any> void giveResettableChunk(
        @NotNull ResettableReadOnlyChunk<ATTR> resettableChunk);

    /**
     * Take a {@link ResettableWritableChunk}. The result belongs to the caller until released.
     *
     * @return A {@link ResettableWritableChunk} that belongs to the caller until released
     */
    <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk();

    /**
     * Return a {@link ResettableWritableChunk} of to the pool.
     *
     * @param resettableWritableChunk The chunk to give
     */
    <ATTR extends Any> void giveResettableWritableChunk(
        @NotNull ResettableWritableChunk<ATTR> resettableWritableChunk);
}
