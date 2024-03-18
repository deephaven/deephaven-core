//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.util.datastructures.SegmentedSoftPool;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link ChunkPool} implementation for chunks of chars.
 */
@SuppressWarnings("rawtypes")
public final class CharChunkSoftPool implements CharChunkPool {

    private final WritableCharChunk<Any> EMPTY = WritableCharChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_CHAR_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableCharChunk}s.
     */
    private final SegmentedSoftPool<WritableCharChunk>[] writableCharChunks;

    /**
     * Sub-pool of {@link ResettableCharChunk}s.
     */
    private final SegmentedSoftPool<ResettableCharChunk> resettableCharChunks;

    /**
     * Sub-pool of {@link ResettableWritableCharChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableCharChunk> resettableWritableCharChunks;

    CharChunkSoftPool() {
        // noinspection unchecked
        writableCharChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int chunkLog2Capacity = pcci + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableCharChunks[pcci] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation
                            .getAndRecord(() -> WritableCharChunk.makeWritableChunkForPool(chunkCapacity)),
                    (final WritableCharChunk chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableCharChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableCharChunk::makeResettableChunkForPool),
                ResettableCharChunk::clear);
        resettableWritableCharChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableWritableCharChunk::makeResettableChunkForPool),
                ResettableWritableCharChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableCharChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableCharChunk(writableChunk.asWritableCharChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableCharChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(
                    @NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableCharChunk(resettableChunk.asResettableCharChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableCharChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(
                    @NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableCharChunk(resettableWritableChunk.asResettableWritableCharChunk());
            }
        };
    }

    @Override
    public <ATTR extends Any> WritableCharChunk<ATTR> takeWritableCharChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableCharChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource
            final WritableCharChunk result = writableCharChunks[poolIndexForTake].take();
            result.setSize(capacity);
            // noinspection unchecked
            return ChunkPoolReleaseTracking.onTake(result);
        }
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(WritableCharChunk.makeWritableChunkForPool(capacity));
    }

    @Override
    public void giveWritableCharChunk(@NotNull final WritableCharChunk<?> writableCharChunk) {
        if (writableCharChunk == EMPTY || writableCharChunk.isAlias(EMPTY)) {
            return;
        }
        ChunkPoolReleaseTracking.onGive(writableCharChunk);
        final int capacity = writableCharChunk.capacity();
        final int poolIndexForGive = getPoolIndexForGive(checkCapacityBounds(capacity));
        if (poolIndexForGive >= 0) {
            writableCharChunks[poolIndexForGive].give(writableCharChunk);
        }
    }

    @Override
    public <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableCharChunks.take());
    }

    @Override
    public void giveResettableCharChunk(@NotNull final ResettableCharChunk resettableCharChunk) {
        resettableCharChunks.give(ChunkPoolReleaseTracking.onGive(resettableCharChunk));
    }

    @Override
    public <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableCharChunks.take());
    }

    @Override
    public void giveResettableWritableCharChunk(
            @NotNull final ResettableWritableCharChunk resettableWritableCharChunk) {
        resettableWritableCharChunks.give(ChunkPoolReleaseTracking.onGive(resettableWritableCharChunk));
    }
}
