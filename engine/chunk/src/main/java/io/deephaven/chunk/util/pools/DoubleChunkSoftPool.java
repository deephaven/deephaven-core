/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkSoftPool and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.util.datastructures.SegmentedSoftPool;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link ChunkPool} implementation for chunks of doubles.
 */
@SuppressWarnings("rawtypes")
public final class DoubleChunkSoftPool implements DoubleChunkPool {

    private final WritableDoubleChunk<Any> EMPTY = WritableDoubleChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_DOUBLE_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableDoubleChunk}s.
     */
    private final SegmentedSoftPool<WritableDoubleChunk>[] writableDoubleChunks;

    /**
     * Sub-pool of {@link ResettableDoubleChunk}s.
     */
    private final SegmentedSoftPool<ResettableDoubleChunk> resettableDoubleChunks;

    /**
     * Sub-pool of {@link ResettableWritableDoubleChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableDoubleChunk> resettableWritableDoubleChunks;

    DoubleChunkSoftPool() {
        //noinspection unchecked
        writableDoubleChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int chunkLog2Capacity = pcci + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableDoubleChunks[pcci] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(() -> WritableDoubleChunk.makeWritableChunkForPool(chunkCapacity)),
                    (final WritableDoubleChunk chunk) -> chunk.setSize(chunkCapacity)
            );
        }
        resettableDoubleChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableDoubleChunk::makeResettableChunkForPool),
                ResettableDoubleChunk::clear
        );
        resettableWritableDoubleChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableWritableDoubleChunk::makeResettableChunkForPool),
                ResettableWritableDoubleChunk::clear
        );
    }

    @Override
    public <ATTR extends Any> WritableDoubleChunk<ATTR> takeWritableDoubleChunk(final int capacity) {
        if (capacity == 0) {
            //noinspection unchecked
            return (WritableDoubleChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            //noinspection resource
            final WritableDoubleChunk result = writableDoubleChunks[poolIndexForTake].take();
            result.setSize(capacity);
            //noinspection unchecked
            return ChunkPoolReleaseTracking.onTake(result);
        }
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(WritableDoubleChunk.makeWritableChunkForPool(capacity));
    }

    @Override
    public void giveWritableDoubleChunk(@NotNull final WritableDoubleChunk<?> writableDoubleChunk) {
        if (writableDoubleChunk == EMPTY || writableDoubleChunk.isAlias(EMPTY)) {
            return;
        }
        ChunkPoolReleaseTracking.onGive(writableDoubleChunk);
        final int capacity = writableDoubleChunk.capacity();
        final int poolIndexForGive = getPoolIndexForGive(checkCapacityBounds(capacity));
        if (poolIndexForGive >= 0) {
            writableDoubleChunks[poolIndexForGive].give(writableDoubleChunk);
        }
    }

    @Override
    public <ATTR extends Any> ResettableDoubleChunk<ATTR> takeResettableDoubleChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableDoubleChunks.take());
    }

    @Override
    public void giveResettableDoubleChunk(@NotNull final ResettableDoubleChunk resettableDoubleChunk) {
        resettableDoubleChunks.give(ChunkPoolReleaseTracking.onGive(resettableDoubleChunk));
    }

    @Override
    public <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableDoubleChunks.take());
    }

    @Override
    public void giveResettableWritableDoubleChunk(@NotNull final ResettableWritableDoubleChunk resettableWritableDoubleChunk) {
        resettableWritableDoubleChunks.give(ChunkPoolReleaseTracking.onGive(resettableWritableDoubleChunk));
    }
}
