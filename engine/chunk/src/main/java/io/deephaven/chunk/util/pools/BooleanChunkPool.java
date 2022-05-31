/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPool and regenerate
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
 * {@link ChunkPool} implementation for chunks of booleans.
 */
@SuppressWarnings("rawtypes")
public final class BooleanChunkPool implements ChunkPool {

    private final WritableBooleanChunk<Any> EMPTY = WritableBooleanChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_BOOLEAN_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableBooleanChunk}s.
     */
    private final SegmentedSoftPool<WritableBooleanChunk>[] writableBooleanChunks;

    /**
     * Sub-pool of {@link ResettableBooleanChunk}s.
     */
    private final SegmentedSoftPool<ResettableBooleanChunk> resettableBooleanChunks;

    /**
     * Sub-pool of {@link ResettableWritableBooleanChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableBooleanChunk> resettableWritableBooleanChunks;

    BooleanChunkPool() {
        //noinspection unchecked
        writableBooleanChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int chunkLog2Capacity = pcci + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableBooleanChunks[pcci] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(() -> WritableBooleanChunk.makeWritableChunkForPool(chunkCapacity)),
                    (final WritableBooleanChunk chunk) -> chunk.setSize(chunkCapacity)
            );
        }
        resettableBooleanChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableBooleanChunk::makeResettableChunkForPool),
                ResettableBooleanChunk::clear
        );
        resettableWritableBooleanChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableWritableBooleanChunk::makeResettableChunkForPool),
                ResettableWritableBooleanChunk::clear
        );
    }

    @Override
    public final <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
        return takeWritableBooleanChunk(capacity);
    }

    @Override
    public final <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
        giveWritableBooleanChunk(writableChunk.asWritableBooleanChunk());
    }

    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
        return takeResettableBooleanChunk();
    }

    @Override
    public final <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
        giveResettableBooleanChunk(resettableChunk.asResettableBooleanChunk());
    }

    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
        return takeResettableWritableBooleanChunk();
    }

    @Override
    public final <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
        giveResettableWritableBooleanChunk(resettableWritableChunk.asResettableWritableBooleanChunk());
    }

    public final <ATTR extends Any> WritableBooleanChunk<ATTR> takeWritableBooleanChunk(final int capacity) {
        if (capacity == 0) {
            //noinspection unchecked
            return (WritableBooleanChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            final WritableBooleanChunk result = writableBooleanChunks[poolIndexForTake].take();
            result.setSize(capacity);
            //noinspection unchecked
            return ChunkPoolReleaseTracking.onTake(result);
        }
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(WritableBooleanChunk.makeWritableChunkForPool(capacity));
    }

    public final void giveWritableBooleanChunk(@NotNull final WritableBooleanChunk writableBooleanChunk) {
        if (writableBooleanChunk == EMPTY || writableBooleanChunk.isAlias(EMPTY)) {
            return;
        }
        ChunkPoolReleaseTracking.onGive(writableBooleanChunk);
        final int capacity = writableBooleanChunk.capacity();
        final int poolIndexForGive = getPoolIndexForGive(checkCapacityBounds(capacity));
        if (poolIndexForGive >= 0) {
            writableBooleanChunks[poolIndexForGive].give(writableBooleanChunk);
        }
    }

    public final <ATTR extends Any> ResettableBooleanChunk<ATTR> takeResettableBooleanChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableBooleanChunks.take());
    }

    public final void giveResettableBooleanChunk(@NotNull final ResettableBooleanChunk resettableBooleanChunk) {
        resettableBooleanChunks.give(ChunkPoolReleaseTracking.onGive(resettableBooleanChunk));
    }

    public final <ATTR extends Any> ResettableWritableBooleanChunk<ATTR> takeResettableWritableBooleanChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableBooleanChunks.take());
    }

    public final void giveResettableWritableBooleanChunk(@NotNull final ResettableWritableBooleanChunk resettableWritableBooleanChunk) {
        resettableWritableBooleanChunks.give(ChunkPoolReleaseTracking.onGive(resettableWritableBooleanChunk));
    }
}
