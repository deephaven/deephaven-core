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
 * {@link ChunkPool} implementation for chunks of shorts.
 */
@SuppressWarnings("rawtypes")
public final class ShortChunkPool implements ChunkPool {

    private final WritableShortChunk<Any> EMPTY = WritableShortChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_SHORT_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableShortChunk}s.
     */
    private final SegmentedSoftPool<WritableShortChunk>[] writableShortChunks;

    /**
     * Sub-pool of {@link ResettableShortChunk}s.
     */
    private final SegmentedSoftPool<ResettableShortChunk> resettableShortChunks;

    /**
     * Sub-pool of {@link ResettableWritableShortChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableShortChunk> resettableWritableShortChunks;

    ShortChunkPool() {
        //noinspection unchecked
        writableShortChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int chunkLog2Capacity = pcci + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableShortChunks[pcci] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(() -> WritableShortChunk.makeWritableChunkForPool(chunkCapacity)),
                    (final WritableShortChunk chunk) -> chunk.setSize(chunkCapacity)
            );
        }
        resettableShortChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableShortChunk::makeResettableChunkForPool),
                ResettableShortChunk::clear
        );
        resettableWritableShortChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableWritableShortChunk::makeResettableChunkForPool),
                ResettableWritableShortChunk::clear
        );
    }

    @Override
    public final <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
        return takeWritableShortChunk(capacity);
    }

    @Override
    public final <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
        giveWritableShortChunk(writableChunk.asWritableShortChunk());
    }

    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
        return takeResettableShortChunk();
    }

    @Override
    public final <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
        giveResettableShortChunk(resettableChunk.asResettableShortChunk());
    }

    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
        return takeResettableWritableShortChunk();
    }

    @Override
    public final <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
        giveResettableWritableShortChunk(resettableWritableChunk.asResettableWritableShortChunk());
    }

    public final <ATTR extends Any> WritableShortChunk<ATTR> takeWritableShortChunk(final int capacity) {
        if (capacity == 0) {
            //noinspection unchecked
            return (WritableShortChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            final WritableShortChunk result = writableShortChunks[poolIndexForTake].take();
            result.setSize(capacity);
            //noinspection unchecked
            return ChunkPoolReleaseTracking.onTake(result);
        }
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(WritableShortChunk.makeWritableChunkForPool(capacity));
    }

    public final void giveWritableShortChunk(@NotNull final WritableShortChunk writableShortChunk) {
        if (writableShortChunk == EMPTY || writableShortChunk.isAlias(EMPTY)) {
            return;
        }
        ChunkPoolReleaseTracking.onGive(writableShortChunk);
        final int capacity = writableShortChunk.capacity();
        final int poolIndexForGive = getPoolIndexForGive(checkCapacityBounds(capacity));
        if (poolIndexForGive >= 0) {
            writableShortChunks[poolIndexForGive].give(writableShortChunk);
        }
    }

    public final <ATTR extends Any> ResettableShortChunk<ATTR> takeResettableShortChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableShortChunks.take());
    }

    public final void giveResettableShortChunk(@NotNull final ResettableShortChunk resettableShortChunk) {
        resettableShortChunks.give(ChunkPoolReleaseTracking.onGive(resettableShortChunk));
    }

    public final <ATTR extends Any> ResettableWritableShortChunk<ATTR> takeResettableWritableShortChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableShortChunks.take());
    }

    public final void giveResettableWritableShortChunk(@NotNull final ResettableWritableShortChunk resettableWritableShortChunk) {
        resettableWritableShortChunks.give(ChunkPoolReleaseTracking.onGive(resettableWritableShortChunk));
    }
}
