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
public final class CharChunkPool implements ChunkPool {

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

    CharChunkPool() {
        //noinspection unchecked
        writableCharChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int chunkLog2Capacity = pcci + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableCharChunks[pcci] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(() -> WritableCharChunk.makeWritableChunkForPool(chunkCapacity)),
                    (final WritableCharChunk chunk) -> chunk.setSize(chunkCapacity)
            );
        }
        resettableCharChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableCharChunk::makeResettableChunkForPool),
                ResettableCharChunk::clear
        );
        resettableWritableCharChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableWritableCharChunk::makeResettableChunkForPool),
                ResettableWritableCharChunk::clear
        );
    }

    @Override
    public final <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
        return takeWritableCharChunk(capacity);
    }

    @Override
    public final <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
        giveWritableCharChunk(writableChunk.asWritableCharChunk());
    }

    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
        return takeResettableCharChunk();
    }

    @Override
    public final <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
        giveResettableCharChunk(resettableChunk.asResettableCharChunk());
    }

    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
        return takeResettableWritableCharChunk();
    }

    @Override
    public final <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
        giveResettableWritableCharChunk(resettableWritableChunk.asResettableWritableCharChunk());
    }

    public final <ATTR extends Any> WritableCharChunk<ATTR> takeWritableCharChunk(final int capacity) {
        if (capacity == 0) {
            //noinspection unchecked
            return (WritableCharChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            final WritableCharChunk result = writableCharChunks[poolIndexForTake].take();
            result.setSize(capacity);
            //noinspection unchecked
            return ChunkPoolReleaseTracking.onTake(result);
        }
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(WritableCharChunk.makeWritableChunkForPool(capacity));
    }

    public final void giveWritableCharChunk(@NotNull final WritableCharChunk writableCharChunk) {
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

    public final <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableCharChunks.take());
    }

    public final void giveResettableCharChunk(@NotNull final ResettableCharChunk resettableCharChunk) {
        resettableCharChunks.give(ChunkPoolReleaseTracking.onGive(resettableCharChunk));
    }

    public final <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableCharChunks.take());
    }

    public final void giveResettableWritableCharChunk(@NotNull final ResettableWritableCharChunk resettableWritableCharChunk) {
        resettableWritableCharChunks.give(ChunkPoolReleaseTracking.onGive(resettableWritableCharChunk));
    }
}
