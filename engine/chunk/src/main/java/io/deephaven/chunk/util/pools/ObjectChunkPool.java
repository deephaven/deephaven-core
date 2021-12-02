package io.deephaven.chunk.util.pools;

import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.util.datastructures.SegmentedSoftPool;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link ChunkPool} implementation for chunks of objects.
 */
@SuppressWarnings("rawtypes")
public final class ObjectChunkPool implements ChunkPool {

    private final WritableObjectChunk<?, Any> EMPTY = WritableObjectChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_OBJECT_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableObjectChunk}s.
     */
    private final SegmentedSoftPool<WritableObjectChunk>[] writableObjectChunks;

    /**
     * Sub-pool of {@link ResettableObjectChunk}s.
     */
    private final SegmentedSoftPool<ResettableObjectChunk> resettableObjectChunks;

    /**
     * Sub-pool of {@link ResettableWritableObjectChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableObjectChunk> resettableWritableObjectChunks;

    ObjectChunkPool() {
        //noinspection unchecked
        writableObjectChunks = (SegmentedSoftPool<WritableObjectChunk>[]) new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int chunkLog2Capacity = pcci + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableObjectChunks[pcci] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(() -> WritableObjectChunk.makeWritableChunkForPool(chunkCapacity)),
                    (final WritableObjectChunk chunk) -> {
                        chunk.fillWithNullValue(0, chunkCapacity);
                        chunk.setSize(chunkCapacity);
                    }
            );
        }
        resettableObjectChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableObjectChunk::makeResettableChunkForPool),
                ResettableObjectChunk::clear
        );
        resettableWritableObjectChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableWritableObjectChunk::makeResettableChunkForPool),
                ResettableWritableObjectChunk::clear
        );
    }

    @Override
    public final <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
        return takeWritableObjectChunk(capacity);
    }

    @Override
    public final <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
        giveWritableObjectChunk(writableChunk.asWritableObjectChunk());
    }

    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
        return takeResettableObjectChunk();
    }

    @Override
    public final <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
        giveResettableObjectChunk(resettableChunk.asResettableObjectChunk());
    }

    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
        return takeResettableWritableObjectChunk();
    }

    @Override
    public final <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
        giveResettableWritableObjectChunk(resettableWritableChunk.asResettableWritableObjectChunk());
    }

    public final <TYPE, ATTR extends Any> WritableObjectChunk<TYPE, ATTR> takeWritableObjectChunk(final int capacity) {
        if (capacity == 0) {
            //noinspection unchecked
            return (WritableObjectChunk<TYPE, ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            final WritableObjectChunk result = writableObjectChunks[poolIndexForTake].take();
            result.setSize(capacity);
            //noinspection unchecked
            return ChunkPoolReleaseTracking.onTake(result);
        }
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(WritableObjectChunk.makeWritableChunkForPool(capacity));
    }

    public final void giveWritableObjectChunk(@NotNull final WritableObjectChunk writableObjectChunk) {
        if (writableObjectChunk == EMPTY || writableObjectChunk.isAlias(EMPTY)) {
            return;
        }
        ChunkPoolReleaseTracking.onGive(writableObjectChunk);
        final int capacity = writableObjectChunk.capacity();
        final int poolIndexForGive = getPoolIndexForGive(checkCapacityBounds(capacity));
        if (poolIndexForGive >= 0) {
            writableObjectChunks[poolIndexForGive].give(writableObjectChunk);
        }
    }

    public final <TYPE, ATTR extends Any> ResettableObjectChunk<TYPE, ATTR> takeResettableObjectChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableObjectChunks.take());
    }

    public final void giveResettableObjectChunk(@NotNull final ResettableObjectChunk resettableObjectChunk) {
        resettableObjectChunks.give(ChunkPoolReleaseTracking.onGive(resettableObjectChunk));
    }

    public final <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk() {
        //noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableObjectChunks.take());
    }

    public final void giveResettableWritableObjectChunk(@NotNull final ResettableWritableObjectChunk resettableWritableObjectChunk) {
        resettableWritableObjectChunks.give(ChunkPoolReleaseTracking.onGive(resettableWritableObjectChunk));
    }
}
