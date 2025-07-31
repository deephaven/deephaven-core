//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ResettableObjectChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link ObjectChunkPool} implementation that pools chunks of Objects in a data structure that only enforces soft
 * reachability.
 */
public final class ObjectChunkSoftPool implements ObjectChunkPool {

    private final WritableObjectChunk<?, Any> EMPTY =
            WritableObjectChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_OBJECT_ARRAY);

    /**
     * Subpools by power-of-two sizes for {@link WritableObjectChunk WritableObjectChunks}.
     */
    private final SegmentedSoftPool<WritableObjectChunk<?, Any>>[] writableObjectChunks;

    /**
     * Subpool of {@link ResettableObjectChunk ResettableObjectChunks}.
     */
    private final SegmentedSoftPool<ResettableObjectChunk<?, Any>> resettableObjectChunks;

    /**
     * Subpool of {@link ResettableWritableObjectChunk ResettableWritableObjectChunks}.
     */
    private final SegmentedSoftPool<ResettableWritableObjectChunk<?, Any>> resettableWritableObjectChunks;

    ObjectChunkSoftPool() {
        // noinspection unchecked
        writableObjectChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableObjectChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableObjectChunk<Object, Any>(ObjectChunk.makeArray(chunkCapacity), 0,
                                    chunkCapacity) {
                                @Override
                                public void close() {
                                    writableObjectChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableObjectChunk<?, Any> chunk) -> {
                        chunk.fillWithNullValue(0, chunkCapacity);
                        chunk.setSize(chunkCapacity);
                    });
        }
        resettableObjectChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableObjectChunk<Object, Any>() {
                    @Override
                    public void close() {
                        resettableObjectChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableObjectChunk::clear);
        resettableWritableObjectChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableObjectChunk<Object, Any>() {
                    @Override
                    public void close() {
                        resettableWritableObjectChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableWritableObjectChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableObjectChunk(capacity);
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableObjectChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableObjectChunk();
            }
        };
    }

    @Override
    public <TYPE, ATTR extends Any> WritableObjectChunk<TYPE, ATTR> takeWritableObjectChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableObjectChunk<TYPE, ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource,unchecked
            final WritableObjectChunk<TYPE, ATTR> result =
                    (WritableObjectChunk<TYPE, ATTR>) writableObjectChunks[poolIndexForTake].take();
            result.setSize(capacity);
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableObjectChunk<>(ObjectChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableObjectChunk<TYPE, ATTR> takeResettableObjectChunk() {
        // noinspection unchecked
        return (ResettableObjectChunk<TYPE, ATTR>) ChunkPoolReleaseTracking.onTake(resettableObjectChunks.take());
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk() {
        // noinspection unchecked
        return (ResettableWritableObjectChunk<TYPE, ATTR>) ChunkPoolReleaseTracking.onTake(
                resettableWritableObjectChunks.take());
    }
}
