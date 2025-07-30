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
 * {@link ChunkPool} implementation for chunks of objects.
 */
@SuppressWarnings("rawtypes")
public final class ObjectChunkSoftPool implements ObjectChunkPool {

    private final WritableObjectChunk<?, Any> EMPTY =
            WritableObjectChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_OBJECT_ARRAY);

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

    ObjectChunkSoftPool() {
        // noinspection unchecked
        writableObjectChunks =
                (SegmentedSoftPool<WritableObjectChunk>[]) new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableObjectChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableObjectChunk<>(ObjectChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableObjectChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableObjectChunk chunk) -> {
                        chunk.fillWithNullValue(0, chunkCapacity);
                        chunk.setSize(chunkCapacity);
                    });
        }
        resettableObjectChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableObjectChunk() {
                    @Override
                    public void close() {
                        resettableObjectChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableObjectChunk::clear);
        resettableWritableObjectChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableObjectChunk() {
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
            // noinspection resource
            final WritableObjectChunk result = writableObjectChunks[poolIndexForTake].take();
            result.setSize(capacity);
            // noinspection unchecked
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
        return ChunkPoolReleaseTracking.onTake(resettableObjectChunks.take());
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableObjectChunks.take());
    }
}
