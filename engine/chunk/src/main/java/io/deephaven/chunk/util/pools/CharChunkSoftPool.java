//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.ResettableCharChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableCharChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link CharChunkPool} implementation that pools chunks of chars in a data structure that only enforces soft
 * reachability.
 */
public final class CharChunkSoftPool implements CharChunkPool {

    private static final WritableCharChunk<Any> EMPTY =
            WritableCharChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_CHAR_ARRAY);

    /**
     * Subpools by power-of-two sizes for {@link WritableCharChunk WritableCharChunks}.
     */
    private final SegmentedSoftPool<WritableCharChunk<Any>>[] writableCharChunks;

    /**
     * Subpool of {@link ResettableCharChunk ResettableCharChunks}.
     */
    private final SegmentedSoftPool<ResettableCharChunk<Any>> resettableCharChunks;

    /**
     * Subpool of {@link ResettableWritableCharChunk ResettableWritableCharChunks}.
     */
    private final SegmentedSoftPool<ResettableWritableCharChunk<Any>> resettableWritableCharChunks;

    CharChunkSoftPool() {
        // noinspection unchecked
        writableCharChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableCharChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableCharChunk<Any>(CharChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableCharChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableCharChunk<Any> chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableCharChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableCharChunk<Any>() {
                    @Override
                    public void close() {
                        resettableCharChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableCharChunk::clear);
        resettableWritableCharChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableCharChunk<Any>() {
                    @Override
                    public void close() {
                        resettableWritableCharChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
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
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableCharChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableCharChunk();
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
            // noinspection resource,unchecked
            final WritableCharChunk<ATTR> result =
                    (WritableCharChunk<ATTR>) writableCharChunks[poolIndexForTake].take();
            result.setSize(capacity);
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableCharChunk<>(CharChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk() {
        // noinspection unchecked
        return (ResettableCharChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableCharChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk() {
        // noinspection unchecked
        return (ResettableWritableCharChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableWritableCharChunks.take());
    }
}
