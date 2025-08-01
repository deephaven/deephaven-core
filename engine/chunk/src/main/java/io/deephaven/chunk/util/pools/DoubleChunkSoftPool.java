//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkSoftPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.ResettableDoubleChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableDoubleChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link DoubleChunkPool} implementation that pools chunks of doubles in a data structure that only enforces soft
 * reachability.
 */
public final class DoubleChunkSoftPool implements DoubleChunkPool {

    private static final WritableDoubleChunk<Any> EMPTY =
            WritableDoubleChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_DOUBLE_ARRAY);

    /**
     * Subpools by power-of-two sizes for {@link WritableDoubleChunk WritableDoubleChunks}.
     */
    private final SegmentedSoftPool<WritableDoubleChunk<Any>>[] writableDoubleChunks;

    /**
     * Subpool of {@link ResettableDoubleChunk ResettableDoubleChunks}.
     */
    private final SegmentedSoftPool<ResettableDoubleChunk<Any>> resettableDoubleChunks;

    /**
     * Subpool of {@link ResettableWritableDoubleChunk ResettableWritableDoubleChunks}.
     */
    private final SegmentedSoftPool<ResettableWritableDoubleChunk<Any>> resettableWritableDoubleChunks;

    DoubleChunkSoftPool() {
        // noinspection unchecked
        writableDoubleChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableDoubleChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableDoubleChunk<Any>(DoubleChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableDoubleChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableDoubleChunk<Any> chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableDoubleChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableDoubleChunk<Any>() {
                    @Override
                    public void close() {
                        resettableDoubleChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableDoubleChunk::clear);
        resettableWritableDoubleChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableDoubleChunk<Any>() {
                    @Override
                    public void close() {
                        resettableWritableDoubleChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableWritableDoubleChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableDoubleChunk(capacity);
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableDoubleChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableDoubleChunk();
            }
        };
    }

    @Override
    public <ATTR extends Any> WritableDoubleChunk<ATTR> takeWritableDoubleChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableDoubleChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource,unchecked
            final WritableDoubleChunk<ATTR> result =
                    (WritableDoubleChunk<ATTR>) writableDoubleChunks[poolIndexForTake].take();
            result.setSize(capacity);
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableDoubleChunk<>(DoubleChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <ATTR extends Any> ResettableDoubleChunk<ATTR> takeResettableDoubleChunk() {
        // noinspection unchecked
        return (ResettableDoubleChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableDoubleChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk() {
        // noinspection unchecked
        return (ResettableWritableDoubleChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableWritableDoubleChunks.take());
    }
}
