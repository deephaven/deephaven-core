//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkSoftPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.ResettableFloatChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableFloatChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link FloatChunkPool} implementation that pools chunks of floats in a data structure that only enforces soft
 * reachability.
 */
public final class FloatChunkSoftPool implements FloatChunkPool {

    private static final WritableFloatChunk<Any> EMPTY =
            WritableFloatChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_FLOAT_ARRAY);

    /**
     * Subpools by power-of-two sizes for {@link WritableFloatChunk WritableFloatChunks}.
     */
    private final SegmentedSoftPool<WritableFloatChunk<Any>>[] writableFloatChunks;

    /**
     * Subpool of {@link ResettableFloatChunk ResettableFloatChunks}.
     */
    private final SegmentedSoftPool<ResettableFloatChunk<Any>> resettableFloatChunks;

    /**
     * Subpool of {@link ResettableWritableFloatChunk ResettableWritableFloatChunks}.
     */
    private final SegmentedSoftPool<ResettableWritableFloatChunk<Any>> resettableWritableFloatChunks;

    FloatChunkSoftPool() {
        // noinspection unchecked
        writableFloatChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableFloatChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableFloatChunk<Any>(FloatChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableFloatChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableFloatChunk<Any> chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableFloatChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableFloatChunk<Any>() {
                    @Override
                    public void close() {
                        resettableFloatChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableFloatChunk::clear);
        resettableWritableFloatChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableFloatChunk<Any>() {
                    @Override
                    public void close() {
                        resettableWritableFloatChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableWritableFloatChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableFloatChunk(capacity);
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableFloatChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableFloatChunk();
            }
        };
    }

    @Override
    public <ATTR extends Any> WritableFloatChunk<ATTR> takeWritableFloatChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableFloatChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource,unchecked
            final WritableFloatChunk<ATTR> result =
                    (WritableFloatChunk<ATTR>) writableFloatChunks[poolIndexForTake].take();
            result.setSize(capacity);
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableFloatChunk<>(FloatChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <ATTR extends Any> ResettableFloatChunk<ATTR> takeResettableFloatChunk() {
        // noinspection unchecked
        return (ResettableFloatChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableFloatChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableFloatChunk<ATTR> takeResettableWritableFloatChunk() {
        // noinspection unchecked
        return (ResettableWritableFloatChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableWritableFloatChunks.take());
    }
}
