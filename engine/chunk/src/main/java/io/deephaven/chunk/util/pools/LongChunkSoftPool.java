//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkSoftPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ResettableLongChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableLongChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link LongChunkPool} implementation that pools chunks of longs in a data structure that only enforces soft
 * reachability.
 */
public final class LongChunkSoftPool implements LongChunkPool {

    private static final WritableLongChunk<Any> EMPTY =
            WritableLongChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_LONG_ARRAY);

    /**
     * Subpools by power-of-two sizes for {@link WritableLongChunk WritableLongChunks}.
     */
    private final SegmentedSoftPool<WritableLongChunk<Any>>[] writableLongChunks;

    /**
     * Subpool of {@link ResettableLongChunk ResettableLongChunks}.
     */
    private final SegmentedSoftPool<ResettableLongChunk<Any>> resettableLongChunks;

    /**
     * Subpool of {@link ResettableWritableLongChunk ResettableWritableLongChunks}.
     */
    private final SegmentedSoftPool<ResettableWritableLongChunk<Any>> resettableWritableLongChunks;

    LongChunkSoftPool() {
        // noinspection unchecked
        writableLongChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableLongChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableLongChunk<Any>(LongChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableLongChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableLongChunk<Any> chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableLongChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableLongChunk<Any>() {
                    @Override
                    public void close() {
                        resettableLongChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableLongChunk::clear);
        resettableWritableLongChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableLongChunk<Any>() {
                    @Override
                    public void close() {
                        resettableWritableLongChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableWritableLongChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableLongChunk(capacity);
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableLongChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableLongChunk();
            }
        };
    }

    @Override
    public <ATTR extends Any> WritableLongChunk<ATTR> takeWritableLongChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableLongChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource,unchecked
            final WritableLongChunk<ATTR> result =
                    (WritableLongChunk<ATTR>) writableLongChunks[poolIndexForTake].take();
            result.setSize(capacity);
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableLongChunk<>(LongChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <ATTR extends Any> ResettableLongChunk<ATTR> takeResettableLongChunk() {
        // noinspection unchecked
        return (ResettableLongChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableLongChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableLongChunk<ATTR> takeResettableWritableLongChunk() {
        // noinspection unchecked
        return (ResettableWritableLongChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableWritableLongChunks.take());
    }
}
