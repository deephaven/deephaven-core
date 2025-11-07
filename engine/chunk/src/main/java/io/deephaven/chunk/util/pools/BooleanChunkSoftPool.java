//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkSoftPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.BooleanChunk;
import io.deephaven.chunk.ResettableBooleanChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableBooleanChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link BooleanChunkPool} implementation that pools chunks of booleans in a data structure that only enforces soft
 * reachability.
 */
public final class BooleanChunkSoftPool implements BooleanChunkPool {

    private static final WritableBooleanChunk<Any> EMPTY =
            WritableBooleanChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_BOOLEAN_ARRAY);

    /**
     * Subpools by power-of-two sizes for {@link WritableBooleanChunk WritableBooleanChunks}.
     */
    private final SegmentedSoftPool<WritableBooleanChunk<Any>>[] writableBooleanChunks;

    /**
     * Subpool of {@link ResettableBooleanChunk ResettableBooleanChunks}.
     */
    private final SegmentedSoftPool<ResettableBooleanChunk<Any>> resettableBooleanChunks;

    /**
     * Subpool of {@link ResettableWritableBooleanChunk ResettableWritableBooleanChunks}.
     */
    private final SegmentedSoftPool<ResettableWritableBooleanChunk<Any>> resettableWritableBooleanChunks;

    BooleanChunkSoftPool() {
        // noinspection unchecked
        writableBooleanChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableBooleanChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableBooleanChunk<Any>(BooleanChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableBooleanChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableBooleanChunk<Any> chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableBooleanChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableBooleanChunk<Any>() {
                    @Override
                    public void close() {
                        resettableBooleanChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableBooleanChunk::clear);
        resettableWritableBooleanChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableBooleanChunk<Any>() {
                    @Override
                    public void close() {
                        resettableWritableBooleanChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableWritableBooleanChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableBooleanChunk(capacity);
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableBooleanChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableBooleanChunk();
            }
        };
    }

    @Override
    public <ATTR extends Any> WritableBooleanChunk<ATTR> takeWritableBooleanChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableBooleanChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource,unchecked
            final WritableBooleanChunk<ATTR> result =
                    (WritableBooleanChunk<ATTR>) writableBooleanChunks[poolIndexForTake].take();
            result.setSize(capacity);
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableBooleanChunk<>(BooleanChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <ATTR extends Any> ResettableBooleanChunk<ATTR> takeResettableBooleanChunk() {
        // noinspection unchecked
        return (ResettableBooleanChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableBooleanChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableBooleanChunk<ATTR> takeResettableWritableBooleanChunk() {
        // noinspection unchecked
        return (ResettableWritableBooleanChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableWritableBooleanChunks.take());
    }
}
