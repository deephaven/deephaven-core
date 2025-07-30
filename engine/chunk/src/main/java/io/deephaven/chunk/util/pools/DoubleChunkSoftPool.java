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
 * {@link ChunkPool} implementation for chunks of doubles.
 */
@SuppressWarnings("rawtypes")
public final class DoubleChunkSoftPool implements DoubleChunkPool {

    private static final WritableDoubleChunk<Any> EMPTY =
            WritableDoubleChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_DOUBLE_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableDoubleChunk}s.
     */
    private final SegmentedSoftPool<WritableDoubleChunk>[] writableDoubleChunks;

    /**
     * Sub-pool of {@link ResettableDoubleChunk}s.
     */
    private final SegmentedSoftPool<ResettableDoubleChunk> resettableDoubleChunks;

    /**
     * Sub-pool of {@link ResettableWritableDoubleChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableDoubleChunk> resettableWritableDoubleChunks;

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
                            () -> new WritableDoubleChunk(DoubleChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableDoubleChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableDoubleChunk chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableDoubleChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableDoubleChunk() {
                    @Override
                    public void close() {
                        resettableDoubleChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableDoubleChunk::clear);
        resettableWritableDoubleChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableDoubleChunk() {
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
            // noinspection resource
            final WritableDoubleChunk result = writableDoubleChunks[poolIndexForTake].take();
            result.setSize(capacity);
            // noinspection unchecked
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
        return ChunkPoolReleaseTracking.onTake(resettableDoubleChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableDoubleChunks.take());
    }
}
