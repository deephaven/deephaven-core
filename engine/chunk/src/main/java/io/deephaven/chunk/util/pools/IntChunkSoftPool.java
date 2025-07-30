//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkSoftPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ResettableIntChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableIntChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link ChunkPool} implementation for chunks of ints.
 */
@SuppressWarnings("rawtypes")
public final class IntChunkSoftPool implements IntChunkPool {

    private static final WritableIntChunk<Any> EMPTY =
            WritableIntChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_INT_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableIntChunk}s.
     */
    private final SegmentedSoftPool<WritableIntChunk>[] writableIntChunks;

    /**
     * Sub-pool of {@link ResettableIntChunk}s.
     */
    private final SegmentedSoftPool<ResettableIntChunk> resettableIntChunks;

    /**
     * Sub-pool of {@link ResettableWritableIntChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableIntChunk> resettableWritableIntChunks;

    IntChunkSoftPool() {
        // noinspection unchecked
        writableIntChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableIntChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableIntChunk(IntChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableIntChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableIntChunk chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableIntChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableIntChunk() {
                    @Override
                    public void close() {
                        resettableIntChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableIntChunk::clear);
        resettableWritableIntChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableIntChunk() {
                    @Override
                    public void close() {
                        resettableWritableIntChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableWritableIntChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableIntChunk(capacity);
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableIntChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableIntChunk();
            }
        };
    }

    @Override
    public <ATTR extends Any> WritableIntChunk<ATTR> takeWritableIntChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableIntChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource
            final WritableIntChunk result = writableIntChunks[poolIndexForTake].take();
            result.setSize(capacity);
            // noinspection unchecked
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableIntChunk<>(IntChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <ATTR extends Any> ResettableIntChunk<ATTR> takeResettableIntChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableIntChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableIntChunk<ATTR> takeResettableWritableIntChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableIntChunks.take());
    }
}
