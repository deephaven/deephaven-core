//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkSoftPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.ResettableByteChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableByteChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link ByteChunkPool} implementation that pools chunks of bytes in a data structure that only enforces soft
 * reachability.
 */
public final class ByteChunkSoftPool implements ByteChunkPool {

    private static final WritableByteChunk<Any> EMPTY =
            WritableByteChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_BYTE_ARRAY);

    /**
     * Subpools by power-of-two sizes for {@link WritableByteChunk WritableByteChunks}.
     */
    private final SegmentedSoftPool<WritableByteChunk<Any>>[] writableByteChunks;

    /**
     * Subpool of {@link ResettableByteChunk ResettableByteChunks}.
     */
    private final SegmentedSoftPool<ResettableByteChunk<Any>> resettableByteChunks;

    /**
     * Subpool of {@link ResettableWritableByteChunk ResettableWritableByteChunks}.
     */
    private final SegmentedSoftPool<ResettableWritableByteChunk<Any>> resettableWritableByteChunks;

    ByteChunkSoftPool() {
        // noinspection unchecked
        writableByteChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableByteChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableByteChunk<Any>(ByteChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableByteChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableByteChunk<Any> chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableByteChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableByteChunk<Any>() {
                    @Override
                    public void close() {
                        resettableByteChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableByteChunk::clear);
        resettableWritableByteChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableByteChunk<Any>() {
                    @Override
                    public void close() {
                        resettableWritableByteChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableWritableByteChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableByteChunk(capacity);
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableByteChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableByteChunk();
            }
        };
    }

    @Override
    public <ATTR extends Any> WritableByteChunk<ATTR> takeWritableByteChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableByteChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource,unchecked
            final WritableByteChunk<ATTR> result =
                    (WritableByteChunk<ATTR>) writableByteChunks[poolIndexForTake].take();
            result.setSize(capacity);
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableByteChunk<>(ByteChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <ATTR extends Any> ResettableByteChunk<ATTR> takeResettableByteChunk() {
        // noinspection unchecked
        return (ResettableByteChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableByteChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk() {
        // noinspection unchecked
        return (ResettableWritableByteChunk<ATTR>) ChunkPoolReleaseTracking.onTake(resettableWritableByteChunks.take());
    }
}
