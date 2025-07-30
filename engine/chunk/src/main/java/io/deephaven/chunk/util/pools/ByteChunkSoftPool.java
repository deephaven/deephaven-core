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
 * {@link ChunkPool} implementation for chunks of bytes.
 */
@SuppressWarnings("rawtypes")
public final class ByteChunkSoftPool implements ByteChunkPool {

    private static final WritableByteChunk<Any> EMPTY =
            WritableByteChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_BYTE_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableByteChunk}s.
     */
    private final SegmentedSoftPool<WritableByteChunk>[] writableByteChunks;

    /**
     * Sub-pool of {@link ResettableByteChunk}s.
     */
    private final SegmentedSoftPool<ResettableByteChunk> resettableByteChunks;

    /**
     * Sub-pool of {@link ResettableWritableByteChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableByteChunk> resettableWritableByteChunks;

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
                            () -> new WritableByteChunk(ByteChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableByteChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableByteChunk chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableByteChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableByteChunk() {
                    @Override
                    public void close() {
                        resettableByteChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableByteChunk::clear);
        resettableWritableByteChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableByteChunk() {
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
            // noinspection resource
            final WritableByteChunk result = writableByteChunks[poolIndexForTake].take();
            result.setSize(capacity);
            // noinspection unchecked
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
        return ChunkPoolReleaseTracking.onTake(resettableByteChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableByteChunks.take());
    }
}
