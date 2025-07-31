//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkSoftPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.ResettableShortChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableShortChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.SegmentedSoftPool;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link ChunkPool} implementation for chunks of shorts.
 */
@SuppressWarnings("rawtypes")
public final class ShortChunkSoftPool implements ShortChunkPool {

    private static final WritableShortChunk<Any> EMPTY =
            WritableShortChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_SHORT_ARRAY);

    /**
     * Subpools by power-of-two sizes for {@link WritableShortChunk WritableShortChunks}.
     */
    private final SegmentedSoftPool<WritableShortChunk>[] writableShortChunks;

    /**
     * Subpool of {@link ResettableShortChunk ResettableShortChunks}.
     */
    private final SegmentedSoftPool<ResettableShortChunk> resettableShortChunks;

    /**
     * Subpool of {@link ResettableWritableShortChunk ResettableWritableShortChunks}.
     */
    private final SegmentedSoftPool<ResettableWritableShortChunk> resettableWritableShortChunks;

    ShortChunkSoftPool() {
        // noinspection unchecked
        writableShortChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int poolIndex = pcci;
            final int chunkLog2Capacity = poolIndex + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableShortChunks[poolIndex] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation.getAndRecord(
                            () -> new WritableShortChunk(ShortChunk.makeArray(chunkCapacity), 0, chunkCapacity) {
                                @Override
                                public void close() {
                                    writableShortChunks[poolIndex].give(ChunkPoolReleaseTracking.onGive(this));
                                }
                            }),
                    (final WritableShortChunk chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableShortChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableShortChunk() {
                    @Override
                    public void close() {
                        resettableShortChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableShortChunk::clear);
        resettableWritableShortChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(() -> new ResettableWritableShortChunk() {
                    @Override
                    public void close() {
                        resettableWritableShortChunks.give(ChunkPoolReleaseTracking.onGive(this));
                    }
                }),
                ResettableWritableShortChunk::clear);
    }

    @Override
    public ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableShortChunk(capacity);
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableShortChunk();
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableShortChunk();
            }
        };
    }

    @Override
    public <ATTR extends Any> WritableShortChunk<ATTR> takeWritableShortChunk(final int capacity) {
        if (capacity == 0) {
            // noinspection unchecked
            return (WritableShortChunk<ATTR>) EMPTY;
        }
        final int poolIndexForTake = getPoolIndexForTake(checkCapacityBounds(capacity));
        if (poolIndexForTake >= 0) {
            // noinspection resource,unchecked
            final WritableShortChunk<ATTR> result = writableShortChunks[poolIndexForTake].take();
            result.setSize(capacity);
            return ChunkPoolReleaseTracking.onTake(result);
        }
        return ChunkPoolReleaseTracking.onTake(
                new WritableShortChunk<ATTR>(ShortChunk.makeArray(capacity), 0, capacity) {
                    @Override
                    public void close() {
                        ChunkPoolReleaseTracking.onGive(this);
                    }
                });
    }

    @Override
    public <ATTR extends Any> ResettableShortChunk<ATTR> takeResettableShortChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableShortChunks.take());
    }

    @Override
    public <ATTR extends Any> ResettableWritableShortChunk<ATTR> takeResettableWritableShortChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableShortChunks.take());
    }
}
