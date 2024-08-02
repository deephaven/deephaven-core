//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkSoftPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.util.datastructures.SegmentedSoftPool;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.*;

/**
 * {@link ChunkPool} implementation for chunks of floats.
 */
@SuppressWarnings("rawtypes")
public final class FloatChunkSoftPool implements FloatChunkPool {

    private final WritableFloatChunk<Any> EMPTY = WritableFloatChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_FLOAT_ARRAY);

    /**
     * Sub-pools by power-of-two sizes for {@link WritableFloatChunk}s.
     */
    private final SegmentedSoftPool<WritableFloatChunk>[] writableFloatChunks;

    /**
     * Sub-pool of {@link ResettableFloatChunk}s.
     */
    private final SegmentedSoftPool<ResettableFloatChunk> resettableFloatChunks;

    /**
     * Sub-pool of {@link ResettableWritableFloatChunk}s.
     */
    private final SegmentedSoftPool<ResettableWritableFloatChunk> resettableWritableFloatChunks;

    FloatChunkSoftPool() {
        // noinspection unchecked
        writableFloatChunks = new SegmentedSoftPool[NUM_POOLED_CHUNK_CAPACITIES];
        for (int pcci = 0; pcci < NUM_POOLED_CHUNK_CAPACITIES; ++pcci) {
            final int chunkLog2Capacity = pcci + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableFloatChunks[pcci] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation
                            .getAndRecord(() -> WritableFloatChunk.makeWritableChunkForPool(chunkCapacity)),
                    (final WritableFloatChunk chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableFloatChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableFloatChunk::makeResettableChunkForPool),
                ResettableFloatChunk::clear);
        resettableWritableFloatChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableWritableFloatChunk::makeResettableChunkForPool),
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
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableFloatChunk(writableChunk.asWritableFloatChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableFloatChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(
                    @NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableFloatChunk(resettableChunk.asResettableFloatChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableFloatChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(
                    @NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableFloatChunk(resettableWritableChunk.asResettableWritableFloatChunk());
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
            // noinspection resource
            final WritableFloatChunk result = writableFloatChunks[poolIndexForTake].take();
            result.setSize(capacity);
            // noinspection unchecked
            return ChunkPoolReleaseTracking.onTake(result);
        }
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(WritableFloatChunk.makeWritableChunkForPool(capacity));
    }

    @Override
    public void giveWritableFloatChunk(@NotNull final WritableFloatChunk<?> writableFloatChunk) {
        if (writableFloatChunk == EMPTY || writableFloatChunk.isAlias(EMPTY)) {
            return;
        }
        ChunkPoolReleaseTracking.onGive(writableFloatChunk);
        final int capacity = writableFloatChunk.capacity();
        final int poolIndexForGive = getPoolIndexForGive(checkCapacityBounds(capacity));
        if (poolIndexForGive >= 0) {
            writableFloatChunks[poolIndexForGive].give(writableFloatChunk);
        }
    }

    @Override
    public <ATTR extends Any> ResettableFloatChunk<ATTR> takeResettableFloatChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableFloatChunks.take());
    }

    @Override
    public void giveResettableFloatChunk(@NotNull final ResettableFloatChunk resettableFloatChunk) {
        resettableFloatChunks.give(ChunkPoolReleaseTracking.onGive(resettableFloatChunk));
    }

    @Override
    public <ATTR extends Any> ResettableWritableFloatChunk<ATTR> takeResettableWritableFloatChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableFloatChunks.take());
    }

    @Override
    public void giveResettableWritableFloatChunk(
            @NotNull final ResettableWritableFloatChunk resettableWritableFloatChunk) {
        resettableWritableFloatChunks.give(ChunkPoolReleaseTracking.onGive(resettableWritableFloatChunk));
    }
}
