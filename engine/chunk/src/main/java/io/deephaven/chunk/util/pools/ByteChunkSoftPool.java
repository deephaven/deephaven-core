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
 * {@link ChunkPool} implementation for chunks of bytes.
 */
@SuppressWarnings("rawtypes")
public final class ByteChunkSoftPool implements ByteChunkPool {

    private final WritableByteChunk<Any> EMPTY = WritableByteChunk.writableChunkWrap(ArrayTypeUtils.EMPTY_BYTE_ARRAY);

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
            final int chunkLog2Capacity = pcci + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
            final int chunkCapacity = 1 << chunkLog2Capacity;
            writableByteChunks[pcci] = new SegmentedSoftPool<>(
                    SUB_POOL_SEGMENT_CAPACITY,
                    () -> ChunkPoolInstrumentation
                            .getAndRecord(() -> WritableByteChunk.makeWritableChunkForPool(chunkCapacity)),
                    (final WritableByteChunk chunk) -> chunk.setSize(chunkCapacity));
        }
        resettableByteChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableByteChunk::makeResettableChunkForPool),
                ResettableByteChunk::clear);
        resettableWritableByteChunks = new SegmentedSoftPool<>(
                SUB_POOL_SEGMENT_CAPACITY,
                () -> ChunkPoolInstrumentation.getAndRecord(ResettableWritableByteChunk::makeResettableChunkForPool),
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
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableByteChunk(writableChunk.asWritableByteChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableByteChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(
                    @NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableByteChunk(resettableChunk.asResettableByteChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableByteChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(
                    @NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableByteChunk(resettableWritableChunk.asResettableWritableByteChunk());
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
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(WritableByteChunk.makeWritableChunkForPool(capacity));
    }

    @Override
    public void giveWritableByteChunk(@NotNull final WritableByteChunk<?> writableByteChunk) {
        if (writableByteChunk == EMPTY || writableByteChunk.isAlias(EMPTY)) {
            return;
        }
        ChunkPoolReleaseTracking.onGive(writableByteChunk);
        final int capacity = writableByteChunk.capacity();
        final int poolIndexForGive = getPoolIndexForGive(checkCapacityBounds(capacity));
        if (poolIndexForGive >= 0) {
            writableByteChunks[poolIndexForGive].give(writableByteChunk);
        }
    }

    @Override
    public <ATTR extends Any> ResettableByteChunk<ATTR> takeResettableByteChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableByteChunks.take());
    }

    @Override
    public void giveResettableByteChunk(@NotNull final ResettableByteChunk resettableByteChunk) {
        resettableByteChunks.give(ChunkPoolReleaseTracking.onGive(resettableByteChunk));
    }

    @Override
    public <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk() {
        // noinspection unchecked
        return ChunkPoolReleaseTracking.onTake(resettableWritableByteChunks.take());
    }

    @Override
    public void giveResettableWritableByteChunk(
            @NotNull final ResettableWritableByteChunk resettableWritableByteChunk) {
        resettableWritableByteChunks.give(ChunkPoolReleaseTracking.onGive(resettableWritableByteChunk));
    }
}
