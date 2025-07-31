//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.annotations.VisibleForTesting;

/**
 * Repository for constants used by {@link ChunkPool} implementations.
 */
public final class ChunkPoolConstants {

    /**
     * The log (base 2) of the smallest chunk capacity that will be pooled. Must be in the range [0, 30]. Must be less
     * than or equal to {@link #LARGEST_POOLED_CHUNK_LOG2_CAPACITY}.
     */
    static final int SMALLEST_POOLED_CHUNK_LOG2_CAPACITY = Configuration.getInstance().getIntegerForClassWithDefault(
            ChunkPoolConstants.class, "smallestPooledChunkLog2Capacity", 5);
    /**
     * The smallest chunk capacity that will be pooled.
     */
    public static final int SMALLEST_POOLED_CHUNK_CAPACITY = 1 << SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;

    /**
     * The log (base 2) of the largest chunk capacity that will be pooled. Must be in the range [0, 30]. Must be greater
     * than or equal to {@link #SMALLEST_POOLED_CHUNK_LOG2_CAPACITY}.
     */
    @VisibleForTesting
    static final int LARGEST_POOLED_CHUNK_LOG2_CAPACITY = Configuration.getInstance().getIntegerForClassWithDefault(
            ChunkPoolConstants.class, "largestPooledChunkLog2Capacity", 16);
    /**
     * The largest chunk capacity that will be pooled.
     */
    public static final int LARGEST_POOLED_CHUNK_CAPACITY = 1 << LARGEST_POOLED_CHUNK_LOG2_CAPACITY;

    static {
        if (SMALLEST_POOLED_CHUNK_LOG2_CAPACITY < 0
                || LARGEST_POOLED_CHUNK_LOG2_CAPACITY > 30
                || SMALLEST_POOLED_CHUNK_LOG2_CAPACITY > LARGEST_POOLED_CHUNK_LOG2_CAPACITY) {
            throw new IllegalArgumentException(
                    "Pooled chunk log capacity bounds must uncontradictory and in the range [0, 30], were ["
                            + SMALLEST_POOLED_CHUNK_LOG2_CAPACITY + ", " + LARGEST_POOLED_CHUNK_LOG2_CAPACITY + ']');
        }
    }

    /**
     * The total number of pooled chunk capacities.
     */
    static final int NUM_POOLED_CHUNK_CAPACITIES =
            LARGEST_POOLED_CHUNK_LOG2_CAPACITY - SMALLEST_POOLED_CHUNK_LOG2_CAPACITY + 1;

    /**
     * Whether the chunk pool should pool resettable chunks, i.e., * {@link io.deephaven.chunk.ResettableReadOnlyChunk
     * ResettableReadOnlyChunk} or {@link io.deephaven.chunk.ResettableWritableChunk ResettableWritableChunk} instances.
     */
    public static final boolean POOL_RESETTABLE_CHUNKS = Configuration.getInstance().getBooleanForClassWithDefault(
            ChunkPoolConstants.class, "poolResettableChunks", false);
    /**
     * Whether the chunk pool should pool writable chunks, i.e., {@link io.deephaven.chunk.WritableChunk WritableChunk
     * WritableChunk} instances.
     */
    public static final boolean POOL_WRITABLE_CHUNKS = Configuration.getInstance().getBooleanForClassWithDefault(
            ChunkPoolConstants.class, "poolWritableChunks", true);

    /**
     * The array size used in the subpool segments. This is an implementation detail of the chunk pool that dictates how
     * many chunks should be kept together under a single softly reachable array.
     */
    static final int SUB_POOL_SEGMENT_CAPACITY = Configuration.getInstance().getIntegerForClassWithDefault(
            ChunkPoolConstants.class, "subPoolSegmentCapacity", 10);;

    /**
     * Check that the given chunk capacity is valid.
     *
     * @param chunkCapacity The chunk capacity to check
     * @return The chunk capacity, if it is valid
     */
    static int checkCapacityBounds(final int chunkCapacity) {
        return Require.geqZero(chunkCapacity, "chunkCapacity");
    }

    /**
     * Get the index of the pool to remove a chunk from.
     *
     * @param minimumChunkCapacity A lower bound on the result chunk capacity
     * @return The pool index, or -1 if no pool should be used
     */
    static int getPoolIndexForTake(final int minimumChunkCapacity) {
        if (minimumChunkCapacity == 0) {
            return 0;
        }
        final int roundedChunkLog2Capacity =
                Math.max(MathUtil.ceilLog2(minimumChunkCapacity), SMALLEST_POOLED_CHUNK_LOG2_CAPACITY);
        if (roundedChunkLog2Capacity > LARGEST_POOLED_CHUNK_LOG2_CAPACITY) {
            return -1;
        }
        return getChunkLog2CapacityOffset(roundedChunkLog2Capacity);
    }

    /**
     * Get the index of the pool to return a chunk to.
     *
     * @param actualChunkCapacity The chunk's actual capacity
     * @return The pool index, or -1 if the chunk should not be returned to any pool
     */
    @SuppressWarnings("unused")
    static int getPoolIndexForGive(final int actualChunkCapacity) {
        if (Integer.bitCount(actualChunkCapacity) != 1) {
            return -1;
        }
        final int chunkLog2Capacity = MathUtil.ceilLog2(actualChunkCapacity);
        if (chunkLog2Capacity < SMALLEST_POOLED_CHUNK_LOG2_CAPACITY
                || chunkLog2Capacity > LARGEST_POOLED_CHUNK_LOG2_CAPACITY) {
            return -1;
        }
        return getChunkLog2CapacityOffset(chunkLog2Capacity);
    }

    private static int getChunkLog2CapacityOffset(final int chunkLog2Capacity) {
        return chunkLog2Capacity - SMALLEST_POOLED_CHUNK_LOG2_CAPACITY;
    }

    private ChunkPoolConstants() {}
}
