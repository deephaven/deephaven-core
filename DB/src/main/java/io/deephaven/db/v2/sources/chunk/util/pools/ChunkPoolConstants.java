package io.deephaven.db.v2.sources.chunk.util.pools;

import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
import io.deephaven.util.annotations.VisibleForTesting;

/**
 * Repository for constants used by {@link ChunkPool} implementations.
 */
final class ChunkPoolConstants {

    static final int SMALLEST_POOLED_CHUNK_LOG2_CAPACITY = 5;
    // NB: It's probably best for this to allow Barrage delta chunks to be poolable. See
    // BarrageMessageProducer.DELTA_CHUNK_SIZE.
    @VisibleForTesting
    static final int LARGEST_POOLED_CHUNK_LOG2_CAPACITY = 16;
    static final int NUM_POOLED_CHUNK_CAPACITIES =
        LARGEST_POOLED_CHUNK_LOG2_CAPACITY - SMALLEST_POOLED_CHUNK_LOG2_CAPACITY + 1;

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

    static final int SUB_POOL_SEGMENT_CAPACITY = 10;

    private ChunkPoolConstants() {}
}
