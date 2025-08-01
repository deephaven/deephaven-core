//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ChunkType;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.LARGEST_POOLED_CHUNK_LOG2_CAPACITY;

/**
 * Basic unit tests for chunk pooling.
 */
public class TestChunkPooling extends TestCase {

    public void testTakeAndGiveWithTracking() {
        ChunkPoolReleaseTracking.enable();
        try {
            MultiChunkPool.enableDedicatedPoolForThisThread();
            final List<PoolableChunk<?>> chunksToGive = new ArrayList<>();
            final int maxLog2Capacity = LARGEST_POOLED_CHUNK_LOG2_CAPACITY == 30
                    ? LARGEST_POOLED_CHUNK_LOG2_CAPACITY
                    : LARGEST_POOLED_CHUNK_LOG2_CAPACITY + 1;
            for (ChunkType chunkType : List.of(ChunkType.Long)) {
                for (int ci = 0; ci < 100; ++ci) {
                    for (int log2Capacity = 0; log2Capacity <= maxLog2Capacity; ++log2Capacity) {
                        chunksToGive.add(chunkType.makeWritableChunk(1 << log2Capacity));
                    }
                    chunksToGive.add(chunkType.makeResettableReadOnlyChunk());
                    chunksToGive.add(chunkType.makeResettableWritableChunk());
                }
                chunksToGive.forEach(PoolableChunk::close);
                chunksToGive.clear();
                ChunkPoolReleaseTracking.check();
            }
        } finally {
            ChunkPoolReleaseTracking.disable();
        }
    }

    public void testTakeAndGiveEmpty() {
        ChunkPoolReleaseTracking.enable();
        try {
            MultiChunkPool.enableDedicatedPoolForThisThread();
            final List<PoolableChunk<?>> chunksToGive = new ArrayList<>();
            for (ChunkType chunkType : ChunkType.values()) {
                for (int ci = 0; ci < 100; ++ci) {
                    chunksToGive.add(chunkType.makeWritableChunk(0));
                }
                TestCase.assertEquals(1, chunksToGive.stream().distinct().count());
                chunksToGive.forEach(PoolableChunk::close);
                chunksToGive.clear();
                ChunkPoolReleaseTracking.check();
            }
        } finally {
            ChunkPoolReleaseTracking.disable();
        }
    }
}
