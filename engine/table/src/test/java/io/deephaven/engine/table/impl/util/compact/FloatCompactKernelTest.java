//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.compact;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.IntChunkEquals;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.util.Arrays;

public class FloatCompactKernelTest {

    @Test
    public void emptyTest() {
        check(new float[0], true, new float[0], new int[0]);
        check(new float[0], false, new float[0], new int[0]);
    }

    @Test
    public void compactAndCount() {
        check(new float[] {
                0.0f, -0.0f, QueryConstants.NULL_FLOAT, Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY},
                true,
                new float[] {QueryConstants.NULL_FLOAT, Float.NEGATIVE_INFINITY, -0.0f, Float.POSITIVE_INFINITY,
                        Float.NaN},
                new int[] {1, 1, 2, 1, 1});
        check(new float[] {
                0.0f, -0.0f, QueryConstants.NULL_FLOAT, Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY},
                false,
                new float[] {Float.NEGATIVE_INFINITY, -0.0f, Float.POSITIVE_INFINITY},
                new int[] {1, 2, 1});
    }

    private static void check(float[] input, boolean countNullNan, float[] expectedSorted, int[] expectedCounts) {
        try (final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(input.length)) {
            final WritableFloatChunk<Values> chunk = WritableFloatChunk.writableChunkWrap(input);
            FloatCompactKernel.compactAndCount(chunk, counts, countNullNan);
            // can't use ChunkEquals b/c that treats -0.0 == 0.0
            Assert.eqTrue(
                    Arrays.equals(input, 0, chunk.size(), expectedSorted, 0, expectedSorted.length),
                    "Arrays.equals(input, 0, chunk.size(), expectedSorted, 0, expectedSorted.length)");
            Assert.eqTrue(
                    IntChunkEquals.equalReduce(counts, IntChunk.chunkWrap(expectedCounts)),
                    "IntChunkEquals.equalReduce(counts, IntChunk.chunkWrap(expectedCounts))");
        }
    }
}
