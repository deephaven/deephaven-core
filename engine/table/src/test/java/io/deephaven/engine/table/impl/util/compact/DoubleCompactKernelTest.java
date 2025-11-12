//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.compact;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.IntChunkEquals;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.util.Arrays;

public class DoubleCompactKernelTest {

    @Test
    public void emptyTest() {
        check(new double[0], true, new double[0], new int[0]);
        check(new double[0], false, new double[0], new int[0]);
    }

    @Test
    public void compactAndCount() {
        check(new double[] {
                0.0, -0.0, QueryConstants.NULL_DOUBLE, Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY},
                true,
                new double[] {QueryConstants.NULL_DOUBLE, Double.NEGATIVE_INFINITY, -0.0, Double.POSITIVE_INFINITY,
                        Double.NaN},
                new int[] {1, 1, 2, 1, 1});
        check(new double[] {
                0.0, -0.0, QueryConstants.NULL_DOUBLE, Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY},
                false,
                new double[] {Double.NEGATIVE_INFINITY, -0.0, Double.POSITIVE_INFINITY},
                new int[] {1, 2, 1});
    }

    private static void check(double[] input, boolean countNullNan, double[] expectedSorted, int[] expectedCounts) {
        try (final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(input.length)) {
            final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.writableChunkWrap(input);
            DoubleCompactKernel.compactAndCount(chunk, counts, countNullNan);
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
