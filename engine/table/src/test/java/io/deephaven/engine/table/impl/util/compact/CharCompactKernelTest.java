//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.compact;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.IntChunkEquals;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.util.Arrays;

public class CharCompactKernelTest {

    @Test
    public void emptyTest() {
        check(new char[0], true, new char[0], new int[0]);
        check(new char[0], false, new char[0], new int[0]);
    }

    @Test
    public void compactAndCount() {
        check(
                new char[] {Character.MIN_VALUE, 'a', QueryConstants.NULL_CHAR},
                true,
                new char[] {QueryConstants.NULL_CHAR, Character.MIN_VALUE, 'a'},
                new int[] {1, 1, 1});
        check(
                new char[] {Character.MIN_VALUE, 'a', QueryConstants.NULL_CHAR},
                false,
                new char[] {Character.MIN_VALUE, 'a'},
                new int[] {1, 1});
    }

    private static void check(char[] input, boolean countNullNan, char[] expectedSorted, int[] expectedCounts) {
        try (final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(input.length)) {
            final WritableCharChunk<Values> chunk = WritableCharChunk.writableChunkWrap(input);
            CharCompactKernel.compactAndCount(chunk, counts, countNullNan);
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
