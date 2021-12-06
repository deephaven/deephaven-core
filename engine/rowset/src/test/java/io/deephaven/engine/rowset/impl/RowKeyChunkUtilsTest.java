/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.rowset.impl;

import io.deephaven.base.Pair;
import io.deephaven.util.datastructures.SizeException;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class RowKeyChunkUtilsTest {

    @Test
    public void testChunkMaxSizeDoesntOverflowLongAccumulator() {
        // Note we accumulate using longs and need to verify adding the next value to the accumulator will not overflow.
        assertTrue((long) (Chunk.MAXIMUM_SIZE) * 2 <= Long.MAX_VALUE);
    }

    // 1) make sure test works with regular limit
    @Test
    public void testConvertToRangesMaxChunkSizeRegularLimit() {
        tryMaxChunkSizeTest(0);
    }

    // 2) lower the limit to one less than needed and make sure it fails
    @Test(expected = SizeException.class)
    public void testConvertToRangesMaxChunkSizeHitsLimit() {
        tryMaxChunkSizeTest(3);
    }

    // 3) use the exactly needed size
    @Test
    public void testConvertToRangesMaxChunkSizeExactLimit() {
        tryMaxChunkSizeTest(4);
    }

    private void tryMaxChunkSizeTest(final long limit) {
        final LongChunk<OrderedRowKeys> chunk = createChunk(0, 2); // mind the gap
        final LongChunk<OrderedRowKeyRanges> newChunk =
                (limit == 0) ? RowKeyChunkUtils.convertToOrderedKeyRanges(chunk)
                        : RowKeyChunkUtils.convertToOrderedKeyRanges(chunk, limit);
        validateChunk(newChunk, 0, 0, 2, 2);
    }

    @Test
    public void testConvertToRangesSingleton() {
        validateChunk(RowKeyChunkUtils.convertToOrderedKeyRanges(createChunk(
                42)),
                42, 42);
    }

    @Test
    public void testConvertToRangesSingleRange() {
        validateChunk(RowKeyChunkUtils.convertToOrderedKeyRanges(createChunk(
                2, 3, 4, 5, 6, 7, 8, 9, 10, 11)),
                2, 11);
    }

    @Test
    public void testConvertToRangesMultipleRanges() {
        validateChunk(RowKeyChunkUtils.convertToOrderedKeyRanges(createChunk(
                2, 3, 4, 8, 10, 11, 12, 13, 15)),
                2, 4, 8, 8, 10, 13, 15, 15);
    }

    @Test
    public void testConvertToRangesExtremes() {
        final int MI = Integer.MAX_VALUE;
        validateChunk(RowKeyChunkUtils.convertToOrderedKeyRanges(createChunk(
                0, 1, 2, 3, 4, MI - 4, MI - 3, MI - 2, MI - 1, MI)),
                0, 4, MI - 4, MI);
    }

    @Test
    public void testConvertToIndicesSingleton() {
        validateChunk(RowKeyChunkUtils.convertToOrderedKeyIndices(createChunk(
                1022, 1022)),
                1022);
    }

    @Test
    public void testConvertToIndicesSingleRange() {
        validateChunk(RowKeyChunkUtils.convertToOrderedKeyIndices(createChunk(
                512, 519)),
                512, 513, 514, 515, 516, 517, 518, 519);
    }

    @Test
    public void testConvertToIndicesMultipleRanges() {
        validateChunk(RowKeyChunkUtils.convertToOrderedKeyIndices(createChunk(
                1, 5, 7, 7, 9, 12)),
                1, 2, 3, 4, 5, 7, 9, 10, 11, 12);
    }

    @Test
    public void testConvertToIndicesExtremes() {
        final int MI = Integer.MAX_VALUE;
        validateChunk(RowKeyChunkUtils.convertToOrderedKeyIndices(createChunk(
                0, 4, MI - 4, MI)),
                0, 1, 2, 3, 4, MI - 4, MI - 3, MI - 2, MI - 1, MI);
    }

    @Test(expected = SizeException.class)
    public void testGeneratedChunkIndiciesTooLarge() {
        // since ranges are inclusive, there is one too many elements to generate an indices chunk
        LongChunk<OrderedRowKeyRanges> chunk = createChunk(0, Chunk.MAXIMUM_SIZE);
        RowKeyChunkUtils.convertToOrderedKeyIndices(chunk);
    }

    @Test
    public void testConvertRoundTripEveryOther() {
        final long[] indices = new long[100];
        final long[] ranges = new long[indices.length * 2];
        for (int i = 0; i < indices.length; ++i) {
            indices[i] = ranges[i * 2] = ranges[i * 2 + 1] = 2 * i;
        }
        validateChunk(createChunk(indices), RowKeyChunkUtils.convertToOrderedKeyIndices(createChunk(ranges)));
    }

    @Test
    public void testConvertRoundTripRandom() {
        for (final int indexCount : new int[] {100, 1_000_000}) {
            for (final int avgElementsPerRange : new int[] {1, 4, 64, 1_000_000}) {
                for (final int sparsityFactor : new int[] {1, 10, 1_000}) {
                    final Pair<LongChunk<OrderedRowKeyRanges>, LongChunk<OrderedRowKeys>> chunks =
                            generateChunks(indexCount, avgElementsPerRange, sparsityFactor);
                    validateChunk(chunks.second, RowKeyChunkUtils.convertToOrderedKeyIndices(chunks.first));
                    validateChunk(chunks.first, RowKeyChunkUtils.convertToOrderedKeyRanges(chunks.second));
                }
            }
        }
    }

    private <ATTR extends Any> LongChunk<ATTR> createChunk(final long... values) {
        final WritableLongChunk<ATTR> chunk = WritableLongChunk.makeWritableChunk(values.length);
        for (int idx = 0; idx < values.length; ++idx) {
            chunk.set(idx, values[idx]);
        }
        return chunk;
    }

    private <ATTR extends Any> void validateChunk(final LongChunk<ATTR> chunk, final long... values) {
        assertEquals(values.length, chunk.size());
        for (int idx = 0; idx < values.length; ++idx) {
            assertEquals(values[idx], chunk.get(idx));
        }
    }

    private <ATTR extends Any> void validateChunk(final LongChunk<ATTR> expected, final LongChunk<ATTR> actual) {
        assertEquals(expected.size(), actual.size());
        for (int idx = 0; idx < expected.size(); ++idx) {
            assertEquals(expected.get(idx), actual.get(idx));
        }
    }

    private Pair<LongChunk<OrderedRowKeyRanges>, LongChunk<OrderedRowKeys>> generateChunks(final int indexCount,
            final int avgElementsPerRange,
            final int sparsityFactor) {
        final Random random = new Random(0);
        final long[] indexPoints = new long[indexCount];
        final int rangeCount = Math.max(1, (indexCount + avgElementsPerRange / 2) / avgElementsPerRange);
        final long[] indexRanges = new long[rangeCount * 2];
        long lastPos = 0;
        int remainingCount = indexCount;
        int j = 0;
        for (int i = 0; i < rangeCount - 1; i++) {
            // This is +2 to ensure that there is a gap between two ranges.
            indexRanges[2 * i] = lastPos + 2 + random.nextInt(2 * avgElementsPerRange - 1) * sparsityFactor;
            final int step = 1 + Math.max(0, Math.min(random.nextInt(2 * avgElementsPerRange - 1),
                    remainingCount - rangeCount));
            lastPos = indexRanges[2 * i + 1] = indexRanges[2 * i] + step - 1;
            remainingCount -= step;
            indexPoints[j++] = indexRanges[2 * i];
            for (int k = 1; k < step; k++) {
                indexPoints[j] = indexPoints[j - 1] + 1;
                j++;
            }
        }
        indexRanges[2 * rangeCount - 2] = lastPos + 2 + random.nextInt(2 * avgElementsPerRange);
        indexRanges[2 * rangeCount - 1] = indexRanges[2 * rangeCount - 2] + remainingCount - 1;
        indexPoints[j++] = indexRanges[2 * rangeCount - 2];
        for (int k = 1; k < remainingCount; k++) {
            indexPoints[j] = indexPoints[j - 1] + 1;
            j++;
        }

        return new Pair<>(LongChunk.chunkWrap(indexRanges), LongChunk.chunkWrap(indexPoints));
    }
}
