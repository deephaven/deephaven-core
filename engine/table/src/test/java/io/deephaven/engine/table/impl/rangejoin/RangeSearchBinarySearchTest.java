//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * The floating point searches treat {@code -0.0} and {@code 0.0} as equal, as the range search kernels compare them,
 * wherever the zero sits in the searched range.
 */
public class RangeSearchBinarySearchTest {

    @Test
    public void testDoubleSignedZeroesAreEqual() {
        final DoubleChunk<Values> negativeZero = DoubleChunk.chunkWrap(new double[] {-2.0, -1.0, -0.0, 1.0, 2.0});
        assertEquals(2, RangeSearchBinarySearch.binarySearch(negativeZero, 0, 5, 0.0));
        assertEquals(2, RangeSearchBinarySearch.binarySearch(negativeZero, 0, 5, -0.0));

        final DoubleChunk<Values> positiveZero = DoubleChunk.chunkWrap(new double[] {-1.0, 0.0, 1.0});
        assertEquals(1, RangeSearchBinarySearch.binarySearch(positiveZero, 0, 3, -0.0));
        assertEquals(1, RangeSearchBinarySearch.binarySearch(positiveZero, 1, 3, -0.0));
        assertEquals(1, RangeSearchBinarySearch.binarySearch(positiveZero, 0, 2, -0.0));
    }

    @Test
    public void testFloatSignedZeroesAreEqual() {
        final FloatChunk<Values> negativeZero = FloatChunk.chunkWrap(new float[] {-2.0f, -1.0f, -0.0f, 1.0f, 2.0f});
        assertEquals(2, RangeSearchBinarySearch.binarySearch(negativeZero, 0, 5, 0.0f));
        assertEquals(2, RangeSearchBinarySearch.binarySearch(negativeZero, 0, 5, -0.0f));

        final FloatChunk<Values> positiveZero = FloatChunk.chunkWrap(new float[] {-1.0f, 0.0f, 1.0f});
        assertEquals(1, RangeSearchBinarySearch.binarySearch(positiveZero, 0, 3, -0.0f));
        assertEquals(1, RangeSearchBinarySearch.binarySearch(positiveZero, 1, 3, -0.0f));
        assertEquals(1, RangeSearchBinarySearch.binarySearch(positiveZero, 0, 2, -0.0f));
    }

    @Test
    public void testAbsentKeysReturnTheInsertionPoint() {
        final DoubleChunk<Values> doubles = DoubleChunk.chunkWrap(new double[] {-1.0, 0.0, 1.0, 3.0});
        assertEquals(~0, RangeSearchBinarySearch.binarySearch(doubles, 0, 4, -2.0));
        assertEquals(~3, RangeSearchBinarySearch.binarySearch(doubles, 0, 4, 2.0));
        assertEquals(~4, RangeSearchBinarySearch.binarySearch(doubles, 0, 4, 4.0));
        assertEquals(~2, RangeSearchBinarySearch.binarySearch(doubles, 2, 4, 0.5));

        final FloatChunk<Values> floats = FloatChunk.chunkWrap(new float[] {-1.0f, 0.0f, 1.0f, 3.0f});
        assertEquals(~0, RangeSearchBinarySearch.binarySearch(floats, 0, 4, -2.0f));
        assertEquals(~3, RangeSearchBinarySearch.binarySearch(floats, 0, 4, 2.0f));
        assertEquals(~4, RangeSearchBinarySearch.binarySearch(floats, 0, 4, 4.0f));
        assertEquals(~2, RangeSearchBinarySearch.binarySearch(floats, 2, 4, 0.5f));
    }
}
