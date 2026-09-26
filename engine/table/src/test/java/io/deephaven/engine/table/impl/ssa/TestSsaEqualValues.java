//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the as-of join segmented sorted array iterator when equal stamp values are not identical: distinct
 * instances of equal objects, and NaN floating point values, which the SSA orders as equal to each other.
 */
public class TestSsaEqualValues {
    private static final int NODE_SIZE = 4;
    private static final int RUN_LENGTH = 2 * NODE_SIZE;

    /**
     * Distinct String instances that compare equal to each other.
     */
    private static String fresh(final String value) {
        return new String(value.toCharArray());
    }

    private static WritableLongChunk<RowKeys> keys(final long... keys) {
        final WritableLongChunk<RowKeys> chunk = WritableLongChunk.makeWritableChunk(keys.length);
        for (int ii = 0; ii < keys.length; ++ii) {
            chunk.set(ii, keys[ii]);
        }
        return chunk;
    }

    private static WritableObjectChunk<Object, Values> objects(final Object... values) {
        final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(values.length);
        for (int ii = 0; ii < values.length; ++ii) {
            chunk.set(ii, values[ii]);
        }
        return chunk;
    }

    private static WritableDoubleChunk<Values> doubles(final double... values) {
        final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(values.length);
        for (int ii = 0; ii < values.length; ++ii) {
            chunk.set(ii, values[ii]);
        }
        return chunk;
    }

    private static WritableFloatChunk<Values> floats(final float... values) {
        final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(values.length);
        for (int ii = 0; ii < values.length; ++ii) {
            chunk.set(ii, values[ii]);
        }
        return chunk;
    }

    private static long[] sequentialKeys(final int count) {
        final long[] result = new long[count];
        for (int ii = 0; ii < count; ++ii) {
            result[ii] = ii;
        }
        return result;
    }

    /**
     * Builds an SSA holding RUN_LENGTH equal values (row keys 0 .. RUN_LENGTH - 1), which spans two leaves, then stamps
     * a single left value equal to that run. An exact match takes the last row of the run.
     */
    private static long stampAgainstEqualRun(final ChunkType chunkType, final boolean reverse,
            final WritableChunk<Values> rightValues, final WritableChunk<Values> leftValue) {
        final SegmentedSortedArray ssa = SegmentedSortedArray.make(chunkType, reverse, NODE_SIZE);
        try (final WritableLongChunk<RowKeys> rightKeys = keys(sequentialKeys(RUN_LENGTH));
                final WritableLongChunk<RowKeys> leftKeys = keys(100);
                final WritableLongChunk<RowKeys> result = WritableLongChunk.makeWritableChunk(1)) {
            ssa.insert(rightValues, rightKeys);
            ChunkSsaStamp.make(chunkType, reverse).processEntry(leftValue, leftKeys, ssa, result, false);
            return result.get(0);
        } finally {
            rightValues.close();
            leftValue.close();
        }
    }

    private static Object[] freshRun(final String value) {
        final Object[] result = new Object[RUN_LENGTH];
        for (int ii = 0; ii < RUN_LENGTH; ++ii) {
            result[ii] = fresh(value);
        }
        return result;
    }

    private static double[] doubleRun(final double value) {
        final double[] result = new double[RUN_LENGTH];
        Arrays.fill(result, value);
        return result;
    }

    private static float[] floatRun(final float value) {
        final float[] result = new float[RUN_LENGTH];
        Arrays.fill(result, value);
        return result;
    }

    @Test
    public void testObjectChunkStampEqualRunAcrossLeaves() {
        assertEquals(RUN_LENGTH - 1, stampAgainstEqualRun(ChunkType.Object, false,
                objects(freshRun("b")), objects(fresh("b"))));
    }

    @Test
    public void testObjectReverseChunkStampEqualRunAcrossLeaves() {
        assertEquals(RUN_LENGTH - 1, stampAgainstEqualRun(ChunkType.Object, true,
                objects(freshRun("b")), objects(fresh("b"))));
    }

    @Test
    public void testDoubleReverseChunkStampNaNRunAcrossLeaves() {
        assertEquals(RUN_LENGTH - 1, stampAgainstEqualRun(ChunkType.Double, true,
                doubles(doubleRun(Double.NaN)), doubles(Double.NaN)));
    }

    @Test
    public void testFloatReverseChunkStampNaNRunAcrossLeaves() {
        assertEquals(RUN_LENGTH - 1, stampAgainstEqualRun(ChunkType.Float, true,
                floats(floatRun(Float.NaN)), floats(Float.NaN)));
    }

    /**
     * With exact matches disallowed, a left row equal to an inserted right row must not be restamped to it. The left
     * SSA holds "a" (row 0) and "b" (row 1); the right SSA holds "a" (row 0), so left row 1 matches right row 0. The
     * right side then gains "b" (row 1), which leaves left row 1 matched to right row 0.
     */
    @Test
    public void testObjectSsaSsaInsertionDisallowExact() {
        final SegmentedSortedArray leftSsa = SegmentedSortedArray.make(ChunkType.Object, false, NODE_SIZE);
        final SegmentedSortedArray rightSsa = SegmentedSortedArray.make(ChunkType.Object, false, NODE_SIZE);
        final ContiguousWritableRowRedirection redirection = new ContiguousWritableRowRedirection(8);
        try (final WritableObjectChunk<Object, Values> leftValues = objects(fresh("a"), fresh("b"));
                final WritableLongChunk<RowKeys> leftKeys = keys(0, 1);
                final WritableObjectChunk<Object, Values> rightValues = objects(fresh("a"));
                final WritableLongChunk<RowKeys> rightKeys = keys(0);
                final WritableObjectChunk<Object, Values> insertValues = objects(fresh("b"));
                final WritableLongChunk<RowKeys> insertKeys = keys(1);
                final WritableObjectChunk<Object, Values> nextValues = WritableObjectChunk.makeWritableChunk(1)) {
            leftSsa.insert(leftValues, leftKeys);
            rightSsa.insert(rightValues, rightKeys);
            final SsaSsaStamp stamp = SsaSsaStamp.make(ChunkType.Object, false);
            stamp.processEntry(leftSsa, rightSsa, redirection, true);
            assertEquals(RowSequence.NULL_ROW_KEY, redirection.get(0));
            assertEquals(0, redirection.get(1));

            final int valuesWithNext = rightSsa.insertAndGetNextValue(insertValues, insertKeys, nextValues);
            // the inserted value is the last one in the right SSA
            assertEquals(0, valuesWithNext);
            stamp.processInsertion(leftSsa, insertValues, insertKeys, nextValues, redirection,
                    RowSetFactory.builderRandom(), true, true);
            assertEquals(RowSequence.NULL_ROW_KEY, redirection.get(0));
            assertEquals(0, redirection.get(1));
        }
    }

    /**
     * With exact matches disallowed, removing a right row restamps the left rows that matched it. The left SSA holds
     * "a" (row 0), "b" (row 1) and "c" (row 2); the right SSA holds "a" (row 0) and "b" (row 1), so left row 2 matches
     * right row 1. Removing right row 1 restamps left row 2 to right row 0.
     */
    @Test
    public void testObjectSsaSsaRemovalDisallowExact() {
        final SegmentedSortedArray leftSsa = SegmentedSortedArray.make(ChunkType.Object, false, NODE_SIZE);
        final SegmentedSortedArray rightSsa = SegmentedSortedArray.make(ChunkType.Object, false, NODE_SIZE);
        final ContiguousWritableRowRedirection redirection = new ContiguousWritableRowRedirection(8);
        try (final WritableObjectChunk<Object, Values> leftValues = objects(fresh("a"), fresh("b"), fresh("c"));
                final WritableLongChunk<RowKeys> leftKeys = keys(0, 1, 2);
                final WritableObjectChunk<Object, Values> rightValues = objects(fresh("a"), fresh("b"));
                final WritableLongChunk<RowKeys> rightKeys = keys(0, 1);
                final WritableObjectChunk<Object, Values> removeValues = objects(fresh("b"));
                final WritableLongChunk<RowKeys> removeKeys = keys(1);
                final WritableLongChunk<RowKeys> priorKeys = WritableLongChunk.makeWritableChunk(1)) {
            leftSsa.insert(leftValues, leftKeys);
            rightSsa.insert(rightValues, rightKeys);
            final SsaSsaStamp stamp = SsaSsaStamp.make(ChunkType.Object, false);
            stamp.processEntry(leftSsa, rightSsa, redirection, true);
            assertEquals(0, redirection.get(1));
            assertEquals(1, redirection.get(2));

            rightSsa.removeAndGetPrior(removeValues, removeKeys, priorKeys);
            assertEquals(0, priorKeys.get(0));
            stamp.processRemovals(leftSsa, removeValues, removeKeys, priorKeys, redirection,
                    RowSetFactory.builderRandom(), true);
            assertEquals(0, redirection.get(1));
            assertEquals(0, redirection.get(2));
        }
    }

    /**
     * With exact matches disallowed, a NaN left row is not matched by a NaN right row, because the SSA orders NaN as
     * equal to NaN. The left SSA holds 1.0 (row 0) and NaN (row 1); the right SSA holds 1.0 (row 0), so left row 1
     * matches right row 0. Inserting a NaN right row (row 1) leaves left row 1 matched to right row 0.
     */
    @Test
    public void testDoubleSsaSsaInsertionDisallowExactNaN() {
        final SegmentedSortedArray leftSsa = SegmentedSortedArray.make(ChunkType.Double, false, NODE_SIZE);
        final SegmentedSortedArray rightSsa = SegmentedSortedArray.make(ChunkType.Double, false, NODE_SIZE);
        final ContiguousWritableRowRedirection redirection = new ContiguousWritableRowRedirection(8);
        try (final WritableDoubleChunk<Values> leftValues = doubles(1.0, Double.NaN);
                final WritableLongChunk<RowKeys> leftKeys = keys(0, 1);
                final WritableDoubleChunk<Values> rightValues = doubles(1.0);
                final WritableLongChunk<RowKeys> rightKeys = keys(0);
                final WritableDoubleChunk<Values> insertValues = doubles(Double.NaN);
                final WritableLongChunk<RowKeys> insertKeys = keys(1);
                final WritableDoubleChunk<Values> nextValues = WritableDoubleChunk.makeWritableChunk(1)) {
            leftSsa.insert(leftValues, leftKeys);
            rightSsa.insert(rightValues, rightKeys);
            final SsaSsaStamp stamp = SsaSsaStamp.make(ChunkType.Double, false);
            stamp.processEntry(leftSsa, rightSsa, redirection, true);
            assertEquals(0, redirection.get(1));

            final int valuesWithNext = rightSsa.insertAndGetNextValue(insertValues, insertKeys, nextValues);
            assertEquals(0, valuesWithNext);
            stamp.processInsertion(leftSsa, insertValues, insertKeys, nextValues, redirection,
                    RowSetFactory.builderRandom(), true, true);
            assertEquals(0, redirection.get(1));
        }
    }
}
