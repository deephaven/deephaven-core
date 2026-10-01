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
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.join.dupcompact.DupCompactKernel;
import io.deephaven.engine.table.impl.join.dupcompact.EqualsConsistentObjectDupCompactKernel;
import io.deephaven.engine.table.impl.join.dupcompact.EqualsConsistentObjectReverseDupCompactKernel;
import io.deephaven.engine.table.impl.join.dupcompact.ObjectDupCompactKernel;
import io.deephaven.engine.table.impl.join.dupcompact.ObjectReverseDupCompactKernel;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.compareConsistentWithEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the as-of join segmented sorted array iterator when equal stamp values are not identical: distinct
 * instances of equal objects, objects that compare equal without being equals (BigDecimal 1.0 and 1.00), and NaN
 * floating point values, which the SSA orders as equal to each other.
 */
public class TestSsaEqualValues {
    private static final int NODE_SIZE = 4;
    private static final int RUN_LENGTH = 2 * NODE_SIZE;

    /**
     * String values run through both the equals consistent Object classes and the general Object classes.
     */
    private static final boolean[] EQUALS_CONSISTENT = {true, false};

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
    private static long stampAgainstEqualRun(final ChunkType chunkType, final boolean equalsConsistent,
            final boolean reverse, final WritableChunk<Values> rightValues, final WritableChunk<Values> leftValue) {
        final SegmentedSortedArray ssa = SegmentedSortedArray.make(chunkType, equalsConsistent, reverse, NODE_SIZE);
        try (final WritableLongChunk<RowKeys> rightKeys = keys(sequentialKeys(RUN_LENGTH));
                final WritableLongChunk<RowKeys> leftKeys = keys(100);
                final WritableLongChunk<RowKeys> result = WritableLongChunk.makeWritableChunk(1)) {
            ssa.insert(rightValues, rightKeys);
            ChunkSsaStamp.make(chunkType, equalsConsistent, reverse).processEntry(leftValue, leftKeys, ssa, result,
                    false);
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
        for (final boolean equalsConsistent : EQUALS_CONSISTENT) {
            assertEquals("equalsConsistent=" + equalsConsistent, RUN_LENGTH - 1,
                    stampAgainstEqualRun(ChunkType.Object, equalsConsistent, false,
                            objects(freshRun("b")), objects(fresh("b"))));
        }
    }

    @Test
    public void testObjectReverseChunkStampEqualRunAcrossLeaves() {
        for (final boolean equalsConsistent : EQUALS_CONSISTENT) {
            assertEquals("equalsConsistent=" + equalsConsistent, RUN_LENGTH - 1,
                    stampAgainstEqualRun(ChunkType.Object, equalsConsistent, true,
                            objects(freshRun("b")), objects(fresh("b"))));
        }
    }

    @Test
    public void testDoubleReverseChunkStampNaNRunAcrossLeaves() {
        assertEquals(RUN_LENGTH - 1, stampAgainstEqualRun(ChunkType.Double, false, true,
                doubles(doubleRun(Double.NaN)), doubles(Double.NaN)));
    }

    @Test
    public void testFloatReverseChunkStampNaNRunAcrossLeaves() {
        assertEquals(RUN_LENGTH - 1, stampAgainstEqualRun(ChunkType.Float, false, true,
                floats(floatRun(Float.NaN)), floats(Float.NaN)));
    }

    /**
     * With exact matches disallowed, a left row equal to an inserted right row must not be restamped to it. The left
     * SSA holds "a" (row 0) and "b" (row 1); the right SSA holds "a" (row 0), so left row 1 matches right row 0. The
     * right side then gains "b" (row 1), which leaves left row 1 matched to right row 0.
     */
    @Test
    public void testObjectSsaSsaInsertionDisallowExact() {
        for (final boolean equalsConsistent : EQUALS_CONSISTENT) {
            final SegmentedSortedArray leftSsa =
                    SegmentedSortedArray.make(ChunkType.Object, equalsConsistent, false, NODE_SIZE);
            final SegmentedSortedArray rightSsa =
                    SegmentedSortedArray.make(ChunkType.Object, equalsConsistent, false, NODE_SIZE);
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
                final SsaSsaStamp stamp = SsaSsaStamp.make(ChunkType.Object, equalsConsistent, false);
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
    }

    /**
     * With exact matches disallowed, removing a right row restamps the left rows that matched it. The left SSA holds
     * "a" (row 0), "b" (row 1) and "c" (row 2); the right SSA holds "a" (row 0) and "b" (row 1), so left row 2 matches
     * right row 1. Removing right row 1 restamps left row 2 to right row 0.
     */
    @Test
    public void testObjectSsaSsaRemovalDisallowExact() {
        for (final boolean equalsConsistent : EQUALS_CONSISTENT) {
            final SegmentedSortedArray leftSsa =
                    SegmentedSortedArray.make(ChunkType.Object, equalsConsistent, false, NODE_SIZE);
            final SegmentedSortedArray rightSsa =
                    SegmentedSortedArray.make(ChunkType.Object, equalsConsistent, false, NODE_SIZE);
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
                final SsaSsaStamp stamp = SsaSsaStamp.make(ChunkType.Object, equalsConsistent, false);
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
    }

    /**
     * With exact matches disallowed, a NaN left row is not matched by a NaN right row, because the SSA orders NaN as
     * equal to NaN. The left SSA holds 1.0 (row 0) and NaN (row 1); the right SSA holds 1.0 (row 0), so left row 1
     * matches right row 0. Inserting a NaN right row (row 1) leaves left row 1 matched to right row 0.
     */
    @Test
    public void testDoubleSsaSsaInsertionDisallowExactNaN() {
        final SegmentedSortedArray leftSsa =
                SegmentedSortedArray.make(ChunkType.Double, false, false, NODE_SIZE);
        final SegmentedSortedArray rightSsa =
                SegmentedSortedArray.make(ChunkType.Double, false, false, NODE_SIZE);
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
            final SsaSsaStamp stamp = SsaSsaStamp.make(ChunkType.Double, false, false);
            stamp.processEntry(leftSsa, rightSsa, redirection, true);
            assertEquals(0, redirection.get(1));

            final int valuesWithNext = rightSsa.insertAndGetNextValue(insertValues, insertKeys, nextValues);
            assertEquals(0, valuesWithNext);
            stamp.processInsertion(leftSsa, insertValues, insertKeys, nextValues, redirection,
                    RowSetFactory.builderRandom(), true, true);
            assertEquals(0, redirection.get(1));
        }
    }

    private static void insertOne(final SegmentedSortedArray ssa, final Object value, final long rowKey) {
        try (final WritableObjectChunk<Object, Values> values = objects(value);
                final WritableLongChunk<RowKeys> rowKeys = keys(rowKey)) {
            ssa.insert(values, rowKeys);
        }
    }

    private static void removeOne(final SegmentedSortedArray ssa, final Object value, final long rowKey) {
        try (final WritableObjectChunk<Object, Values> values = objects(value);
                final WritableLongChunk<RowKeys> rowKeys = keys(rowKey)) {
            ssa.remove(values, rowKeys);
        }
    }

    private static List<Long> keysOf(final SegmentedSortedArray ssa) {
        final List<Long> result = new ArrayList<>();
        ssa.forAllKeys(result::add);
        return result;
    }

    /**
     * BigDecimal 1.0 and 1.00 compare equal, so the SSA orders them by row key like any other run of equal values.
     */
    @Test
    public void testObjectSsaOrdersCompareEqualValuesByRowKey() {
        for (final boolean reverse : new boolean[] {false, true}) {
            final SegmentedSortedArray ssa =
                    SegmentedSortedArray.make(ChunkType.Object, false, reverse, NODE_SIZE);
            insertOne(ssa, new BigDecimal("1.0"), 5);
            insertOne(ssa, new BigDecimal("1.00"), 3);
            insertOne(ssa, new BigDecimal("1.000"), 4);
            assertEquals("reverse=" + reverse, List.of(3L, 4L, 5L), keysOf(ssa));
        }
    }

    /**
     * Removing a value that compares equal to, but is not equals to, its neighbours removes exactly the requested row.
     */
    @Test
    public void testObjectSsaRemovesCompareEqualValue() {
        for (final boolean reverse : new boolean[] {false, true}) {
            for (final int nodeSize : new int[] {2, 4}) {
                final SegmentedSortedArray ssa =
                        SegmentedSortedArray.make(ChunkType.Object, false, reverse, nodeSize);
                insertOne(ssa, new BigDecimal("0"), 0);
                insertOne(ssa, new BigDecimal("1.0"), 5);
                insertOne(ssa, new BigDecimal("1.0"), 8);
                insertOne(ssa, new BigDecimal("2"), 9);
                insertOne(ssa, new BigDecimal("1.00"), 3);
                insertOne(ssa, new BigDecimal("1.00"), 6);
                removeOne(ssa, new BigDecimal("1.00"), 6);
                removeOne(ssa, new BigDecimal("1.0"), 5);
                final List<Long> expected = reverse ? List.of(9L, 3L, 8L, 0L) : List.of(0L, 3L, 8L, 9L);
                assertEquals("reverse=" + reverse + ", nodeSize=" + nodeSize, expected, keysOf(ssa));
                assertEquals(4, ssa.size());
            }
        }
    }

    /**
     * Duplicate compaction treats BigDecimal 1.0 and 1.00 as one run: compactDuplicates keeps the last row of the run
     * and compactDuplicatesPreferFirst keeps the first.
     */
    @Test
    public void testObjectDupCompactCompareEqualValues() {
        for (final boolean reverse : new boolean[] {false, true}) {
            final DupCompactKernel kernel =
                    DupCompactKernel.makeDupCompactNaturalOrdering(ChunkType.Object, false, reverse);
            final Object low = reverse ? new BigDecimal("2") : new BigDecimal("0");
            final Object high = reverse ? new BigDecimal("0") : new BigDecimal("2");
            try (final WritableObjectChunk<Object, Values> values =
                    objects(low, new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("1"), high);
                    final WritableLongChunk<RowKeys> rowKeys = keys(10, 11, 12, 13, 14)) {
                assertEquals("reverse=" + reverse, -1, kernel.compactDuplicates(values, rowKeys));
                assertEquals("reverse=" + reverse, 3, rowKeys.size());
                assertEquals(10, rowKeys.get(0));
                assertEquals(13, rowKeys.get(1));
                assertEquals(14, rowKeys.get(2));
            }
            try (final WritableObjectChunk<Object, Values> values =
                    objects(low, new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("1"), high);
                    final WritableIntChunk<ChunkPositions> positions = WritableIntChunk.makeWritableChunk(5)) {
                for (int ii = 0; ii < 5; ++ii) {
                    positions.set(ii, ii);
                }
                assertEquals("reverse=" + reverse, -1, kernel.compactDuplicatesPreferFirst(values, positions));
                assertEquals("reverse=" + reverse, 3, positions.size());
                assertEquals(0, positions.get(0));
                assertEquals(1, positions.get(1));
                assertEquals(4, positions.get(2));
            }
        }
    }

    /**
     * A Comparable whose natural ordering is not known to be consistent with equals.
     */
    private static final class OtherComparable implements Comparable<OtherComparable> {
        @Override
        public int compareTo(final OtherComparable other) {
            return 0;
        }
    }

    /**
     * The registry reports a natural ordering consistent with equals for String, and not for BigDecimal, other
     * Comparables, Object or CharSequence; BigDecimal values therefore use the general Object classes above.
     */
    @Test
    public void testRegistryDecisionByDataType() {
        assertTrue(compareConsistentWithEquality(String.class));
        for (final Class<?> dataType : new Class<?>[] {BigDecimal.class, OtherComparable.class, Object.class,
                CharSequence.class}) {
            assertFalse(dataType.getName(), compareConsistentWithEquality(dataType));
        }
    }

    /**
     * The factories choose the equals consistent Object classes when equalsConsistent is true, and the general Object
     * classes when it is false.
     */
    @Test
    public void testObjectFactoriesChooseByDecision() {
        for (final boolean reverse : new boolean[] {false, true}) {
            final String context = "reverse=" + reverse;
            assertTrue(context, SegmentedSortedArray.make(ChunkType.Object, true, reverse,
                    NODE_SIZE) instanceof EqualsConsistentObjectSegmentedSortedArray != reverse);
            assertTrue(context, SegmentedSortedArray.make(ChunkType.Object, true, reverse,
                    NODE_SIZE) instanceof EqualsConsistentObjectReverseSegmentedSortedArray == reverse);
            assertTrue(context, SegmentedSortedArray.makeFactory(ChunkType.Object, true, reverse,
                    NODE_SIZE).get() instanceof EqualsConsistentObjectSegmentedSortedArray != reverse);
            assertTrue(context, ChunkSsaStamp.make(ChunkType.Object, true,
                    reverse) instanceof EqualsConsistentObjectChunkSsaStamp != reverse);
            assertTrue(context, ChunkSsaStamp.make(ChunkType.Object, true,
                    reverse) instanceof EqualsConsistentObjectReverseChunkSsaStamp == reverse);
            assertTrue(context, SsaSsaStamp.make(ChunkType.Object, true,
                    reverse) instanceof EqualsConsistentObjectSsaSsaStamp != reverse);
            assertTrue(context, SsaSsaStamp.make(ChunkType.Object, true,
                    reverse) instanceof EqualsConsistentObjectReverseSsaSsaStamp == reverse);
            for (final DupCompactKernel kernel : new DupCompactKernel[] {
                    DupCompactKernel.makeDupCompactNaturalOrdering(ChunkType.Object, true, reverse),
                    DupCompactKernel.makeDupCompactDeephavenOrdering(ChunkType.Object, true, reverse)}) {
                assertTrue(context, kernel instanceof EqualsConsistentObjectDupCompactKernel != reverse);
                assertTrue(context, kernel instanceof EqualsConsistentObjectReverseDupCompactKernel == reverse);
            }

            assertTrue(context, SegmentedSortedArray.make(ChunkType.Object, false, reverse,
                    NODE_SIZE) instanceof ObjectSegmentedSortedArray != reverse);
            assertTrue(context, SegmentedSortedArray.make(ChunkType.Object, false, reverse,
                    NODE_SIZE) instanceof ObjectReverseSegmentedSortedArray == reverse);
            assertTrue(context, SegmentedSortedArray.makeFactory(ChunkType.Object, false, reverse,
                    NODE_SIZE).get() instanceof ObjectSegmentedSortedArray != reverse);
            assertTrue(context, ChunkSsaStamp.make(ChunkType.Object, false,
                    reverse) instanceof ObjectChunkSsaStamp != reverse);
            assertTrue(context, ChunkSsaStamp.make(ChunkType.Object, false,
                    reverse) instanceof ObjectReverseChunkSsaStamp == reverse);
            assertTrue(context, SsaSsaStamp.make(ChunkType.Object, false,
                    reverse) instanceof ObjectSsaSsaStamp != reverse);
            assertTrue(context, SsaSsaStamp.make(ChunkType.Object, false,
                    reverse) instanceof ObjectReverseSsaSsaStamp == reverse);
            for (final DupCompactKernel kernel : new DupCompactKernel[] {
                    DupCompactKernel.makeDupCompactNaturalOrdering(ChunkType.Object, false, reverse),
                    DupCompactKernel.makeDupCompactDeephavenOrdering(ChunkType.Object, false, reverse)}) {
                assertTrue(context, kernel instanceof ObjectDupCompactKernel != reverse);
                assertTrue(context, kernel instanceof ObjectReverseDupCompactKernel == reverse);
            }
        }
    }

    /**
     * Primitive chunk types have a single family, which the factories return for either decision.
     */
    @Test
    public void testPrimitiveFactoriesIgnoreDecision() {
        for (final boolean equalsConsistent : EQUALS_CONSISTENT) {
            final String context = "equalsConsistent=" + equalsConsistent;
            assertTrue(context, SegmentedSortedArray.make(ChunkType.Double, equalsConsistent, false,
                    NODE_SIZE) instanceof DoubleSegmentedSortedArray);
            assertTrue(context, ChunkSsaStamp.make(ChunkType.Double, equalsConsistent,
                    false) instanceof DoubleChunkSsaStamp);
            assertTrue(context, SsaSsaStamp.make(ChunkType.Double, equalsConsistent,
                    false) instanceof DoubleSsaSsaStamp);
        }
    }
}
