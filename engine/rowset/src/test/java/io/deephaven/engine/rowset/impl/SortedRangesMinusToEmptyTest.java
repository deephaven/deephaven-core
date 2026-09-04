//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link RowSet#minus} of a sorted-ranges-backed rowset and a single range covering all of it is empty. An empty result
 * is represented by {@link OrderedLongSet#EMPTY}, like every other empty rowset, so that it answers every query the way
 * an empty rowset does: its run length estimate is at least one (the contract on
 * {@link io.deephaven.engine.rowset.RowSequence#getAverageRunLengthEstimate()}), and {@link RowSet#invert} cannot find
 * a position for a key that is not there.
 */
public class SortedRangesMinusToEmptyTest {

    private static WritableRowSet emptyByMinus() {
        try (final WritableRowSet sorted = sortedRangesOf(new long[] {10, 20}, new long[] {30, 40});
                final RowSet all = RowSetFactory.fromRange(10, 40)) {
            final WritableRowSet result = sorted.minus(all);
            assertTrue("fixture: the difference is empty", result.isEmpty());
            return result;
        }
    }

    @Test
    public void testEmptyDifferenceIsTheSharedEmptySet() {
        try (final WritableRowSet empty = emptyByMinus()) {
            assertSame(OrderedLongSet.EMPTY, ((WritableRowSetImpl) empty).getInnerSet());
        }
    }

    @Test
    public void testAverageRunLengthEstimateOfEmptyDifferenceIsAtLeastOne() {
        try (final WritableRowSet empty = emptyByMinus()) {
            final long estimate = empty.getAverageRunLengthEstimate();
            assertTrue("getAverageRunLengthEstimate()=" + estimate + " of an empty rowset must be >= 1", estimate >= 1);
        }
    }

    @Test
    public void testAverageRunLengthEstimateOfEmptySortedRangesIsAtLeastOne() {
        final long estimate = SortedRanges.makeEmpty().getAverageRunLengthEstimate();
        assertTrue("getAverageRunLengthEstimate()=" + estimate + " of empty sorted ranges must be >= 1", estimate >= 1);
    }

    @Test
    public void testInvertOnEmptyDifferenceMatchesEmptyRowSet() {
        // The same operation on RowSetFactory.empty() is the reference behavior.
        final String expected;
        try (final WritableRowSet empty = RowSetFactory.empty()) {
            expected = invertResult(empty);
        }
        final String actual;
        try (final WritableRowSet empty = emptyByMinus()) {
            actual = invertResult(empty);
        }
        if (!expected.equals(actual)) {
            fail("invert on an empty rowset: RowSetFactory.empty() gives " + expected
                    + " but the empty result of minus gives " + actual);
        }
    }

    private static String invertResult(final WritableRowSet empty) {
        try (final RowSet keys = RowSetFactory.fromKeys(10)) {
            try (final WritableRowSet positions = empty.invert(keys, Long.MAX_VALUE)) {
                assertEquals("an empty rowset holds no key, so no position can be reported", 0, positions.size());
                return positions.toString();
            } catch (IllegalArgumentException e) {
                return "IllegalArgumentException";
            }
        }
    }
}
