//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.assertBackedBy;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.keysOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Row keys are non-negative, so a key range that starts below zero asks about the same keys as one that starts at zero.
 * The RSP backing compares block keys unsigned, which would read a negative start as lying past every key; a packed
 * SortedRanges would read it as an offset-relative key inside the set. Both must answer as the plain backings do.
 */
public class RowSetNegativeStartKeyTest {

    private static WritableRowSet packedSortedRangesOf(final long[]... ranges) {
        final WritableRowSet rs = sortedRangesOf(ranges);
        rs.compact();
        assertBackedBy("compacted sorted ranges", rs, "Short");
        return rs;
    }

    /** The same keys in every backing that can hold them; the single range holds a contiguous subset. */
    private static Supplier<?>[] rowSets() {
        return new Supplier<?>[] {
                () -> singleRangeOf(100, 300),
                () -> sortedRangesOf(new long[] {100, 100}, new long[] {150, 250}, new long[] {300, 300}),
                () -> packedSortedRangesOf(new long[] {100, 100}, new long[] {150, 250}, new long[] {300, 300}),
                () -> rspOf(new long[] {100, 100}, new long[] {150, 250}, new long[] {300, 300}),
        };
    }

    private static String nameOf(final WritableRowSet rs) {
        return ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
    }

    @Test
    public void testQueriesWithANegativeStart() {
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get()) {
                final String name = nameOf(rs);
                final List<Long> all = keysOf(rs);
                assertEquals(name + " find", -1, rs.find(-1));
                assertFalse(name + " containsRange", rs.containsRange(-1, 100));
                assertTrue(name + " overlapsRange", rs.overlapsRange(-1, 100));
                assertFalse(name + " overlapsRange below zero", rs.overlapsRange(-10, -1));
                try (final WritableRowSet sub = rs.subSetByKeyRange(-1, Long.MAX_VALUE)) {
                    assertEquals(name + " subSetByKeyRange", all, keysOf(sub));
                }
                try (final WritableRowSet sub = rs.subSetByKeyRange(-10, -1)) {
                    assertTrue(name + " subSetByKeyRange below zero", sub.isEmpty());
                }
                try (final RowSequence seq = rs.getRowSequenceByKeyRange(-1, Long.MAX_VALUE)) {
                    assertEquals(name + " getRowSequenceByKeyRange", all, keysOf(seq));
                }
                try (final RowSequence seq = rs.getRowSequenceByKeyRange(-10, -1)) {
                    assertTrue(name + " getRowSequenceByKeyRange below zero", seq.isEmpty());
                }
                try (final RowSequence.Iterator it = rs.getRowSequenceIterator()) {
                    assertTrue(name + " advance(-1) keeps going", it.advance(-1));
                    assertEquals(name + " advance(-1) is a no-op", rs.firstRowKey(), it.peekNextKey());
                }
            }
        }
    }

    @Test
    public void testMutationsWithANegativeStart() {
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get()) {
                final String name = nameOf(rs);
                try (final WritableRowSet removed = rs.copy();
                        final WritableRowSet expected = rs.minus(RowSetFactory.fromRange(0, 120))) {
                    removed.removeRange(-1, 120);
                    removed.validate();
                    assertEquals(name + " removeRange", keysOf(expected), keysOf(removed));
                }
                try (final WritableRowSet removed = rs.copy()) {
                    removed.removeRange(-10, -1);
                    removed.validate();
                    assertEquals(name + " removeRange below zero", keysOf(rs), keysOf(removed));
                }
                try (final WritableRowSet retained = rs.copy();
                        final WritableRowSet expected = rs.intersect(RowSetFactory.fromRange(0, 120))) {
                    retained.retainRange(-1, 120);
                    retained.validate();
                    assertEquals(name + " retainRange", keysOf(expected), keysOf(retained));
                }
                try (final WritableRowSet retained = rs.copy()) {
                    retained.retainRange(-10, -1);
                    retained.validate();
                    assertTrue(name + " retainRange below zero", retained.isEmpty());
                }
            }
        }
    }

    @Test
    public void testPositiveShiftedRowSequenceBelowTheShift() {
        // A positive shift asks the wrapped sequence about keys below zero whenever the requested start lies below
        // the shift amount.
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get()) {
                final String name = nameOf(rs);
                final RowSequence shifted = ShiftedRowSequence.wrap(rs, 1000);
                try (final RowSequence seq = shifted.getRowSequenceByKeyRange(5, 1100)) {
                    assertEquals(name + " shifted getRowSequenceByKeyRange", List.of(1100L), keysOf(seq));
                }
                try (final RowSequence.Iterator it = shifted.getRowSequenceIterator()) {
                    assertTrue(name + " shifted advance(0) keeps going", it.advance(0));
                    assertEquals(name + " shifted advance(0) is a no-op", 1100L, it.peekNextKey());
                }
            }
        }
    }

    @Test
    public void testUnapplyingAShiftWhoseWindowReachesBelowZero() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(0, 300, -3);
        final RowSetShiftData shiftData = builder.build();
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get();
                    final WritableRowSet expected = rs.copy();
                    final WritableRowSet postShift = rs.shift(-3)) {
                final String name = nameOf(rs);
                shiftData.unapply(postShift);
                postShift.validate();
                assertEquals(name + " unapply", keysOf(expected), keysOf(postShift));
            }
            try (final WritableRowSet rs = (WritableRowSet) supplier.get();
                    final WritableRowSet expected = rs.copy();
                    final WritableRowSet postShift = rs.shift(-3)) {
                final String name = nameOf(rs);
                shiftData.unapply(postShift, 0);
                postShift.validate();
                assertEquals(name + " unapply with offset", keysOf(expected), keysOf(postShift));
            }
        }
    }

    /** The same shape as {@link #rowSets()}, at the top of the key space. */
    private static Supplier<?>[] highRowSets() {
        final long m = Long.MAX_VALUE;
        return new Supplier<?>[] {
                () -> singleRangeOf(m - 300, m - 100),
                () -> sortedRangesOf(new long[] {m - 300, m - 300}, new long[] {m - 250, m - 150},
                        new long[] {m - 100, m - 100}),
                () -> rspOf(new long[] {m - 300, m - 300}, new long[] {m - 250, m - 150},
                        new long[] {m - 100, m - 100}),
        };
    }

    /**
     * The mirror image at the top of the key space: a positive shift whose window ends at Long.MAX_VALUE has a
     * post-shift window that wraps, and only the part past the maximum is empty.
     */
    @Test
    public void testUnapplyingAShiftWhoseWindowReachesPastTheMaximum() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(Long.MAX_VALUE - 400, Long.MAX_VALUE, 3);
        final RowSetShiftData shiftData = builder.build();
        for (final Supplier<?> supplier : highRowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get();
                    final WritableRowSet expected = rs.copy();
                    final WritableRowSet postShift = rs.shift(3)) {
                final String name = nameOf(rs);
                shiftData.unapply(postShift);
                postShift.validate();
                assertEquals(name + " unapply past max", keysOf(expected), keysOf(postShift));
            }
            try (final WritableRowSet rs = (WritableRowSet) supplier.get();
                    final WritableRowSet expected = rs.copy();
                    final WritableRowSet postShift = rs.shift(3)) {
                final String name = nameOf(rs);
                shiftData.unapply(postShift, 0);
                postShift.validate();
                assertEquals(name + " unapply past max with offset", keysOf(expected), keysOf(postShift));
            }
            try (final WritableRowSet rs = (WritableRowSet) supplier.get();
                    final WritableRowSet postShift = rs.shift(3);
                    final WritableRowSet untouched = postShift.copy()) {
                // An offset that carries the whole window past the maximum, so far that the combined shift itself
                // wraps: nothing in the key space is affected.
                final String name = nameOf(rs);
                shiftData.unapply(postShift, Long.MAX_VALUE);
                postShift.validate();
                assertEquals(name + " unapply with an overflowing offset", keysOf(untouched), keysOf(postShift));
            }
        }
    }
}
