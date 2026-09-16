//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Row keys are non-negative longs. A shift that would carry a key below zero or past {@code Long.MAX_VALUE} must be
 * rejected, whatever the backing implementation and whether or not the shift is a whole number of blocks, and the
 * rejected row set must be left as it was.
 */
public class RowSetShiftOutOfRangeTest {

    private static final long FIRST = 5;
    private static final long LAST = Long.MAX_VALUE - 10;

    /** Each supplier builds a fresh row set with first key {@link #FIRST} in a different backing implementation. */
    private static Supplier<?>[] lowRowSets() {
        return new Supplier<?>[] {
                () -> new WritableRowSetImpl(SingleRange.make(FIRST, FIRST + 4)),
                () -> new WritableRowSetImpl(SortedRanges.makeSingleRange(FIRST, FIRST + 4).add(3 * BLOCK_SIZE)),
                () -> {
                    final RspBitmap rsp = RspBitmap.makeSingleRange(FIRST, FIRST + 4);
                    rsp.addRangeUnsafeNoWriteCheck(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 5);
                    rsp.finishMutations();
                    return new WritableRowSetImpl(rsp);
                },
        };
    }

    /** As {@link #lowRowSets}, with last key {@link #LAST}. */
    private static Supplier<?>[] highRowSets() {
        return new Supplier<?>[] {
                () -> new WritableRowSetImpl(SingleRange.make(LAST - 4, LAST)),
                () -> new WritableRowSetImpl(SortedRanges.makeSingleRange(LAST - 4, LAST).add(LAST - 3 * BLOCK_SIZE)),
                () -> {
                    final RspBitmap rsp = RspBitmap.makeSingleRange(LAST - 2 * BLOCK_SIZE, LAST - 2 * BLOCK_SIZE + 5);
                    rsp.addRangeUnsafeNoWriteCheck(LAST - 4, LAST);
                    rsp.finishMutations();
                    return new WritableRowSetImpl(rsp);
                },
        };
    }

    private static void assertRejected(final Supplier<?>[] rowSets, final long shift) {
        for (final Supplier<?> supplier : rowSets) {
            try (final WritableRowSet rowSet = (WritableRowSet) supplier.get()) {
                final String name = ((WritableRowSetImpl) rowSet).getInnerSet().getClass().getSimpleName()
                        + " shift " + shift;
                final long first = rowSet.firstRowKey();
                final long last = rowSet.lastRowKey();
                final long size = rowSet.size();
                try {
                    rowSet.shift(shift).close();
                    fail(name + " on new was not rejected");
                } catch (IllegalArgumentException expected) {
                }
                try {
                    rowSet.shiftInPlace(shift);
                    fail(name + " in place was not rejected");
                } catch (IllegalArgumentException expected) {
                }
                rowSet.validate();
                assertEquals(name + " first", first, rowSet.firstRowKey());
                assertEquals(name + " last", last, rowSet.lastRowKey());
                assertEquals(name + " size", size, rowSet.size());
            }
        }
    }

    @Test
    public void testShiftBelowZeroIsRejected() {
        // One more than the distance to zero, both with and without a whole number of blocks in it.
        assertRejected(lowRowSets(), -(FIRST + 1));
        assertRejected(lowRowSets(), -(FIRST + 1) - 3 * BLOCK_SIZE);
    }

    @Test
    public void testShiftPastMaxValueIsRejected() {
        assertRejected(highRowSets(), Long.MAX_VALUE - LAST + 1);
        assertRejected(highRowSets(), Long.MAX_VALUE - LAST + 1 + 3 * BLOCK_SIZE);
    }

    @Test
    public void testShiftToTheEdgesIsAccepted() {
        for (final Supplier<?> supplier : lowRowSets()) {
            try (final WritableRowSet rowSet = (WritableRowSet) supplier.get();
                    final WritableRowSet shifted = rowSet.shift(-FIRST)) {
                shifted.validate();
                assertEquals(0, shifted.firstRowKey());
                assertEquals(rowSet.size(), shifted.size());
            }
        }
        for (final Supplier<?> supplier : highRowSets()) {
            try (final WritableRowSet rowSet = (WritableRowSet) supplier.get();
                    final WritableRowSet shifted = rowSet.shift(Long.MAX_VALUE - LAST)) {
                shifted.validate();
                assertEquals(Long.MAX_VALUE, shifted.lastRowKey());
                assertEquals(rowSet.size(), shifted.size());
            }
        }
    }
}
