//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Shifting by zero hands back a reference to the set being shifted rather than a copy of it, since there is nothing to
 * move; that reference still has to be given back once the insert is done with it.
 */
public class InsertWithShiftRefCountTest {

    private static final long BLOCK_SIZE = 65536;
    private static final int REPETITIONS = 20;

    private static SortedRanges receiver() {
        return SortedRanges.makeSingleRange(5, 7);
    }

    @Test
    public void testInsertingABitmapUnshiftedDoesNotRetainIt() {
        final RspBitmap other = RspBitmap.makeSingleRange(1005, 1009);
        other.addRangeUnsafeNoWriteCheck(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + 5);
        other.finishMutations();
        final int steadyState = other.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            receiver().ixInsertWithShift(0, other);
        }
        assertEquals("reference count after " + REPETITIONS + " unshifted inserts", steadyState, other.ixRefCount());
    }

    @Test
    public void testInsertingSortedRangesUnshiftedDoesNotRetainThem() {
        SortedRanges other = SortedRanges.makeSingleRange(1000, 1009);
        other = other.addRange(2000, 2009);
        final int steadyState = other.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            receiver().ixInsertWithShift(0, other);
        }
        assertEquals("reference count after " + REPETITIONS + " unshifted inserts", steadyState, other.ixRefCount());
    }

    @Test
    public void testAShiftedInsertStillHoldsSteady() {
        final RspBitmap other = RspBitmap.makeSingleRange(1005, 1009);
        other.addRangeUnsafeNoWriteCheck(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + 5);
        other.finishMutations();
        final int steadyState = other.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            receiver().ixInsertWithShift(11, other);
        }
        assertEquals("reference count after " + REPETITIONS + " shifted inserts", steadyState, other.ixRefCount());
    }
}
