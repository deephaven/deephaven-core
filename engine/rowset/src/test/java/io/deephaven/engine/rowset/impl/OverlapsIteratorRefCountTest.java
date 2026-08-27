//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * An overlap test stops walking at the first overlap it finds; the iterator abandoned there still holds a reference to
 * the rowset it was walking.
 */
public class OverlapsIteratorRefCountTest {

    private static final long BLOCK_SIZE = 65536;
    private static final int REPETITIONS = 20;

    /** Several blocks, so that stopping at the first one leaves the rest of the spans unread. */
    private static RspBitmap manyBlockRsp() {
        final RspBitmap rsp = RspBitmap.makeSingleRange(5, 9);
        for (int i = 2; i < 10; ++i) {
            rsp.addRangeUnsafeNoWriteCheck(i * BLOCK_SIZE, i * BLOCK_SIZE + 5);
        }
        rsp.finishMutations();
        return rsp;
    }

    private static SortedRanges manyRangeSortedRanges() {
        SortedRanges sr = SortedRanges.makeSingleRange(5, 9);
        for (int i = 2; i < 10; ++i) {
            sr = sr.addRange(i * BLOCK_SIZE, i * BLOCK_SIZE + 5);
        }
        return sr;
    }

    @Test
    public void testRspOverlapsSortedRangesDoesNotRetainTheReceiver() {
        // The iterator here is taken off the receiver.
        final RspBitmap receiver = manyBlockRsp();
        final SortedRanges argument = manyRangeSortedRanges();
        final int steadyState = receiver.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            assertTrue(receiver.ixOverlaps(argument));
        }
        assertEquals("receiver reference count", steadyState, receiver.ixRefCount());
    }

    @Test
    public void testSortedRangesOverlapsRspDoesNotRetainTheArgument() {
        // And here off the argument.
        final RspBitmap argument = manyBlockRsp();
        final SortedRanges receiver = manyRangeSortedRanges();
        final int steadyState = argument.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            assertTrue(receiver.ixOverlaps(argument));
        }
        assertEquals("argument reference count", steadyState, argument.ixRefCount());
    }
}
