//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Deciding a bitmap is not a subset stops at the first key found outside the other set; the iterator walking that other
 * set still holds a reference to it.
 */
public class RspSubsetOfIteratorRefCountTest {

    private static final long BLOCK_SIZE = 65536;
    private static final int REPETITIONS = 20;

    /** A first range, then a gap, then many more ranges that go unread once the answer is known. */
    private static SortedRanges withAnEarlyGap() {
        SortedRanges sr = SortedRanges.makeSingleRange(0, 100);
        for (int i = 3; i < 12; ++i) {
            sr = sr.addRange(i * BLOCK_SIZE, i * BLOCK_SIZE + 50);
        }
        return sr;
    }

    @Test
    public void testDecidingNotASubsetDoesNotRetainTheOtherSet() {
        final SortedRanges other = withAnEarlyGap();
        // Within other's first and last keys, but with a key sitting in its first gap.
        final RspBitmap subject = RspBitmap.makeSingleRange(5, 9);
        subject.addRangeUnsafeNoWriteCheck(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 1);
        subject.addRangeUnsafeNoWriteCheck(11 * BLOCK_SIZE, 11 * BLOCK_SIZE + 1);
        subject.finishMutations();
        final int steadyState = other.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            assertFalse(subject.ixSubsetOf(other));
        }
        assertEquals("other reference count", steadyState, other.ixRefCount());
    }
}
