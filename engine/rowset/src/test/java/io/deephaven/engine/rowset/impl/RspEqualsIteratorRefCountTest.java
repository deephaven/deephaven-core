//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Two bitmaps that differ early are compared with iterators that never reach the end of either one; the references
 * those hold have to be given back anyway.
 */
public class RspEqualsIteratorRefCountTest {

    private static final long BLOCK_SIZE = 65536;
    private static final int REPETITIONS = 20;

    /** Blocks well past the first, so that a difference in the first leaves the rest of the spans unread. */
    private static RspBitmap manyBlockRsp(final long firstStart) {
        final RspBitmap rsp = RspBitmap.makeSingleRange(firstStart, firstStart + 4);
        for (int i = 2; i < 10; ++i) {
            rsp.addRangeUnsafeNoWriteCheck(i * BLOCK_SIZE, i * BLOCK_SIZE + 5);
        }
        rsp.finishMutations();
        return rsp;
    }

    @Test
    public void testComparingBitmapsThatDifferEarlyDoesNotRetainThem() {
        // Equal cardinalities, so the comparison gets past that check and on to walking both.
        final RspBitmap left = manyBlockRsp(5);
        final RspBitmap right = manyBlockRsp(6);
        assertEquals("cardinality", left.getCardinality(), right.getCardinality());
        final int leftCount = left.ixRefCount();
        final int rightCount = right.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            assertFalse(left.equals(right));
        }
        assertEquals("left reference count", leftCount, left.ixRefCount());
        assertEquals("right reference count", rightCount, right.ixRefCount());
    }
}
