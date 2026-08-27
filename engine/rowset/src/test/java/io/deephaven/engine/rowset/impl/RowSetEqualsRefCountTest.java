//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Comparing two rowsets walks them with range iterators, which hold references to the sets they walk; those have to be
 * given back however the comparison ends.
 */
public class RowSetEqualsRefCountTest {

    private static final int REPETITIONS = 20;

    private static WritableRowSetImpl multiBlockRsp(final long extraKey) {
        final RspBitmap rsp = RspBitmap.makeSingleRange(5, 9);
        rsp.addRangeUnsafeNoWriteCheck(65536 * 3, 65536 * 3 + 10);
        rsp.addRangeUnsafeNoWriteCheck(65536 * 7 + 1, 65536 * 7 + 3);
        rsp.addUnsafeNoWriteCheck(extraKey);
        rsp.finishMutations();
        return new WritableRowSetImpl(rsp);
    }

    @Test
    public void testEqualRowSetsAreNotRetained() {
        try (final WritableRowSetImpl a = multiBlockRsp(100);
                final WritableRowSetImpl b = multiBlockRsp(100)) {
            final int aCount = a.refCount();
            final int bCount = b.refCount();
            for (int i = 0; i < REPETITIONS; ++i) {
                assertTrue(a.equals(b));
            }
            assertEquals("left reference count", aCount, a.refCount());
            assertEquals("right reference count", bCount, b.refCount());
        }
    }

    @Test
    public void testUnequalRowSetsAreNotRetained() {
        try (final WritableRowSetImpl a = multiBlockRsp(100);
                final WritableRowSetImpl b = multiBlockRsp(101)) {
            final int aCount = a.refCount();
            final int bCount = b.refCount();
            for (int i = 0; i < REPETITIONS; ++i) {
                // Same size, differing content: the comparison walks both and gives up part way through.
                assertFalse(a.equals(b));
            }
            assertEquals("left reference count", aCount, a.refCount());
            assertEquals("right reference count", bCount, b.refCount());
        }
    }
}
