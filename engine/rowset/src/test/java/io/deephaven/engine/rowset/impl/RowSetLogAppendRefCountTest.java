//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Logging a rowset stops after a couple hundred ranges; the iterator abandoned at that point still holds a reference to
 * the rowset it was walking.
 */
public class RowSetLogAppendRefCountTest {

    private static final int REPETITIONS = 20;

    /** Enough separated ranges that logging gives up before running out of them. */
    private static WritableRowSetImpl manyRanges() {
        final RspBitmap rsp = RspBitmap.makeSingleRange(0, 0);
        for (int i = 1; i < 500; ++i) {
            rsp.addRangeUnsafeNoWriteCheck(4L * i, 4L * i + 1);
        }
        rsp.finishMutations();
        return new WritableRowSetImpl(rsp);
    }

    @Test
    public void testTruncatedLoggingDoesNotRetainTheRowSet() {
        try (final WritableRowSetImpl rowSet = manyRanges()) {
            final int steadyState = rowSet.refCount();
            for (int i = 0; i < REPETITIONS; ++i) {
                final LogOutputStringImpl logOutput = new LogOutputStringImpl();
                rowSet.append(logOutput);
                assertTrue("logging was truncated", logOutput.toString().contains("..."));
            }
            assertEquals("reference count after " + REPETITIONS + " truncated logs", steadyState, rowSet.refCount());
        }
    }
}
