//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Inverting stops once the requested maximum position is reached; the iterator walking the keys is left holding a
 * reference to the set it came from.
 */
public class InvertOnNewIteratorRefCountTest {

    private static final long BLOCK_SIZE = 65536;
    private static final int REPETITIONS = 20;

    /** More blocks than the maximum position below will reach. */
    private static RspBitmap manyBlockKeys() {
        final RspBitmap keys = RspBitmap.makeSingleRange(5, 9);
        for (int i = 2; i < 10; ++i) {
            keys.addRangeUnsafeNoWriteCheck(i * BLOCK_SIZE, i * BLOCK_SIZE + 5);
        }
        keys.finishMutations();
        return keys;
    }

    @Test
    public void testSingleRangeInvertTruncatedByMaxPositionDoesNotRetainTheKeys() {
        final RspBitmap keys = manyBlockKeys();
        final SingleRange subject = SingleRange.make(0, 10 * BLOCK_SIZE);
        final int steadyState = keys.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            subject.ixInvertOnNew(keys, 2);
        }
        assertEquals("keys reference count", steadyState, keys.ixRefCount());
    }

    @Test
    public void testSortedRangesInvertTruncatedByMaxPositionDoesNotRetainTheKeys() {
        final RspBitmap keys = manyBlockKeys();
        SortedRanges subject = SortedRanges.makeSingleRange(0, 100);
        for (int i = 2; i < 10; ++i) {
            subject = subject.addRange(i * BLOCK_SIZE, i * BLOCK_SIZE + 50);
        }
        final int steadyState = keys.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            subject.ixInvertOnNew(keys, 2);
        }
        assertEquals("keys reference count", steadyState, keys.ixRefCount());
    }
}
