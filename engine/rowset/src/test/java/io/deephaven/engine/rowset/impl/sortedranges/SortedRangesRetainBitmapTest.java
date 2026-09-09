//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.assertBackedBy;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.intersectRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.renderRanges;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Retaining a bitmap's keys in a writable SortedRanges takes the same key-advancing merge as {@code intersect}, rather
 * than converting to a bitmap and walking every span of the argument. The result must match the model whether or not it
 * still fits in a SortedRanges.
 */
public class SortedRangesRetainBitmapTest {

    private static void checkRetain(final SortedRanges receiver, final RspBitmap arg, final String what) {
        try (final WritableRowSet rs = new WritableRowSetImpl(receiver.deepCopy());
                final WritableRowSet other = new WritableRowSetImpl(arg.deepCopy())) {
            // Compared as ranges: the receiver may hold thousands of whole blocks, far too many keys to enumerate.
            final String expected = render(intersectRanges(rangesOf(rs), rangesOf(other)));
            rs.retain(other);
            rs.validate();
            assertEquals(what, expected, renderRanges(rs));
        }
    }

    @Test
    public void testFewKeysAgainstManySpans() {
        final RspBitmap arg = RspBitmap.makeEmpty();
        for (int i = 0; i < 5000; ++i) {
            arg.addUnsafeNoWriteCheck((long) i * BLOCK_SIZE + 7);
        }
        arg.finishMutations();
        final SortedRanges receiver = SortedRanges.makeSingleRange(7, 7)
                .add(2500L * BLOCK_SIZE + 7) // present
                .add(2500L * BLOCK_SIZE + 8) // absent
                .add(4999L * BLOCK_SIZE + 7);
        checkRetain(receiver, arg, "few keys");
        try (final WritableRowSet rs = new WritableRowSetImpl(receiver.deepCopy());
                final WritableRowSet other = new WritableRowSetImpl(arg.deepCopy())) {
            rs.retain(other);
            assertBackedBy("a small result", rs, "SortedRanges");
        }
    }

    /** A range covering many of the argument's blocks: the result has too many ranges for a SortedRanges. */
    @Test
    public void testResultTooLargeForSortedRanges() {
        final RspBitmap arg = RspBitmap.makeEmpty();
        for (int i = 0; i < 20000; ++i) {
            arg.addUnsafeNoWriteCheck((long) i * BLOCK_SIZE + 7);
        }
        arg.finishMutations();
        final SortedRanges receiver = SortedRanges.makeSingleRange(0, 20000L * BLOCK_SIZE);
        checkRetain(receiver, arg, "wide range");
        try (final WritableRowSet rs = new WritableRowSetImpl(receiver.deepCopy());
                final WritableRowSet other = new WritableRowSetImpl(arg.deepCopy())) {
            rs.retain(other);
            assertBackedBy("a large result", rs, "RspBitmap");
        }
    }

    @Test
    public void testRandomShapes() {
        final Random random = new Random(20260903);
        for (int trial = 0; trial < 200; ++trial) {
            final RspBitmap arg = RspBitmap.makeEmpty();
            final int blocks = 1 + random.nextInt(40);
            for (int b = 0; b < blocks; ++b) {
                final long base = (long) b * BLOCK_SIZE;
                switch (random.nextInt(3)) {
                    case 0:
                        arg.addRangeUnsafeNoWriteCheck(base, base + BLOCK_SIZE - 1); // full block
                        break;
                    case 1:
                        arg.addUnsafeNoWriteCheck(base + random.nextInt(BLOCK_SIZE)); // singleton
                        break;
                    default:
                        for (int j = 0; j < 5; ++j) {
                            final int s = random.nextInt(BLOCK_SIZE - 10);
                            arg.addRangeUnsafeNoWriteCheck(base + s, base + s + random.nextInt(10));
                        }
                }
            }
            arg.finishMutations();
            SortedRanges receiver = null;
            final int ranges = 1 + random.nextInt(12);
            long key = random.nextInt(BLOCK_SIZE);
            for (int r = 0; r < ranges; ++r) {
                final long end = key + random.nextInt(3 * BLOCK_SIZE);
                receiver = receiver == null ? SortedRanges.makeSingleRange(key, end) : receiver.addRange(key, end);
                key = end + 2 + random.nextInt(2 * BLOCK_SIZE);
            }
            checkRetain(receiver, arg, "trial " + trial);
        }
    }
}
