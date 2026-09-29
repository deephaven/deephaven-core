//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Retaining a range out of a block dense enough to be held as a bitmap edits that container in place. Keys outside the
 * retained range have to disappear, not merely stop being counted.
 */
public class RspBitmapRetainBitmapContainerTest {

    private static final long BS = BLOCK_SIZE;

    private static List<Long> keysOf(final WritableRowSet rs) {
        final List<Long> out = new ArrayList<>();
        rs.forAllRowKeys(out::add);
        return out;
    }

    /** A block dense enough to be stored as a bitmap, plus a stray key well below the range we retain. */
    private static WritableRowSet denseBlockWithStrayKeyBelow() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendUnsafe(2);
        for (long v = 20000; v <= 36382; v += 2) {
            rb = rb.appendUnsafe(v);
        }
        rb = rb.appendRangeUnsafe(2 * BS + 5, 2 * BS + 9);
        rb.finishMutations();
        return new WritableRowSetImpl(rb);
    }

    @Test
    public void testRetainDropsKeysBelowTheRetainedRange() {
        RspBitmap keep = RspBitmap.makeEmpty();
        keep = keep.appendRangeUnsafe(20000, 36382);
        keep = keep.appendUnsafe(2 * BS + 7);
        keep.finishMutations();
        try (final WritableRowSet rs = denseBlockWithStrayKeyBelow();
                final WritableRowSet toRetain = new WritableRowSetImpl(keep)) {

            final List<Long> expected = new ArrayList<>();
            for (final long k : keysOf(rs)) {
                if ((k >= 20000 && k <= 36382) || k == 2 * BS + 7) {
                    expected.add(k);
                }
            }

            rs.retain(toRetain);
            assertEquals("keys after retain", expected, keysOf(rs));
            assertEquals("size must agree with the contents", expected.size(), rs.size());
        }
    }

    @Test
    public void testRetainDropsKeysAboveTheRetainedRange() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (long v = 1000; v <= 17000; v += 2) {
            rb = rb.appendUnsafe(v);
        }
        rb = rb.appendUnsafe(60000);
        rb.finishMutations();
        RspBitmap keep = RspBitmap.makeEmpty();
        keep = keep.appendRangeUnsafe(1000, 17000);
        keep.finishMutations();
        try (final WritableRowSet rs = new WritableRowSetImpl(rb);
                final WritableRowSet toRetain = new WritableRowSetImpl(keep)) {

            final List<Long> expected = new ArrayList<>();
            for (final long k : keysOf(rs)) {
                if (k >= 1000 && k <= 17000) {
                    expected.add(k);
                }
            }

            rs.retain(toRetain);
            assertEquals("keys after retain", expected, keysOf(rs));
            assertEquals("size must agree with the contents", expected.size(), rs.size());
        }
    }
}
