//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.intersectRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.renderRanges;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Intersecting two bitmaps walks the argument's spans, jumping over the ones that cannot overlap the receiver's next
 * span. The jump must land on every span that can overlap, including a full block span whose key lies below the
 * receiver's span but whose blocks reach it, and the result must match the model for every mix of span kinds on both
 * sides and in both roles.
 */
public class RspBitmapRetainSparseReceiverTest {

    private static final long BS = BLOCK_SIZE;

    /** A bitmap over {@code blocks} blocks, each block independently empty, full, a lone key, or a few ranges. */
    private static RspBitmap randomBitmap(final Random random, final int blocks, final int fullEvery) {
        final RspBitmap rb = RspBitmap.makeEmpty();
        int b = 0;
        while (b < blocks) {
            final long base = (long) b * BS;
            final int kind = random.nextInt(fullEvery);
            if (kind == 0) {
                // A run of whole blocks, which becomes one multi-block full block span.
                final int run = 1 + random.nextInt(4);
                rb.addRangeUnsafeNoWriteCheck(base, base + run * BS - 1);
                b += run;
                continue;
            }
            if (kind == 1) {
                rb.addUnsafeNoWriteCheck(base + random.nextInt(BLOCK_SIZE));
            } else if (kind == 2) {
                for (int j = 0; j < 4; ++j) {
                    final int s = random.nextInt(BLOCK_SIZE - 20);
                    rb.addRangeUnsafeNoWriteCheck(base + s, base + s + random.nextInt(20));
                }
            }
            // otherwise the block stays empty
            ++b;
        }
        rb.finishMutations();
        return rb;
    }

    private static void check(final RspBitmap receiver, final RspBitmap arg, final String what) {
        // Compared as ranges: the bitmaps hold whole blocks, far too many keys to enumerate per trial.
        final String expected = render(intersectRanges(rangesOf(receiver), rangesOf(arg)));
        try (final WritableRowSet rs = new WritableRowSetImpl(receiver.deepCopy());
                final WritableRowSet other = new WritableRowSetImpl(arg.deepCopy())) {
            rs.retain(other);
            rs.validate();
            assertEquals(what + " retain", expected, renderRanges(rs));
        }
        try (final WritableRowSet rs = new WritableRowSetImpl(receiver.deepCopy());
                final WritableRowSet other = new WritableRowSetImpl(arg.deepCopy());
                final WritableRowSet result = rs.intersect(other)) {
            result.validate();
            assertEquals(what + " intersect", expected, renderRanges(result));
        }
    }

    @Test
    public void testTwoKeysAgainstManySpans() {
        final RspBitmap arg = RspBitmap.makeEmpty();
        for (int i = 0; i < 3000; ++i) {
            arg.addUnsafeNoWriteCheck((long) i * BS + 7);
        }
        arg.finishMutations();
        final RspBitmap receiver = RspBitmap.makeEmpty();
        receiver.addUnsafeNoWriteCheck(7);
        receiver.addUnsafeNoWriteCheck(1500L * BS + 8); // absent from arg
        receiver.addUnsafeNoWriteCheck(2999L * BS + 7);
        receiver.finishMutations();
        check(receiver, arg, "two keys");
        check(arg, receiver, "two keys, reversed");
    }

    /** The argument's full block span starts well below the receiver's span but reaches it. */
    @Test
    public void testFullBlockSpanReachingTheReceiverFromBelow() {
        final RspBitmap arg = RspBitmap.makeEmpty();
        arg.addUnsafeNoWriteCheck(7);
        arg.addRangeUnsafeNoWriteCheck(10 * BS, 20 * BS - 1); // blocks 10..19
        arg.addUnsafeNoWriteCheck(30 * BS + 7);
        arg.finishMutations();
        final RspBitmap receiver = RspBitmap.makeEmpty();
        receiver.addUnsafeNoWriteCheck(15 * BS + 100); // inside the full span
        receiver.addRangeUnsafeNoWriteCheck(19 * BS, 21 * BS - 1); // straddles its end
        receiver.addUnsafeNoWriteCheck(40 * BS + 7);
        receiver.finishMutations();
        check(receiver, arg, "reaching full span");
        check(arg, receiver, "reaching full span, reversed");
    }

    @Test
    public void testRandomShapesInBothRoles() {
        final Random random = new Random(20260904);
        for (int trial = 0; trial < 300; ++trial) {
            final RspBitmap dense = randomBitmap(random, 60, 4);
            final RspBitmap sparse = randomBitmap(random, 60, 12);
            if (dense.isEmpty() || sparse.isEmpty()) {
                continue;
            }
            check(sparse, dense, "trial " + trial + " sparse receiver");
            check(dense, sparse, "trial " + trial + " dense receiver");
        }
    }
}
