//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.minusRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.renderRanges;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Removing one bitmap from another walks the argument's spans, jumping over the ones that cannot reach the receiver's
 * next span. The jump must land on every span that can reach it, including a full block span whose key lies below the
 * receiver's span but whose blocks extend into it, and one that lands on a span the receiver has already been split
 * around, where what is left of the split block is queued rather than present in the receiver's arrays. The result must
 * match the model for every mix of span kinds on both sides and in both roles.
 */
public class RspBitmapAndNotSparseReceiverTest {

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
        final String expected = render(minusRanges(rangesOf(receiver), rangesOf(arg)));
        try (final WritableRowSet rs = new WritableRowSetImpl(receiver.deepCopy());
                final WritableRowSet other = new WritableRowSetImpl(arg.deepCopy())) {
            rs.remove(other);
            rs.validate();
            assertEquals(what + " remove", expected, renderRanges(rs));
        }
        try (final WritableRowSet rs = new WritableRowSetImpl(receiver.deepCopy());
                final WritableRowSet other = new WritableRowSetImpl(arg.deepCopy());
                final WritableRowSet result = rs.minus(other)) {
            result.validate();
            assertEquals(what + " minus", expected, renderRanges(result));
        }
    }

    /** The receiver's two keys sit at either end of a long run of argument spans that have nothing to remove. */
    @Test
    public void testTwoKeysAgainstManySpans() {
        final RspBitmap arg = RspBitmap.makeEmpty();
        for (int i = 0; i < 3000; ++i) {
            arg.addUnsafeNoWriteCheck((long) i * BS + 7);
        }
        arg.finishMutations();
        final RspBitmap receiver = RspBitmap.makeEmpty();
        receiver.addUnsafeNoWriteCheck(7); // present in arg, so removed
        receiver.addUnsafeNoWriteCheck(1500L * BS + 8); // absent from arg, so kept
        receiver.addUnsafeNoWriteCheck(2999L * BS + 7); // present in arg, so removed
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

    /**
     * The jump lands on the receiver's own multi-block full block span while an earlier argument span has already split
     * it. The piece of the split block that survives is queued rather than sitting in the receiver's arrays, so a jump
     * that overshot it would leave keys behind.
     */
    @Test
    public void testJumpOntoAFullBlockSpanAlreadySplit() {
        final RspBitmap receiver = RspBitmap.makeEmpty();
        receiver.addUnsafeNoWriteCheck(7);
        receiver.addRangeUnsafeNoWriteCheck(10 * BS, 40 * BS - 1); // one span over blocks 10..39
        receiver.finishMutations();
        final RspBitmap arg = RspBitmap.makeEmpty();
        arg.addUnsafeNoWriteCheck(7);
        arg.addUnsafeNoWriteCheck(12 * BS + 5); // splits the span, leaving blocks 13..39 in place
        arg.addUnsafeNoWriteCheck(30 * BS + 9); // lands back inside what the split left behind
        // A stretch of spans past everything the receiver holds, so the walk has a long jump to make first.
        for (int i = 0; i < 200; ++i) {
            arg.addUnsafeNoWriteCheck(50L * BS + (long) i * BS + 3);
        }
        arg.finishMutations();
        check(receiver, arg, "split then jump");
        check(arg, receiver, "split then jump, reversed");
    }

    /** The argument runs past the receiver's last span, so the walk stops on the receiver rather than the argument. */
    @Test
    public void testArgumentExtendsPastTheReceiver() {
        final RspBitmap receiver = RspBitmap.makeEmpty();
        receiver.addUnsafeNoWriteCheck(5 * BS + 1);
        receiver.addUnsafeNoWriteCheck(6 * BS + 1);
        receiver.finishMutations();
        final RspBitmap arg = RspBitmap.makeEmpty();
        for (int i = 0; i < 500; ++i) {
            arg.addUnsafeNoWriteCheck((long) i * BS + 1);
        }
        arg.finishMutations();
        check(receiver, arg, "argument past the receiver");
        check(arg, receiver, "argument past the receiver, reversed");
    }

    @Test
    public void testRandomShapesInBothRoles() {
        final Random random = new Random(20260920);
        for (int trial = 0; trial < 300; ++trial) {
            final RspBitmap dense = randomBitmap(random, 60, 4);
            final RspBitmap sparse = randomBitmap(random, 60, 12);
            // Asserted rather than skipped: a change to the generator that quietly emptied a fixture would
            // otherwise turn every trial into a no-op that still passes.
            assertFalse("dense fixture is empty on trial " + trial, dense.isEmpty());
            assertFalse("sparse fixture is empty on trial " + trial, sparse.isEmpty());
            check(sparse, dense, "trial " + trial + " sparse receiver");
            check(dense, sparse, "trial " + trial + " dense receiver");
        }
    }

    /** Wide gaps on both sides, so the jump is exercised over long stretches rather than a span at a time. */
    @Test
    public void testRandomSparseShapesWithWideGaps() {
        final Random random = new Random(20260921);
        for (int trial = 0; trial < 200; ++trial) {
            final RspBitmap a = RspBitmap.makeEmpty();
            final RspBitmap b = RspBitmap.makeEmpty();
            long block = 0;
            for (int i = 0; i < 40; ++i) {
                block += 1 + random.nextInt(50);
                final long base = block * BS;
                if (random.nextBoolean()) {
                    a.addRangeUnsafeNoWriteCheck(base, base + random.nextInt(3) * BS + 10);
                }
                if (random.nextBoolean()) {
                    b.addRangeUnsafeNoWriteCheck(base + 5, base + random.nextInt(3) * BS + 20);
                }
            }
            a.finishMutations();
            b.finishMutations();
            assertFalse("a is empty on gap trial " + trial, a.isEmpty());
            assertFalse("b is empty on gap trial " + trial, b.isEmpty());
            check(a, b, "gap trial " + trial);
            check(b, a, "gap trial " + trial + " reversed");
        }
    }
}
