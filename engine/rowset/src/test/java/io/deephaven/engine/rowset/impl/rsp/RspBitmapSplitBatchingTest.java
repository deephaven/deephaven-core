//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Removing part of a full block span splits it into up to three spans. These cover the cases a batched split has to get
 * right, above all several removals landing inside one original span: the pieces a split leaves behind are live data
 * that later removals have to be able to find.
 */
public class RspBitmapSplitBatchingTest {

    private static final long BS = BLOCK_SIZE;

    private static TreeSet<Long> keysOf(final RspBitmap rb) {
        final TreeSet<Long> keys = new TreeSet<>();
        rb.forEachLong(v -> {
            keys.add(v);
            return true;
        });
        return keys;
    }

    private static void assertKeysAre(final TreeSet<Long> expected, final RspBitmap actual) {
        final List<Long> got = new ArrayList<>();
        actual.forEachLong(v -> {
            got.add(v);
            return true;
        });
        assertEquals(new ArrayList<>(expected), got);
    }

    /** andNot the argument out of the receiver, checking the key set and our structural invariants. */
    private static void checkAndNot(final RspBitmap receiver, final RspBitmap arg) {
        final TreeSet<Long> expected = keysOf(receiver);
        expected.removeAll(keysOf(arg));
        final RspBitmap w = receiver.writeCheck();
        w.andNotEqualsUnsafeNoWriteCheck(arg);
        w.finishMutations();
        w.validate("after andNot");
        assertKeysAre(expected, w);
    }

    private static void checkRemoveRanges(final RspBitmap receiver, final WritableRowSet arg) {
        final TreeSet<Long> expected = keysOf(receiver);
        arg.forAllRowKeys(expected::remove);
        final RspBitmap w = receiver.writeCheck();
        w.removeRangesUnsafeNoWriteCheck(arg.rangeIterator());
        w.finishMutations();
        w.validate("after removeRanges");
        assertKeysAre(expected, w);
    }

    private static RspBitmap fullBlockSpans(final int count, final int blocksEach, final int stride) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < count; ++i) {
            final long base = (long) i * stride * BS;
            rb = rb.appendRangeUnsafe(base, base + (long) blocksEach * BS - 1);
        }
        rb.finishMutations();
        return rb;
    }

    /** One removal in the middle of a multi-block full block span: the three-way split. */
    @Test
    public void testSingleSplitInMiddle() {
        final RspBitmap recv = fullBlockSpans(4, 3, 4);
        RspBitmap arg = RspBitmap.makeEmpty();
        arg = arg.appendUnsafe(1 * BS + 100);
        arg.finishMutations();
        checkAndNot(recv, arg);
    }

    /**
     * Two removals inside the SAME original full block span. The second has to find the piece the first left behind; if
     * a batched split deferred that piece, the second removal would be applied to a stale span.
     */
    @Test
    public void testTwoRemovalsInOneSpan() {
        final RspBitmap recv = fullBlockSpans(3, 5, 8); // 5-block spans at blocks 0, 8, 16
        RspBitmap arg = RspBitmap.makeEmpty();
        arg = arg.appendUnsafe(1 * BS + 100); // block 1 of the first span
        arg = arg.appendUnsafe(3 * BS + 200); // block 3 of the same span
        arg.finishMutations();
        checkAndNot(recv, arg);
    }

    /** Three removals in one span, including its first and last blocks. */
    @Test
    public void testThreeRemovalsSpanningEnds() {
        final RspBitmap recv = fullBlockSpans(2, 6, 10);
        RspBitmap arg = RspBitmap.makeEmpty();
        arg = arg.appendUnsafe(0 * BS + 5); // first block
        arg = arg.appendUnsafe(2 * BS + 7); // middle
        arg = arg.appendUnsafe(5 * BS + 9); // last block
        arg.finishMutations();
        checkAndNot(recv, arg);
    }

    /** A removal that empties one block of a span entirely, next to one that only partly empties another. */
    @Test
    public void testWholeBlockAndPartialRemovalsInOneSpan() {
        final RspBitmap recv = fullBlockSpans(2, 4, 8);
        RspBitmap arg = RspBitmap.makeEmpty();
        arg = arg.appendRangeUnsafe(1 * BS, 2 * BS - 1); // all of block 1
        arg = arg.appendUnsafe(3 * BS + 11); // part of block 3
        arg.finishMutations();
        checkAndNot(recv, arg);
    }

    /** The same shape through removeRanges rather than andNot. */
    @Test
    public void testTwoRemoveRangesInOneSpan() {
        final RspBitmap recv = fullBlockSpans(3, 5, 8);
        final WritableRowSet arg = RowSetFactory.empty();
        arg.insertRange(1 * BS + 100, 1 * BS + 200);
        arg.insertRange(3 * BS + 300, 3 * BS + 400);
        checkRemoveRanges(recv, arg);
    }

    /** Randomized: full block spans of assorted lengths, removals clustered so several share a span. */
    @Test
    public void testRandomClusteredRemovals() {
        final Random rand = new Random(23407);
        for (int trial = 0; trial < 300; ++trial) {
            RspBitmap recv = RspBitmap.makeEmpty();
            long block = 0;
            final List<long[]> spans = new ArrayList<>();
            for (int i = 0; i < 12; ++i) {
                final int blocks = 1 + rand.nextInt(6);
                final long base = block * BS;
                recv = recv.appendRangeUnsafe(base, base + (long) blocks * BS - 1);
                spans.add(new long[] {block, blocks});
                block += blocks + 1 + rand.nextInt(3);
            }
            recv.finishMutations();

            final TreeSet<Long> toRemove = new TreeSet<>();
            for (final long[] sp : spans) {
                final int removals = rand.nextInt(4);
                for (int r = 0; r < removals; ++r) {
                    final long b = sp[0] + rand.nextInt((int) sp[1]);
                    if (rand.nextInt(4) == 0) {
                        for (long v = b * BS; v < (b + 1) * BS; ++v) {
                            toRemove.add(v); // an entire block
                        }
                    } else {
                        toRemove.add(b * BS + rand.nextInt((int) BS));
                    }
                }
            }
            if (toRemove.isEmpty()) {
                continue;
            }
            RspBitmap arg = RspBitmap.makeEmpty();
            for (final long v : toRemove) {
                arg = arg.appendUnsafe(v);
            }
            arg.finishMutations();
            checkAndNot(recv, arg);
        }
    }
}
