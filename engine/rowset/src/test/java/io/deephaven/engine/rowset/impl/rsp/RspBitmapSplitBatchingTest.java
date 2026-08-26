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

    private static List<long[]> rangesOf(final RspBitmap rb) {
        final List<long[]> out = new ArrayList<>();
        rb.forEachLongRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    private static List<long[]> rangesOf(final RowSet rs) {
        final List<long[]> out = new ArrayList<>();
        rs.forEachRowKeyRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    /**
     * {@code from} minus {@code remove}, by interval arithmetic on the ranges themselves. Deliberately not built from
     * the key sets: a block is 65536 keys, so enumerating them would cost millions of boxed longs per trial, and this
     * stays independent of the code under test either way. Both arguments must be ascending and disjoint.
     */
    private static List<long[]> minusRanges(final List<long[]> from, final List<long[]> remove) {
        final List<long[]> out = new ArrayList<>();
        for (final long[] r : from) {
            long start = r[0];
            final long end = r[1];
            for (final long[] x : remove) {
                if (x[1] < start) {
                    continue;
                }
                if (x[0] > end) {
                    break;
                }
                if (x[0] > start) {
                    out.add(new long[] {start, Math.min(x[0] - 1, end)});
                }
                start = Math.max(start, x[1] + 1);
                if (start > end) {
                    break;
                }
            }
            if (start <= end) {
                out.add(new long[] {start, end});
            }
        }
        return out;
    }

    private static String render(final List<long[]> ranges) {
        final StringBuilder sb = new StringBuilder();
        for (final long[] r : ranges) {
            sb.append(r[0]).append('-').append(r[1]).append(' ');
        }
        return sb.toString();
    }

    private static void assertRangesAre(final List<long[]> expected, final RspBitmap actual) {
        assertEquals(render(expected), render(rangesOf(actual)));
        long card = 0;
        for (final long[] r : expected) {
            card += r[1] - r[0] + 1;
        }
        assertEquals("cardinality", card, actual.getCardinality());
    }

    /** andNot the argument out of the receiver, checking the ranges and our structural invariants. */
    private static void checkAndNot(final RspBitmap receiver, final RspBitmap arg) {
        final List<long[]> expected = minusRanges(rangesOf(receiver), rangesOf(arg));
        final RspBitmap w = receiver.writeCheck();
        w.andNotEqualsUnsafeNoWriteCheck(arg);
        w.finishMutations();
        w.validate("after andNot");
        assertRangesAre(expected, w);
    }

    private static void checkRemoveRanges(final RspBitmap receiver, final WritableRowSet arg) {
        final List<long[]> expected = minusRanges(rangesOf(receiver), rangesOf(arg));
        final RspBitmap w = receiver.writeCheck();
        w.removeRangesUnsafeNoWriteCheck(arg.rangeIterator());
        w.finishMutations();
        w.validate("after removeRanges");
        assertRangesAre(expected, w);
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

    /**
     * Several ranges inside ONE block of a full block span. A range iterator can revisit a block, unlike andNot's
     * per-block spans, so what is left of that block has to be findable again for each range after the first.
     */
    @Test
    public void testSeveralRemoveRangesInOneBlock() {
        final RspBitmap recv = fullBlockSpans(1, 5, 8); // one 5-block span
        final WritableRowSet arg = RowSetFactory.empty();
        final long base = 3 * BS; // all inside block 3
        arg.insertRange(base + 0, base + 1);
        arg.insertRange(base + 12, base + 13);
        arg.insertRange(base + 16, base + 16);
        arg.insertRange(base + 22, base + 22);
        arg.insertRange(base + 25, base + 27);
        checkRemoveRanges(recv, arg);
    }

    /** Ranges revisiting one block, then moving on to a later block of the same span. */
    @Test
    public void testRemoveRangesRevisitBlockThenMoveOn() {
        final RspBitmap recv = fullBlockSpans(2, 6, 10);
        final WritableRowSet arg = RowSetFactory.empty();
        arg.insertRange(1 * BS + 10, 1 * BS + 20);
        arg.insertRange(1 * BS + 100, 1 * BS + 200); // same block again
        arg.insertRange(4 * BS + 5, 4 * BS + 9); // later block, same original span
        arg.insertRange(11 * BS + 5, 11 * BS + 9); // the second span
        checkRemoveRanges(recv, arg);
    }

    /** Randomized removeRanges with ranges deliberately clustered within blocks as well as within spans. */
    @Test
    public void testRandomRemoveRangesClusteredInBlocks() {
        final Random rand = new Random(4231);
        for (int trial = 0; trial < 5000; ++trial) {
            RspBitmap recv = RspBitmap.makeEmpty();
            long block = 0;
            final List<long[]> spans = new ArrayList<>();
            for (int i = 0; i < 8; ++i) {
                final int blocks = 1 + rand.nextInt(5);
                recv = recv.appendRangeUnsafe(block * BS, (block + blocks) * BS - 1);
                spans.add(new long[] {block, blocks});
                block += blocks + 1 + rand.nextInt(2);
            }
            recv.finishMutations();

            final WritableRowSet arg = RowSetFactory.empty();
            for (final long[] sp : spans) {
                final long b = sp[0] + rand.nextInt((int) sp[1]);
                // several ranges inside the same block, ascending and disjoint
                long cursor = b * BS + rand.nextInt(50);
                final int pieces = 1 + rand.nextInt(4);
                for (int k = 0; k < pieces && cursor < (b + 1) * BS - 2; ++k) {
                    final long s = cursor + 1 + rand.nextInt(80);
                    final long e = Math.min(s + rand.nextInt(40), (b + 1) * BS - 1);
                    if (s > e) {
                        break;
                    }
                    arg.insertRange(s, e);
                    cursor = e + 1;
                }
            }
            if (arg.isEmpty()) {
                continue;
            }
            checkRemoveRanges(recv, arg);
        }
    }

    /** A whole block removed from the middle of a bigger full block span: both halves survive. */
    @Test
    public void testFullBlockSpanRemovedFromMiddle() {
        final RspBitmap recv = fullBlockSpans(3, 5, 8);
        RspBitmap arg = RspBitmap.makeEmpty();
        arg = arg.appendRangeUnsafe(2 * BS, 3 * BS - 1); // all of block 2 of the first span
        arg.finishMutations();
        checkAndNot(recv, arg);
    }

    /**
     * Two whole-block removals from the SAME original full block span. The second has to find the half the first left
     * behind.
     */
    @Test
    public void testTwoFullBlockSpansRemovedFromOneSpan() {
        final RspBitmap recv = fullBlockSpans(2, 7, 10);
        RspBitmap arg = RspBitmap.makeEmpty();
        arg = arg.appendRangeUnsafe(1 * BS, 2 * BS - 1); // block 1
        arg = arg.appendRangeUnsafe(4 * BS, 5 * BS - 1); // block 4, same original span
        arg.finishMutations();
        checkAndNot(recv, arg);
    }

    /** Whole-block removals at the very start and very end of a span, where only one half survives. */
    @Test
    public void testFullBlockSpanRemovedAtEnds() {
        final RspBitmap recv = fullBlockSpans(2, 4, 8);
        RspBitmap arg = RspBitmap.makeEmpty();
        arg = arg.appendRangeUnsafe(0, 1 * BS - 1); // first block of span one
        arg = arg.appendRangeUnsafe(11 * BS, 12 * BS - 1); // last block of span two
        arg.finishMutations();
        checkAndNot(recv, arg);
    }

    /** A multi-block removal covering the tail of one span, a gap, and the head of the next. */
    @Test
    public void testFullBlockSpanRemovalAcrossTwoSpans() {
        RspBitmap recv = RspBitmap.makeEmpty();
        recv = recv.appendRangeUnsafe(0, 4L * BS - 1); // blocks 0..3
        // Block 4 is deliberately left empty: adjacent full block spans are merged into one, so without a gap here
        // the two ranges below would be a single span and the removal would never reach more than one of ours.
        recv = recv.appendRangeUnsafe(5L * BS, 9L * BS - 1); // blocks 5..8
        recv = recv.appendRangeUnsafe(12L * BS, 16L * BS - 1);
        recv.finishMutations();
        assertEquals("receiver must hold three distinct spans", 3, recv.size());
        RspBitmap arg = RspBitmap.makeEmpty();
        arg = arg.appendRangeUnsafe(2L * BS, 7L * BS - 1); // blocks 2..6: our first span's tail, the gap, the second's
                                                           // head
        arg.finishMutations();
        checkAndNot(recv, arg);
    }

    /** Randomized whole-block removals, several per original span. */
    @Test
    public void testRandomFullBlockSpanRemovals() {
        final Random rand = new Random(99887);
        for (int trial = 0; trial < 5000; ++trial) {
            RspBitmap recv = RspBitmap.makeEmpty();
            long block = 0;
            final List<long[]> spans = new ArrayList<>();
            for (int i = 0; i < 8; ++i) {
                final int blocks = 2 + rand.nextInt(6);
                recv = recv.appendRangeUnsafe(block * BS, (block + blocks) * BS - 1);
                spans.add(new long[] {block, blocks});
                block += blocks + 1 + rand.nextInt(2);
            }
            recv.finishMutations();

            final TreeSet<Long> removeBlocks = new TreeSet<>();
            for (final long[] sp : spans) {
                final int howMany = rand.nextInt(3);
                for (int k = 0; k < howMany; ++k) {
                    removeBlocks.add(sp[0] + rand.nextInt((int) sp[1]));
                }
            }
            if (removeBlocks.isEmpty()) {
                continue;
            }
            RspBitmap arg = RspBitmap.makeEmpty();
            for (final long b : removeBlocks) {
                arg = arg.appendRangeUnsafe(b * BS, (b + 1) * BS - 1);
            }
            arg.finishMutations();
            checkAndNot(recv, arg);
        }
    }

    /** Randomized: full block spans of assorted lengths, removals clustered so several share a span. */
    @Test
    public void testRandomClusteredRemovals() {
        final Random rand = new Random(23407);
        for (int trial = 0; trial < 5000; ++trial) {
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

            // Ranges, not keys: an entire block is one range rather than 65536 appends.
            final java.util.TreeMap<Long, Long> toRemove = new java.util.TreeMap<>();
            for (final long[] sp : spans) {
                final int removals = rand.nextInt(4);
                for (int r = 0; r < removals; ++r) {
                    final long b = sp[0] + rand.nextInt((int) sp[1]);
                    if (rand.nextInt(4) == 0) {
                        toRemove.put(b * BS, (b + 1) * BS - 1); // an entire block
                    } else {
                        final long v = b * BS + rand.nextInt((int) BS);
                        toRemove.putIfAbsent(v, v);
                    }
                }
            }
            if (toRemove.isEmpty()) {
                continue;
            }
            RspBitmap arg = RspBitmap.makeEmpty();
            long prevEnd = -2;
            for (final java.util.Map.Entry<Long, Long> e : toRemove.entrySet()) {
                if (e.getKey() <= prevEnd) {
                    continue; // subsumed by a whole-block range already appended
                }
                arg = arg.appendRangeUnsafe(Math.max(e.getKey(), prevEnd + 1), e.getValue());
                prevEnd = e.getValue();
            }
            arg.finishMutations();
            checkAndNot(recv, arg);
        }
    }
}
