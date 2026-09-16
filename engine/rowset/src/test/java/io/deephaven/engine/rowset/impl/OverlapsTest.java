//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Cross-checks {@link OrderedLongSet#ixOverlaps} against a linear merge of the same ranges, for every pair of
 * representations and both argument orders.
 */
public class OverlapsTest {

    /**
     * A set of keys as a flat array of {@code [start, end]} pairs, ascending, with at least one key between consecutive
     * ranges.
     */
    private static final class Shape {
        private final String name;
        private final long[] ranges;

        private Shape(final String name, final long[] ranges) {
            this.name = name;
            this.ranges = ranges;
        }

        private int rangeCount() {
            return ranges.length / 2;
        }

        private long first() {
            return ranges[0];
        }

        private long last() {
            return ranges[ranges.length - 1];
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static Shape shape(final String name, final long... ranges) {
        for (int i = 0; i < ranges.length; i += 2) {
            if (ranges[i] > ranges[i + 1]) {
                throw new IllegalArgumentException(name + ": range " + (i / 2) + " is empty");
            }
            if (i > 0 && ranges[i] <= ranges[i - 1] + 1) {
                throw new IllegalArgumentException(name + ": range " + (i / 2) + " is not separated from its left");
            }
        }
        return new Shape(name, ranges);
    }

    /** The answer, by linear merge over the two range lists. */
    private static boolean expectedOverlaps(final Shape a, final Shape b) {
        int i = 0;
        int j = 0;
        while (i < a.ranges.length && j < b.ranges.length) {
            if (a.ranges[i] <= b.ranges[j + 1] && b.ranges[j] <= a.ranges[i + 1]) {
                return true;
            }
            if (a.ranges[i + 1] < b.ranges[j + 1]) {
                i += 2;
            } else {
                j += 2;
            }
        }
        return false;
    }

    private static RspBitmap toRsp(final Shape shape) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < shape.ranges.length; i += 2) {
            rb = rb.addRangeUnsafe(shape.ranges[i], shape.ranges[i + 1]);
        }
        rb.finishMutations();
        return rb;
    }

    /** Null when the shape does not fit in a packed array. */
    private static SortedRanges toSortedRanges(final Shape shape) {
        SortedRanges sr = SortedRanges.makeEmpty();
        for (int i = 0; i < shape.ranges.length && sr != null; i += 2) {
            sr = sr.addRange(shape.ranges[i], shape.ranges[i + 1]);
        }
        return sr;
    }

    /** Null when the shape is not a single range. */
    private static SingleRange toSingleRange(final Shape shape) {
        if (shape.rangeCount() != 1) {
            return null;
        }
        return SingleRange.make(shape.first(), shape.last());
    }

    private static List<OrderedLongSet> representations(final Shape shape) {
        final List<OrderedLongSet> impls = new ArrayList<>(3);
        impls.add(toRsp(shape));
        final SortedRanges sr = toSortedRanges(shape);
        if (sr != null) {
            impls.add(sr);
        }
        final SingleRange single = toSingleRange(shape);
        if (single != null) {
            impls.add(single);
        }
        return impls;
    }

    private static String describe(final OrderedLongSet impl) {
        return impl.getClass().getSimpleName();
    }

    private static void check(final Shape a, final Shape b) {
        final boolean expected = expectedOverlaps(a, b);
        final List<OrderedLongSet> as = representations(a);
        final List<OrderedLongSet> bs = representations(b);
        for (final OrderedLongSet ia : as) {
            for (final OrderedLongSet ib : bs) {
                final String m = a + " (" + describe(ia) + ") vs " + b + " (" + describe(ib) + ")";
                assertEquals(m, expected, ia.ixOverlaps(ib));
                assertEquals("reversed " + m, expected, ib.ixOverlaps(ia));
                // ixIntersectOnNew is the independent oracle: it builds the intersection rather than short circuiting.
                assertEquals(m + " vs intersect", expected, !ia.ixIntersectOnNew(ib).ixIsEmpty());
            }
        }
        // The generic iterator path, which the impl dispatch above no longer reaches for these representations.
        final SortedRanges sra = toSortedRanges(a);
        if (sra != null) {
            for (final OrderedLongSet ib : bs) {
                final RowSet.RangeIterator it = ib.ixRangeIterator();
                assertEquals(a + " (iterator) vs " + b + " (" + describe(ib) + ")", expected, sra.overlaps(it));
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Shape builders
    // ----------------------------------------------------------------------------------------------------------------

    /** {@code count} single keys, {@code stride} apart, starting at {@code start}. */
    private static Shape singletons(final String name, final long start, final long stride, final int count) {
        final long[] ranges = new long[2 * count];
        for (int i = 0; i < count; ++i) {
            ranges[2 * i] = ranges[2 * i + 1] = start + (long) i * stride;
        }
        return shape(name, ranges);
    }

    /** {@code count} ranges of {@code len} keys, {@code stride} apart, starting at {@code start}. */
    private static Shape runs(final String name, final long start, final long stride, final long len,
            final int count) {
        final long[] ranges = new long[2 * count];
        for (int i = 0; i < count; ++i) {
            ranges[2 * i] = start + (long) i * stride;
            ranges[2 * i + 1] = ranges[2 * i] + len - 1;
        }
        return shape(name, ranges);
    }

    /** Ranges drawn at random; gaps and lengths are uniform over the given bounds. */
    private static Shape random(final String name, final Random rand, final long start, final int count,
            final int maxGap, final int maxLen) {
        final long[] ranges = new long[2 * count];
        long key = start;
        for (int i = 0; i < count; ++i) {
            key += 2 + rand.nextInt(maxGap);
            ranges[2 * i] = key;
            key += rand.nextInt(maxLen);
            ranges[2 * i + 1] = key;
        }
        return shape(name, ranges);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Tests
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testHandBuiltShapes() {
        final List<Shape> shapes = new ArrayList<>();
        shapes.add(shape("single key", 7, 7));
        shapes.add(shape("single range", 0, 100));
        shapes.add(shape("single far range", 10 * BLOCK_SIZE, 12 * BLOCK_SIZE));
        shapes.add(shape("two ranges", 0, 10, 1000, 1010));
        shapes.add(singletons("even blocks", 0, 2 * BLOCK_SIZE, 64));
        shapes.add(singletons("odd blocks", BLOCK_SIZE, 2 * BLOCK_SIZE, 64));
        shapes.add(singletons("even keys", 0, 2, 200));
        shapes.add(singletons("odd keys", 1, 2, 200));
        shapes.add(runs("dense runs", 0, 16, 8, 100));
        shapes.add(runs("dense runs offset", 8, 16, 8, 100));
        shapes.add(runs("full blocks", 0, 2 * BLOCK_SIZE, BLOCK_SIZE, 32));
        shapes.add(runs("full blocks offset", BLOCK_SIZE, 2 * BLOCK_SIZE, BLOCK_SIZE, 32));
        shapes.add(shape("late singleton", 7, 7, 1000L * BLOCK_SIZE, 1000L * BLOCK_SIZE));
        shapes.add(shape("early and late", 0, 0, Long.MAX_VALUE / 4, Long.MAX_VALUE / 4));

        for (final Shape a : shapes) {
            for (final Shape b : shapes) {
                check(a, b);
            }
        }
    }

    /**
     * Two interleaved combs that never meet, then the same pair with one key moved so that they do. This is the shape
     * the seek is for: the answer is at the far end, or nowhere.
     */
    @Test
    public void testInterleavedCombs() {
        for (final long stride : new long[] {2, 3, 16, BLOCK_SIZE, 2 * BLOCK_SIZE, 70_000}) {
            for (final int count : new int[] {2, 3, 17, 200}) {
                final Shape a = singletons("comb a stride=" + stride + " n=" + count, 0, 2 * stride, count);
                final Shape b = singletons("comb b stride=" + stride + " n=" + count, stride, 2 * stride, count);
                check(a, b);

                // Drop b's last key onto a's last key.
                final long[] hit = b.ranges.clone();
                hit[hit.length - 1] = hit[hit.length - 2] = a.last();
                check(a, shape("comb b hitting last", hit));
            }
        }
    }

    /** One side entirely inside a gap of the other, and one side entirely to the left or right of the other. */
    @Test
    public void testDisjointByPosition() {
        final Shape outer = shape("outer", 0, 999, 100_000, 100_999);
        check(outer, shape("in the gap", 50_000, 50_999));
        check(outer, shape("left of everything", 0, 0));
        check(outer, shape("to the right", 200_000, 200_999));
        check(outer, singletons("many in the gap", 1_000, 7, 1_000));
        check(outer, singletons("many to the right", 101_000, 7, 1_000));
    }

    /** The overlap is a single key, at each of the interesting places. */
    @Test
    public void testSingleKeyOverlap() {
        final Shape comb = singletons("comb", 0, BLOCK_SIZE, 300);
        for (final int at : new int[] {0, 1, 149, 298, 299}) {
            final long key = comb.ranges[2 * at];
            check(comb, shape("touch at " + at, key, key));
            check(comb, shape("range touching at " + at, key, key + BLOCK_SIZE / 2));
            if (key >= BLOCK_SIZE / 2) {
                check(comb, shape("range ending at " + at, key - BLOCK_SIZE / 2, key));
            }
        }
    }

    @Test
    public void testRandomShapes() {
        final Random rand = new Random(0xD0FA11L);
        for (int trial = 0; trial < 400; ++trial) {
            final long start = rand.nextInt(4) == 0 ? 0 : rand.nextInt(1 << 20);
            final int countA = 1 + rand.nextInt(60);
            final int countB = 1 + rand.nextInt(60);
            final int maxGap = 1 + rand.nextInt(4 * BLOCK_SIZE);
            final int maxLen = 1 + rand.nextInt(BLOCK_SIZE);
            final Shape a = random("randA t=" + trial, rand, start, countA, maxGap, maxLen);
            final Shape b = random("randB t=" + trial, rand, start, countB, maxGap, maxLen);
            check(a, b);
        }
    }

    /**
     * Both sides large enough to leave the packed array, so the RSP x RSP path and the seek in the RSP probe both run
     * over many spans.
     */
    @Test
    public void testLargeSparseShapes() {
        final Random rand = new Random(0x5EEDL);
        for (int trial = 0; trial < 20; ++trial) {
            final Shape a = random("bigA t=" + trial, rand, 0, 5_000, 4 * BLOCK_SIZE, BLOCK_LAST);
            final Shape b = random("bigB t=" + trial, rand, 0, 5_000, 4 * BLOCK_SIZE, BLOCK_LAST);
            check(a, b);
        }
    }

    /** SortedRanges converts to RspBitmap once it outgrows its array; both sides of that boundary must agree. */
    @Test
    public void testAcrossTheSortedRangesBoundary() {
        for (final int count : new int[] {1, 2, 128, 1_000, 2_048, 4_096, 8_192}) {
            final Shape a = singletons("stair n=" + count, 0, 4, count);
            final Shape b = singletons("stair offset n=" + count, 2, 4, count);
            final SortedRanges sra = toSortedRanges(a);
            if (sra == null) {
                // Past the packed-array capacity; the RSP representations are still covered by check().
                check(a, b);
                continue;
            }
            assertNotNull(toRsp(a));
            check(a, b);
        }
    }
}
