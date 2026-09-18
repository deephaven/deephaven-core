//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

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
import static org.junit.Assert.assertTrue;

/**
 * Cross-checks {@link OrderedLongSet#ixSubsetOf} against a linear merge of the same ranges, for every pair of
 * representations.
 * <p>
 * {@link RspBitmap#subsetOf(SortedRanges)} picks between two walks by comparing its cardinality against the other
 * side's array length, so the shapes here are chosen to land on both sides of that choice; {@link #testBothWalks}
 * fails if a run stops covering either one.
 */
public class SubsetOfTest {

    /** A key set as a flat array of inclusive {@code [start, end]} pairs, ascending and non-adjacent. */
    private static long[] shape(final long... ranges) {
        for (int i = 0; i < ranges.length; i += 2) {
            if (ranges[i] > ranges[i + 1]) {
                throw new IllegalArgumentException("range " + (i / 2) + " is empty");
            }
            if (i > 0 && ranges[i] <= ranges[i - 1] + 1) {
                throw new IllegalArgumentException("range " + (i / 2) + " touches the one before it");
            }
        }
        return ranges;
    }

    /** Whether every key of {@code sub} is in {@code sup}, by linear merge. */
    private static boolean expected(final long[] sub, final long[] sup) {
        int j = 0;
        for (int i = 0; i < sub.length; i += 2) {
            while (j < sup.length && sup[j + 1] < sub[i]) {
                j += 2;
            }
            if (j >= sup.length || sup[j] > sub[i] || sup[j + 1] < sub[i + 1]) {
                return false;
            }
        }
        return true;
    }

    private static RspBitmap rsp(final long[] ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < ranges.length; i += 2) {
            rb = rb.addRange(ranges[i], ranges[i + 1]);
        }
        return rb;
    }

    /** Null when the shape does not fit in a packed array. */
    private static SortedRanges sortedRanges(final long[] ranges) {
        SortedRanges sr = SortedRanges.makeEmpty();
        for (int i = 0; i < ranges.length && sr != null; i += 2) {
            sr = sr.addRange(ranges[i], ranges[i + 1]);
        }
        return sr;
    }

    private static List<OrderedLongSet> representations(final long[] ranges) {
        final List<OrderedLongSet> impls = new ArrayList<>(3);
        impls.add(rsp(ranges));
        final SortedRanges sr = sortedRanges(ranges);
        if (sr != null) {
            impls.add(sr);
        }
        if (ranges.length == 2) {
            impls.add(SingleRange.make(ranges[0], ranges[1]));
        }
        return impls;
    }

    /** Which walk {@link RspBitmap#subsetOf(SortedRanges)} takes for this pair, mirroring its guard. */
    private static boolean walksOwnRanges(final long[] sub, final long[] sup) {
        final SortedRanges sr = sortedRanges(sup);
        return sr != null && rsp(sub).getCardinality() < sr.count();
    }

    private static void check(final String m, final long[] sub, final long[] sup) {
        final boolean want = expected(sub, sup);
        for (final OrderedLongSet a : representations(sub)) {
            for (final OrderedLongSet b : representations(sup)) {
                final String at = m + ": " + a.getClass().getSimpleName() + " in " + b.getClass().getSimpleName();
                assertEquals(at, want, a.ixSubsetOf(b));
                // ixMinusOnNew is the independent oracle: everything of ours outside them must be nothing.
                final OrderedLongSet minus = a.ixMinusOnNew(b);
                try {
                    assertEquals(at + " vs minus", want, minus.ixIsEmpty());
                } finally {
                    minus.ixRelease();
                }
            }
        }
    }

    /**
     * The shapes below have to exercise both of {@link RspBitmap#subsetOf(SortedRanges)}'s walks, or the coverage they
     * look like they give is not the coverage they give.
     */
    @Test
    public void testBothWalks() {
        boolean sawOwnRanges = false;
        boolean sawGaps = false;
        for (final long[][] pair : pairs()) {
            if (walksOwnRanges(pair[0], pair[1])) {
                sawOwnRanges = true;
            } else {
                sawGaps = true;
            }
        }
        assertTrue("no shape reaches the walk over our own ranges", sawOwnRanges);
        assertTrue("no shape reaches the walk over the other side's gaps", sawGaps);
    }

    @Test
    public void testShapes() {
        int i = 0;
        for (final long[][] pair : pairs()) {
            check("pair " + i++, pair[0], pair[1]);
        }
    }

    /**
     * Subject/superset pairs, true and false, spanning singletons, containers and full block spans, and both sides of
     * the walk choice: a sparse subject against a finely divided superset takes one walk, a dense subject against a
     * superset of few long ranges takes the other.
     */
    private static List<long[][]> pairs() {
        final List<long[][]> out = new ArrayList<>();

        // Sparse subject, finely divided superset: few keys of ours, many ranges of theirs.
        final long[] sparse = shape(0, 0, 4 * BLOCK_SIZE, 4 * BLOCK_SIZE, 9 * BLOCK_SIZE, 9 * BLOCK_SIZE);
        final long[] fine = comb(0, 2, 600);
        out.add(new long[][] {sparse, union(sparse, fine)});
        out.add(new long[][] {sparse, fine});

        // Dense subject, superset of few long ranges: the other side of the choice.
        final long[] dense = shape(0, 40 * BLOCK_SIZE);
        out.add(new long[][] {dense, shape(0, 80 * BLOCK_SIZE)});
        out.add(new long[][] {dense, shape(0, 40 * BLOCK_SIZE - 1)});
        out.add(new long[][] {dense, shape(1, 80 * BLOCK_SIZE)});

        // Full block spans on the subject, against a superset that does and does not cover them.
        final long[] blocks = shape(0, 3 * BLOCK_SIZE - 1, 8 * BLOCK_SIZE, 11 * BLOCK_SIZE - 1);
        out.add(new long[][] {blocks, shape(0, 12 * BLOCK_SIZE)});
        out.add(new long[][] {blocks, shape(0, 3 * BLOCK_SIZE - 1, 8 * BLOCK_SIZE, 11 * BLOCK_SIZE - 2)});

        // Containers: many keys inside single blocks, low bits only.
        final long[] lows = comb(1, 3, 400);
        out.add(new long[][] {lows, union(lows, comb(2, 3, 400))});
        out.add(new long[][] {lows, comb(1, 6, 200)});

        // Where the answer is: the failing key first, in the middle, and last.
        final long[] comb = comb(0, BLOCK_SIZE, 50);
        for (final int at : new int[] {0, 25, 49}) {
            final long[] holed = new long[comb.length];
            System.arraycopy(comb, 0, holed, 0, comb.length);
            final long[] sup = union(comb, shape(1, 1));
            out.add(new long[][] {holed, without(sup, comb[2 * at])});
        }

        // A range of ours that starts inside one of theirs but runs past its end. The walk has to check the end and
        // not just that the start was found, and the failing range is not the first, so the bound check cannot answer
        // first. Cardinality stays well under the superset's array length, so this takes the walk over our ranges.
        final long[] truncating = union(union(shape(0, 10), shape(1000, 1003)), comb(5000, 2, 600));
        out.add(new long[][] {shape(0, 10, 1000, 1005), truncating});
        out.add(new long[][] {shape(0, 10, 1000, 1003), truncating});
        out.add(new long[][] {shape(0, 10, 1001, 1003), truncating});
        // The same, with our range starting one key before theirs.
        out.add(new long[][] {shape(0, 10, 999, 1003), truncating});
        // And our range bridging a gap in theirs.
        out.add(new long[][] {shape(0, 10, 1000, 5000), truncating});

        // A subject that runs past the superset at either end, which the bound check answers.
        out.add(new long[][] {shape(0, 10), shape(2, 10)});
        out.add(new long[][] {shape(0, 10), shape(0, 8)});

        // Identical, and single keys.
        out.add(new long[][] {comb, comb});
        out.add(new long[][] {shape(7, 7), shape(7, 7)});
        out.add(new long[][] {shape(7, 7), shape(8, 8)});

        // Random, both directions, so the shapes are not only the ones I thought of.
        final Random rand = new Random(0x5AFE7);
        for (int t = 0; t < 60; ++t) {
            final long[] a = random(rand, 1 + rand.nextInt(40));
            final long[] b = random(rand, 1 + rand.nextInt(40));
            out.add(new long[][] {a, union(a, b)});
            out.add(new long[][] {a, b});
            out.add(new long[][] {union(a, b), a});
        }
        return out;
    }

    /** {@code count} single keys {@code stride} apart from {@code start}. */
    private static long[] comb(final long start, final long stride, final int count) {
        final long[] ranges = new long[2 * count];
        for (int i = 0; i < count; ++i) {
            ranges[2 * i] = ranges[2 * i + 1] = start + i * stride;
        }
        return ranges;
    }

    private static long[] random(final Random rand, final int count) {
        final long[] ranges = new long[2 * count];
        long key = rand.nextInt(1 << 12);
        for (int i = 0; i < count; ++i) {
            key += 2 + rand.nextInt(3 * BLOCK_SIZE);
            ranges[2 * i] = key;
            key += rand.nextInt(BLOCK_LAST);
            ranges[2 * i + 1] = key;
        }
        return ranges;
    }

    private static long[] union(final long[] a, final long[] b) {
        final long[] out = new long[a.length + b.length];
        int n = 0;
        int i = 0;
        int j = 0;
        while (i < a.length || j < b.length) {
            final boolean takeA = j >= b.length || (i < a.length && a[i] <= b[j]);
            final long start = takeA ? a[i] : b[j];
            final long end = takeA ? a[i + 1] : b[j + 1];
            if (takeA) {
                i += 2;
            } else {
                j += 2;
            }
            if (n > 0 && start <= out[n - 1] + 1) {
                out[n - 1] = Math.max(out[n - 1], end);
            } else {
                out[n++] = start;
                out[n++] = end;
            }
        }
        final long[] trimmed = new long[n];
        System.arraycopy(out, 0, trimmed, 0, n);
        return trimmed;
    }

    /** {@code ranges} without {@code key}, splitting the range holding it when it falls in the middle. */
    private static long[] without(final long[] ranges, final long key) {
        final long[] out = new long[ranges.length + 2];
        int n = 0;
        for (int i = 0; i < ranges.length; i += 2) {
            final long start = ranges[i];
            final long end = ranges[i + 1];
            if (key < start || key > end) {
                out[n++] = start;
                out[n++] = end;
                continue;
            }
            if (start < key) {
                out[n++] = start;
                out[n++] = key - 1;
            }
            if (key < end) {
                out[n++] = key + 1;
                out[n++] = end;
            }
        }
        final long[] trimmed = new long[n];
        System.arraycopy(out, 0, trimmed, 0, n);
        return trimmed;
    }
}
