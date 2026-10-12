//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * {@link RowSet#reverseIterator()} {@code advance(v)}: moves to the largest key at or below {@code v} (a no-op when the
 * current value already is), returning false -- and leaving {@code hasNext()} false -- when no such key remains. Every
 * pair of consecutive advances, and every advance after a few {@code nextLong()} steps, is checked against a sorted
 * array of the keys.
 */
public class ReverseIteratorAdvanceTest {

    private static final long MAX = Long.MAX_VALUE;
    private static final long BS = RspArray.BLOCK_SIZE;

    private static long[] keysOf(final long[]... ranges) {
        final TreeSet<Long> set = new TreeSet<>();
        for (final long[] r : ranges) {
            for (long k = r[0];; ++k) {
                set.add(k);
                if (k == r[1]) {
                    break;
                }
            }
        }
        final long[] out = new long[set.size()];
        int i = 0;
        for (final long k : set) {
            out[i++] = k;
        }
        return out;
    }

    private static long[] probesOf(final long[]... ranges) {
        final TreeSet<Long> p = new TreeSet<>();
        for (final long[] range : ranges) {
            for (final long k : new long[] {range[0], range[1], range[0] + 1, range[1] - 1}) {
                if (k < range[0] || k > range[1]) {
                    continue;
                }
                for (final long d : new long[] {-2, -1, 0, 1, 2}) {
                    final long v = k + d;
                    if ((d < 0 && v > k) || (d > 0 && v < k)) {
                        continue; // wrapped
                    }
                    p.add(v);
                }
            }
        }
        p.add(0L);
        p.add(MAX);
        p.add(MAX - 1);
        final long[] out = new long[p.size()];
        int i = 0;
        for (final long v : p) {
            out[i++] = v;
        }
        return out;
    }

    /** Largest index with keys[i] <= v, or -1. */
    private static int floorIndex(final long[] keys, final long v) {
        int i = keys.length - 1;
        while (i >= 0 && keys[i] > v) {
            --i;
        }
        return i;
    }

    private static void checkAdvance(final String what, final RowSet.SearchIterator it, final long[] keys,
            final int posIn, final long v, final List<String> failures) {
        // posIn: index of current value, or keys.length when nothing produced yet
        final int expPos;
        if (posIn < keys.length && keys[posIn] <= v) {
            expPos = posIn;
        } else {
            expPos = floorIndex(keys, v);
        }
        final boolean res = it.advance(v);
        if (expPos < 0) {
            if (res) {
                failures.add(what + " advance(" + v + ") from pos " + posIn + ": expected false, got true");
                return;
            }
            if (it.hasNext()) {
                failures.add(what + " advance(" + v + ") from pos " + posIn
                        + ": returned false but hasNext() is still true");
            }
            return;
        }
        if (!res) {
            failures.add(what + " advance(" + v + ") from pos " + posIn + ": expected true, got false");
            return;
        }
        if (it.currentValue() != keys[expPos]) {
            failures.add(what + " advance(" + v + ") from pos " + posIn + ": expected currentValue " + keys[expPos]
                    + ", got " + it.currentValue());
        }
        if (it.hasNext() != (expPos > 0)) {
            failures.add(what + " advance(" + v + ") from pos " + posIn + ": expected hasNext " + (expPos > 0)
                    + ", got " + it.hasNext());
        }
    }

    private static void runPairs(final String what, final WritableRowSet rs, final long[] keys,
            final List<String> failures, final long[]... ranges) {
        final long[] probes = probesOf(ranges);
        for (final long v1 : probes) {
            for (final long v2 : probes) {
                try (final RowSet.SearchIterator it = rs.reverseIterator()) {
                    final int pos1 = keys.length; // nothing produced yet
                    checkAdvance(what, it, keys, pos1, v1, failures);
                    final int p1 = (pos1 < keys.length && keys[pos1] <= v1) ? pos1 : floorIndex(keys, v1);
                    if (p1 < 0) {
                        continue;
                    }
                    checkAdvance(what, it, keys, p1, v2, failures);
                }
            }
        }
        // after some nextLong() steps
        for (int steps = 1; steps <= Math.min(3, keys.length); ++steps) {
            for (final long v : probes) {
                try (final RowSet.SearchIterator it = rs.reverseIterator()) {
                    for (int i = 0; i < steps; ++i) {
                        it.nextLong();
                    }
                    checkAdvance(what + " after " + steps + " steps", it, keys, keys.length - steps, v, failures);
                }
            }
        }
    }

    private static void check(final long[]... ranges) {
        final long[] keys = keysOf(ranges);
        final List<String> failures = new ArrayList<>();
        try (final WritableRowSet rs = rspOf(ranges)) {
            runPairs("rsp " + RowSetTestCommon.render(List.of(ranges)), rs, keys, failures, ranges);
        }
        try (final WritableRowSet rs = sortedRangesOf(ranges)) {
            runPairs("sortedRanges " + RowSetTestCommon.render(List.of(ranges)), rs, keys, failures, ranges);
        }
        if (ranges.length == 1) {
            try (final WritableRowSet rs = singleRangeOf(ranges[0][0], ranges[0][1])) {
                runPairs("singleRange " + RowSetTestCommon.render(List.of(ranges)), rs, keys, failures, ranges);
            }
        }
        if (!failures.isEmpty()) {
            final TreeSet<String> distinct = new TreeSet<>(failures);
            fail(distinct.size() + " distinct failure(s), first few:\n" + String.join("\n",
                    new ArrayList<>(distinct).subList(0, Math.min(12, distinct.size()))));
        }
    }

    @Test
    public void testThreeSpansWithSingletons() {
        // A container span, a singleton span, and a singleton at the last key; advance(-1) after the first step
        // used to report no key and yet leave the iterator claiming to have more.
        check(new long[] {0, 2}, new long[] {4 * BS, 4 * BS}, new long[] {MAX, MAX});
    }

    @Test
    public void testTwoSingletonSpans() {
        check(new long[] {5, 5}, new long[] {BS + 3, BS + 3});
    }

    @Test
    public void testContainerThenSingleton() {
        check(new long[] {10, 12}, new long[] {2 * BS + 3, 2 * BS + 3});
    }

    @Test
    public void testSingletonThenContainer() {
        check(new long[] {10, 10}, new long[] {2 * BS + 3, 2 * BS + 5});
    }

    @Test
    public void testFullBlockSpanThenContainer() {
        check(new long[] {BS, 2 * BS - 1}, new long[] {3 * BS + 3, 3 * BS + 5});
    }

    @Test
    public void testContainerThenFullBlockSpan() {
        check(new long[] {3, 5}, new long[] {2 * BS, 3 * BS - 1});
    }

    @Test
    public void testSingleContainer() {
        check(new long[] {3, 5}, new long[] {9, 9});
    }

    @Test
    public void testSingleRange() {
        check(new long[] {3, 5});
    }

    @Test
    public void testSpansAtTheTopOfTheKeySpace() {
        check(new long[] {MAX - BS - 1, MAX - BS - 1}, new long[] {MAX - 1, MAX});
    }
}
