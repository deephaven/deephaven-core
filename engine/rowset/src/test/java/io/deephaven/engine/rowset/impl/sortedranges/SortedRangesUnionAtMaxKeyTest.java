//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unioning sorted ranges compares a range's end against the next range's start by looking one past the end. A range
 * ending at {@link Long#MAX_VALUE} has nothing past it, and stepping there anyway wraps to a negative key.
 */
public class SortedRangesUnionAtMaxKeyTest {

    private static final long MAX = Long.MAX_VALUE;

    private static WritableRowSet rowSetOf(final long[]... ranges) {
        final WritableRowSet rs = RowSetFactory.empty();
        for (final long[] r : ranges) {
            rs.insertRange(r[0], r[1]);
        }
        return rs;
    }

    /**
     * Ranges, not keys: a rowset holding {@link Long#MAX_VALUE} cannot be enumerated key by key, and comparing ranges
     * is the stronger check anyway since it also pins how the union coalesces.
     */
    private static List<long[]> rangesOf(final RowSet rs) {
        final List<long[]> out = new ArrayList<>();
        rs.forEachRowKeyRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    private static List<long[]> expectedUnion(final long[][] a, final long[][] b) {
        final List<long[]> all = new ArrayList<>();
        for (final long[][] set : new long[][][] {a, b}) {
            for (final long[] r : set) {
                all.add(new long[] {r[0], r[1]});
            }
        }
        all.sort(java.util.Comparator.comparingLong(r -> r[0]));
        final List<long[]> out = new ArrayList<>();
        for (final long[] r : all) {
            if (!out.isEmpty()) {
                final long[] prev = out.get(out.size() - 1);
                // Adjacent or overlapping; guard the +1 so a previous end of MAX does not wrap.
                if (prev[1] == MAX || r[0] <= prev[1] + 1) {
                    prev[1] = Math.max(prev[1], r[1]);
                    continue;
                }
            }
            out.add(new long[] {r[0], r[1]});
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

    private static long cardinalityOf(final List<long[]> ranges) {
        long card = 0;
        for (final long[] r : ranges) {
            card += r[1] - r[0] + 1;
        }
        return card;
    }

    private static void checkInsert(final long[][] receiver, final long[][] argument) {
        final List<long[]> expected = expectedUnion(receiver, argument);
        try (final WritableRowSet rs = rowSetOf(receiver);
                final WritableRowSet arg = rowSetOf(argument)) {
            rs.insert(arg);
            rs.validate("after insert");
            assertEquals("ranges", render(expected), render(rangesOf(rs)));
            assertEquals("size", cardinalityOf(expected), rs.size());
            // A handful of ranges fits sorted ranges comfortably. Falling back to an RSP would mean the union gave up,
            // which is what happens when it spins appending until the result overflows.
            final Object inner = ((io.deephaven.engine.rowset.impl.WritableRowSetImpl) rs).getInnerSet();
            org.junit.Assert.assertFalse("the union should not have fallen back to an RSP: " + inner.getClass(),
                    inner instanceof io.deephaven.engine.rowset.impl.rsp.RspBitmap);
        }
    }

    @Test
    public void testArgumentLowerThanAReceiverRangeEndingAtMax() {
        checkInsert(new long[][] {{MAX - 2, MAX}}, new long[][] {{196606, 196610}});
    }

    @Test
    public void testReceiverLowerThanAnArgumentRangeEndingAtMax() {
        checkInsert(new long[][] {{196606, 196610}}, new long[][] {{MAX - 2, MAX}});
    }

    @Test
    public void testBothEndAtMax() {
        checkInsert(new long[][] {{MAX - 5, MAX}}, new long[][] {{MAX - 2, MAX}});
    }

    @Test
    public void testOverlappingRangesEndingAtMax() {
        checkInsert(new long[][] {{100, 200}, {MAX - 4, MAX}}, new long[][] {{150, 260}, {MAX - 6, MAX - 2}});
    }

    @Test
    public void testAdjacentToARangeEndingAtMax() {
        checkInsert(new long[][] {{MAX - 3, MAX}}, new long[][] {{MAX - 8, MAX - 4}});
    }

    @Test
    public void testSingletonAtMax() {
        checkInsert(new long[][] {{MAX, MAX}}, new long[][] {{5, 9}});
        checkInsert(new long[][] {{5, 9}}, new long[][] {{MAX, MAX}});
    }

    /**
     * One side holds only a range ending at MAX while the other still has several lower ranges, so the comparison that
     * looks past MAX is reached with the other side genuinely behind.
     */
    @Test
    public void testOnlyMaxRangeAgainstSeveralLowerRanges() {
        checkInsert(new long[][] {{MAX - 2, MAX}},
                new long[][] {{100, 200}, {196606, 196610}, {1L << 40, (1L << 40) + 3}});
        checkInsert(new long[][] {{100, 200}, {196606, 196610}, {1L << 40, (1L << 40) + 3}},
                new long[][] {{MAX - 2, MAX}});
    }

    /** The same shapes through union rather than insert. */
    @Test
    public void testUnionAtMax() {
        try (final WritableRowSet a = rowSetOf(new long[] {MAX - 2, MAX});
                final WritableRowSet b = rowSetOf(new long[] {196606, 196610});
                final RowSet u = a.union(b)) {
            assertEquals(render(expectedUnion(new long[][] {{MAX - 2, MAX}}, new long[][] {{196606, 196610}})),
                    render(rangesOf(u)));
        }
    }
}
