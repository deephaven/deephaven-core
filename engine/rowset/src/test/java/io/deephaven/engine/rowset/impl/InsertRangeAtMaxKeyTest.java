//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Inserting a range that ends at {@link Long#MAX_VALUE} into a set that already holds keys near the top. Merging a new
 * range with what follows it is decided by looking one key past the range's end, and there is no key past the last one.
 */
public class InsertRangeAtMaxKeyTest {

    private static final long MAX = Long.MAX_VALUE;

    private static String ranges(final RowSet rs) {
        final List<String> out = new ArrayList<>();
        rs.forEachRowKeyRange((s, e) -> {
            out.add(s + "-" + e);
            return true;
        });
        return String.join(" ", out);
    }

    /** The expected result, coalesced, computed with no reliance on rowset code. */
    private static String expected(final long[][] existing, final long start, final long end) {
        final TreeSet<long[]> all = new TreeSet<>((a, b) -> Long.compare(a[0], b[0]));
        for (final long[] r : existing) {
            all.add(new long[] {r[0], r[1]});
        }
        all.add(new long[] {start, end});
        final List<long[]> merged = new ArrayList<>();
        for (final long[] r : all) {
            if (!merged.isEmpty()) {
                final long[] prev = merged.get(merged.size() - 1);
                // Guard the +1 so a previous end of MAX does not wrap.
                if (prev[1] == MAX || r[0] <= prev[1] + 1) {
                    prev[1] = Math.max(prev[1], r[1]);
                    continue;
                }
            }
            merged.add(new long[] {r[0], r[1]});
        }
        final List<String> out = new ArrayList<>();
        for (final long[] r : merged) {
            out.add(r[0] + "-" + r[1]);
        }
        return String.join(" ", out);
    }

    private static long cardinality(final String rendered) {
        long card = 0;
        for (final String r : rendered.split(" ")) {
            final int dash = r.lastIndexOf('-');
            card += Long.parseLong(r.substring(dash + 1)) - Long.parseLong(r.substring(0, dash)) + 1;
        }
        return card;
    }

    /** Build the shape on each backing that can hold it; a single range cannot hold more than one. */
    private static List<WritableRowSet> backingsFor(final long[][] shape) {
        final List<WritableRowSet> out = new ArrayList<>();
        if (shape.length == 1) {
            out.add(new WritableRowSetImpl(SingleRange.make(shape[0][0], shape[0][1])));
        }
        SortedRanges sr = SortedRanges.makeSingleRange(shape[0][0], shape[0][1]);
        for (int i = 1; i < shape.length; ++i) {
            sr = sr.addRange(shape[i][0], shape[i][1]);
        }
        out.add(new WritableRowSetImpl(sr));
        final RspBitmap rsp = RspBitmap.makeSingleRange(shape[0][0], shape[0][1]);
        for (int i = 1; i < shape.length; ++i) {
            rsp.addRangeUnsafeNoWriteCheck(shape[i][0], shape[i][1]);
        }
        rsp.finishMutations();
        out.add(new WritableRowSetImpl(rsp));
        return out;
    }

    private static void checkInsert(final long[][] shape, final long start, final long end) {
        final String want = expected(shape, start, end);
        for (final WritableRowSet rs : backingsFor(shape)) {
            final String backing = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
            try (final WritableRowSet closeMe = rs) {
                rs.insertRange(start, end);
                final String where = backing + " insertRange(" + start + ", " + end + ")";
                ((WritableRowSetImpl) rs).getInnerSet().ixValidate(where);
                assertEquals(where + " contents", want, ranges(rs));
                assertEquals(where + " size", cardinality(want), rs.size());
            }
        }
    }

    /** The receiver holds the last key as a lone key, which is the shape that exposes the wrap. */
    @Test
    public void testInsertRangeEndingAtMaxWithMaxAlreadyPresentAsAKey() {
        checkInsert(new long[][] {{5, 5}, {MAX, MAX}}, MAX - 3, MAX);
        checkInsert(new long[][] {{MAX, MAX}}, MAX - 5, MAX);
        checkInsert(new long[][] {{5, 5}, {MAX, MAX}}, MAX - 1, MAX);
        checkInsert(new long[][] {{5, 5}, {MAX, MAX}}, MAX, MAX);
        // A gap between the inserted range and the existing key is impossible when the range ends at MAX, but the
        // inserted range may start above or below other keys.
        checkInsert(new long[][] {{5, 5}, {1000, 1000}, {MAX, MAX}}, MAX - 10, MAX);
    }

    /** The receiver holds a range ending at the last key rather than a lone key. */
    @Test
    public void testInsertRangeEndingAtMaxWithARangeAlreadyEndingThere() {
        checkInsert(new long[][] {{5, 5}, {MAX - 2, MAX}}, MAX - 5, MAX);
        checkInsert(new long[][] {{MAX - 2, MAX}}, MAX - 5, MAX);
        checkInsert(new long[][] {{5, 5}, {MAX - 8, MAX}}, MAX - 3, MAX);
    }

    /** Inserting a range that ends at MAX into a set with nothing near the top. */
    @Test
    public void testInsertRangeEndingAtMaxIntoALowSet() {
        checkInsert(new long[][] {{5, 9}}, MAX - 3, MAX);
        checkInsert(new long[][] {{5, 9}, {1000, 1004}}, MAX - 3, MAX);
    }

    /** The same shapes reached through insert of another rowset rather than insertRange. */
    @Test
    public void testInsertOfARowSetEndingAtMax() {
        for (final WritableRowSet rs : backingsFor(new long[][] {{5, 5}, {MAX, MAX}})) {
            final String backing = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
            try (final WritableRowSet closeMe = rs;
                    final WritableRowSet other = new WritableRowSetImpl(SingleRange.make(MAX - 5, MAX))) {
                rs.insert(other);
                final String where = backing + " insert([MAX-5, MAX])";
                ((WritableRowSetImpl) rs).getInnerSet().ixValidate(where);
                assertEquals(where, "5-5 " + (MAX - 5) + "-" + MAX, ranges(rs));
                assertEquals(where + " size", 7, rs.size());
            }
        }
    }

    /** And through union, which must agree. */
    @Test
    public void testUnionWithARangeEndingAtMax() {
        for (final WritableRowSet rs : backingsFor(new long[][] {{5, 5}, {MAX, MAX}})) {
            try (final WritableRowSet closeMe = rs;
                    final WritableRowSet other = new WritableRowSetImpl(SingleRange.make(MAX - 5, MAX));
                    final RowSet united = rs.union(other)) {
                assertTrue("union is valid", united.size() == 7);
                assertEquals("union contents", "5-5 " + (MAX - 5) + "-" + MAX, ranges(united));
            }
        }
    }
}
