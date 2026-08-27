//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * {@link RowSetShiftData#unapply(WritableRowSet, long)} moves a rowset from post-shift to pre-shift keyspace with an
 * extra offset applied to the shift windows. Checked against interval arithmetic: the windows are disjoint and
 * ascending in both keyspaces (see {@code validate()}), so unapplying is "cut each window out, put it back moved".
 */
public class RowSetShiftDataUnapplyOffsetTest {

    private static List<long[]> rangesOf(final RowSet rs) {
        final List<long[]> out = new ArrayList<>();
        rs.forEachRowKeyRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    private static List<long[]> intersect(final List<long[]> ranges, final long lo, final long hi) {
        final List<long[]> out = new ArrayList<>();
        for (final long[] r : ranges) {
            final long s = Math.max(r[0], lo);
            final long e = Math.min(r[1], hi);
            if (s <= e) {
                out.add(new long[] {s, e});
            }
        }
        return out;
    }

    private static List<long[]> minus(final List<long[]> from, final long lo, final long hi) {
        final List<long[]> out = new ArrayList<>();
        for (final long[] r : from) {
            if (r[1] < lo || r[0] > hi) {
                out.add(new long[] {r[0], r[1]});
                continue;
            }
            if (r[0] < lo) {
                out.add(new long[] {r[0], lo - 1});
            }
            if (r[1] > hi) {
                out.add(new long[] {hi + 1, r[1]});
            }
        }
        return out;
    }

    private static List<long[]> normalize(final List<long[]> ranges) {
        final List<long[]> all = new ArrayList<>(ranges);
        all.sort(Comparator.comparingLong(r -> r[0]));
        final List<long[]> out = new ArrayList<>();
        for (final long[] r : all) {
            if (!out.isEmpty() && r[0] <= out.get(out.size() - 1)[1] + 1) {
                final long[] prev = out.get(out.size() - 1);
                prev[1] = Math.max(prev[1], r[1]);
            } else {
                out.add(new long[] {r[0], r[1]});
            }
        }
        return out;
    }

    /** What unapply(rowSet, offset) must produce, by interval arithmetic on the shift windows. */
    private static List<long[]> expected(final RowSetShiftData sd, final RowSet rowSet, final long offset) {
        List<long[]> remaining = rangesOf(rowSet);
        final List<long[]> moved = new ArrayList<>();
        for (int idx = 0; idx < sd.size(); ++idx) {
            final long delta = sd.getShiftDelta(idx);
            final long lo = sd.getBeginRange(idx) + delta + offset;
            final long hi = sd.getEndRange(idx) + delta + offset;
            for (final long[] r : intersect(remaining, lo, hi)) {
                moved.add(new long[] {r[0] - delta, r[1] - delta});
            }
            remaining = minus(remaining, lo, hi);
        }
        final List<long[]> all = new ArrayList<>(remaining);
        all.addAll(moved);
        return normalize(all);
    }

    private static String render(final List<long[]> ranges) {
        final StringBuilder sb = new StringBuilder();
        for (final long[] r : ranges) {
            sb.append(r[0]).append('-').append(r[1]).append(' ');
        }
        return sb.toString();
    }

    private static void check(final RowSetShiftData sd, final WritableRowSet rowSet, final long offset) {
        final List<long[]> want = expected(sd, rowSet, offset);
        sd.unapply(rowSet, offset);
        assertEquals(render(want), render(rangesOf(rowSet)));
    }

    private static WritableRowSet rowSetOf(final long[]... ranges) {
        final WritableRowSet rs = RowSetFactory.empty();
        for (final long[] r : ranges) {
            rs.insertRange(r[0], r[1]);
        }
        return rs;
    }

    @Test
    public void testSinglePositiveShiftZeroOffset() {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(100, 200, 50);
        check(b.build(), rowSetOf(new long[] {120, 260}), 0);
    }

    @Test
    public void testSinglePositiveShiftWithOffset() {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(100, 200, 50);
        check(b.build(), rowSetOf(new long[] {1120, 1260}), 1000);
    }

    @Test
    public void testSingleNegativeShiftWithOffset() {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(500, 600, -80);
        check(b.build(), rowSetOf(new long[] {1000, 1600}), 1000);
    }

    @Test
    public void testKeysOutsideEveryWindowAreUntouched() {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(100, 200, 50);
        b.shiftRange(400, 500, 25);
        check(b.build(), rowSetOf(new long[] {0, 10}, new long[] {900, 910}), 0);
    }

    @Test
    public void testEmptyShiftDataAndEmptyRowSet() {
        check(RowSetShiftData.EMPTY, rowSetOf(new long[] {5, 9}), 7);
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(10, 20, 5);
        check(b.build(), RowSetFactory.empty(), 3);
    }

    @Test
    public void testRandomAgainstIntervalArithmetic() {
        final Random rand = new Random(23407);
        for (int trial = 0; trial < 3000; ++trial) {
            final int sign = (trial % 2 == 0) ? 1 : -1;
            final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
            long next = 100 + rand.nextInt(50);
            final int shifts = 1 + rand.nextInt(8);
            for (int s = 0; s < shifts; ++s) {
                final long begin = next + 20 + rand.nextInt(40);
                final long end = begin + rand.nextInt(30);
                b.shiftRange(begin, end, sign * (1 + rand.nextInt(10)));
                next = end;
            }
            final RowSetShiftData sd = b.build();
            final long offset = switch (rand.nextInt(3)) {
                case 0 -> 0L;
                case 1 -> (long) rand.nextInt(500);
                default -> 1000L + rand.nextInt(5000);
            };
            final WritableRowSet rs = RowSetFactory.empty();
            final int pieces = 1 + rand.nextInt(6);
            long cursor = rand.nextInt(100);
            for (int p = 0; p < pieces; ++p) {
                final long s = cursor + 1 + rand.nextInt(200);
                final long e = s + rand.nextInt(150);
                rs.insertRange(s, e);
                cursor = e + 1;
            }
            check(sd, rs, offset);
        }
    }
}
