//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Fixtures and assertions shared by the rowset tests, the counterpart of {@code ContainerTestCommon} in the Container
 * module.
 * <p>
 * The builders here name the implementation they produce rather than going through {@code RowSetFactory}, which chooses
 * by shape: a lone range becomes a {@link SingleRange}, a handful become {@link SortedRanges}, and inserting a disjoint
 * range into a single range converts it. A test meaning to exercise one implementation and silently given another
 * passes against broken code, so use these and {@link #assertBackedBy} rather than trusting the factory.
 */
public class RowSetTestCommon {

    // Construction, with the backing implementation forced.

    /** A bitmap holding {@code ranges}, which must be given in ascending order. */
    public static RspBitmap rspBitmapOf(final long[]... ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (final long[] r : ranges) {
            rb = rb.appendRangeUnsafe(r[0], r[1]);
        }
        rb.finishMutations();
        return rb;
    }

    /** A bitmap-backed rowset holding {@code ranges}, which must be given in ascending order. */
    public static WritableRowSet rspOf(final long[]... ranges) {
        return new WritableRowSetImpl(rspBitmapOf(ranges));
    }

    /**
     * Sorted ranges holding {@code ranges}.
     * <p>
     * {@link SortedRanges#addRange} answers null once the ranges outgrow its capacity, so each addition is checked:
     * without that, a fixture too large for the backing it names surfaces later as an unrelated
     * {@link NullPointerException} rather than saying what went wrong.
     */
    public static SortedRanges sortedRangesImplOf(final long[]... ranges) {
        SortedRanges sr = SortedRanges.makeSingleRange(ranges[0][0], ranges[0][1]);
        assertNotNull("range did not fit in a SortedRanges: " + ranges[0][0] + "-" + ranges[0][1], sr);
        for (int i = 1; i < ranges.length; ++i) {
            sr = sr.addRange(ranges[i][0], ranges[i][1]);
            assertNotNull("ranges did not fit in a SortedRanges, stopped at " + ranges[i][0] + "-" + ranges[i][1], sr);
        }
        return sr;
    }

    /** A sorted-ranges-backed rowset holding {@code ranges}. */
    public static WritableRowSet sortedRangesOf(final long[]... ranges) {
        return new WritableRowSetImpl(sortedRangesImplOf(ranges));
    }

    /** A single-range-backed rowset holding {@code [start, end]}. */
    public static WritableRowSet singleRangeOf(final long start, final long end) {
        return new WritableRowSetImpl(SingleRange.make(start, end));
    }

    // Extraction and rendering.

    public static List<long[]> rangesOf(final RowSet rs) {
        final List<long[]> out = new ArrayList<>();
        rs.forEachRowKeyRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    public static List<long[]> rangesOf(final RspBitmap rb) {
        final List<long[]> out = new ArrayList<>();
        rb.forEachLongRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    public static String render(final List<long[]> ranges) {
        final StringBuilder sb = new StringBuilder();
        for (final long[] r : ranges) {
            sb.append(r[0]).append('-').append(r[1]).append(' ');
        }
        return sb.toString();
    }

    /** Ranges rather than keys, which is the only safe comparison for a rowset holding {@link Long#MAX_VALUE}. */
    public static String renderRanges(final RowSet rs) {
        return render(rangesOf(rs));
    }

    public static List<Long> keysOf(final RowSet rs) {
        final List<Long> out = new ArrayList<>();
        rs.forAllRowKeys(out::add);
        return out;
    }

    public static List<Long> keysOf(final RowSequence seq) {
        final List<Long> out = new ArrayList<>();
        seq.forAllRowKeys(out::add);
        return out;
    }

    /**
     * Keys of {@code seq}, failing rather than hanging if the walk runs past {@code limit}. Necessary wherever a walk
     * could step past the last key of the key space and wrap.
     */
    public static List<Long> keysOfBounded(final RowSequence seq, final long limit) {
        final List<Long> out = new ArrayList<>();
        seq.forEachRowKey(k -> {
            out.add(k);
            if (out.size() > limit) {
                fail("walk did not stop: " + out.size() + " keys, last was " + k);
            }
            return true;
        });
        return out;
    }

    /** As {@link #keysOfBounded(RowSequence, long)}, for an iterator. */
    public static List<Long> keysOfBounded(final RowSet.Iterator it, final long limit) {
        final List<Long> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(it.nextLong());
            if (out.size() > limit) {
                fail("iteration did not stop: " + out.size() + " keys, last was " + out.get(out.size() - 1));
            }
        }
        return out;
    }

    // Fixture guards and reference counting.

    /**
     * Assert that {@code rs} is backed by the implementation whose class name contains {@code expected}, so a case
     * meant to exercise one implementation cannot quietly run on another.
     */
    public static void assertBackedBy(final String what, final RowSet rs, final String expected) {
        final String backing = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
        assertTrue(what + " is backed by " + backing + ", wanted " + expected, backing.contains(expected));
    }

    /**
     * Repeat {@code op} and assert the reference count of {@code watched} is where it started.
     * <p>
     * Only {@link RspBitmap} and {@link SortedRanges} can show a leak: {@link SingleRange} hands out copies and reports
     * a constant count of one, and {@code OrderedLongSet.EMPTY} ignores release. An iterator also gives back its
     * reference on reaching the end of its data, so a fixture must be shaped to make {@code op} stop with data unread.
     */
    public static void assertRefCountHoldsSteady(final String what, final OrderedLongSet watched, final int repetitions,
            final Runnable op) {
        final int steadyState = watched.ixRefCount();
        for (int i = 0; i < repetitions; ++i) {
            op.run();
        }
        assertEquals(what + ": reference count after " + repetitions + " operations", steadyState,
                watched.ixRefCount());
    }
}
