//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.RowSetUtils;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.container.BitmapContainer;
import io.deephaven.engine.rowset.impl.rsp.container.RunContainer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The bulk operations that rank keys and positions within one bitmap or run block through a resumable cursor answer
 * exactly as a sorted array of the keys does: {@code getKeysForPositions}, {@code subSetForPositions}, the
 * {@link RowSequence.Iterator} advance and slice methods, {@link RowSetShiftData#apply(WritableRowSet)}, and
 * {@link RowSetUtils#forAllInvertedLongRanges}.
 */
public class RspRankCursorBulkOpsTest {

    /** Keys 0, 2, 4, ... : one block held in a BitmapContainer. */
    private static long[] bitmapBlockKeys() {
        final long[] keys = new long[32768];
        for (int i = 0; i < keys.length; ++i) {
            keys[i] = 2L * i;
        }
        return keys;
    }

    /** 1500 runs of 16 keys spaced 43 apart: one block held in a RunContainer. */
    private static long[] runBlockKeys() {
        final long[] keys = new long[1500 * 16];
        int n = 0;
        for (int r = 0; r < 1500; ++r) {
            for (int k = 0; k < 16; ++k) {
                keys[n++] = 43L * r + k;
            }
        }
        return keys;
    }

    private static WritableRowSet rowSetOf(final long[] keys, final Class<?> expectedContainer) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (final long k : keys) {
            b.appendKey(k);
        }
        final WritableRowSet rs = b.build();
        final RspBitmap rb = (RspBitmap) ((WritableRowSetImpl) rs).getInnerSet();
        assertEquals(1, rb.getSize());
        assertTrue("expected " + expectedContainer.getSimpleName() + ", got " + rb.getSpans()[0].getClass(),
                expectedContainer.isInstance(rb.getSpans()[0]));
        return rs;
    }

    private static String renderKeys(final long[] keys, final int from, final int toExclusive) {
        final StringBuilder sb = new StringBuilder();
        int i = from;
        while (i < toExclusive) {
            final long start = keys[i];
            int j = i;
            while (j + 1 < toExclusive && keys[j + 1] == keys[j] + 1) {
                ++j;
            }
            sb.append(start).append('-').append(keys[j]).append(' ');
            i = j + 1;
        }
        return sb.toString();
    }

    private static int lowerBound(final long[] keys, final long v) {
        int i = Arrays.binarySearch(keys, v);
        return i >= 0 ? i : ~i;
    }

    private static void checkGetKeysForPositions(final String what, final RowSet rs, final long[] keys) {
        final List<Long> out = new ArrayList<>();
        // every position
        final long[] all = new long[keys.length];
        for (int i = 0; i < all.length; ++i) {
            all[i] = i;
        }
        rs.getKeysForPositions(Arrays.stream(all).iterator(), out::add);
        assertEquals(what + " all positions count", keys.length, out.size());
        for (int i = 0; i < keys.length; ++i) {
            assertEquals(what + " key at position " + i, keys[i], (long) out.get(i));
        }
        // a sparse ascending subset
        out.clear();
        final long[] sparse = new long[keys.length / 7];
        for (int i = 0; i < sparse.length; ++i) {
            sparse[i] = 7L * i + (i % 3);
        }
        rs.getKeysForPositions(Arrays.stream(sparse).iterator(), out::add);
        for (int i = 0; i < sparse.length; ++i) {
            assertEquals(what + " key at sparse position " + sparse[i], keys[(int) sparse[i]], (long) out.get(i));
        }
    }

    private static void checkSubSetForPositions(final String what, final RowSet rs, final long[] keys) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final StringBuilder expected = new StringBuilder();
        for (int p = 1; p < keys.length; p += 3) {
            b.appendKey(p);
            expected.append(keys[p]).append('-').append(keys[p]).append(' ');
        }
        try (final RowSet positions = b.build(); final WritableRowSet sub = rs.subSetForPositions(positions)) {
            assertEquals(what + " subSetForPositions", expected.toString(), render(rangesOf(sub)));
        }
    }

    /** Standalone slices, which rank without a cursor, agree with the keys and with each other's boundaries. */
    private static void checkStandaloneSlices(final String what, final RowSet rs, final long[] keys) {
        final int sliceLength = 700;
        for (int pos = 0; pos < keys.length; pos += sliceLength) {
            final int endPos = Math.min(keys.length, pos + sliceLength);
            try (final RowSequence slice = rs.getRowSequenceByPosition(pos, sliceLength)) {
                assertEquals(what + " slice at " + pos + " first key", keys[pos], slice.firstRowKey());
                assertEquals(what + " slice at " + pos + " last key", keys[endPos - 1], slice.lastRowKey());
                final StringBuilder actual = new StringBuilder();
                slice.forAllRowKeyRanges((s, e) -> actual.append(s).append('-').append(e).append(' '));
                assertEquals(what + " slice at " + pos + " ranges", renderKeys(keys, pos, endPos), actual.toString());
            }
            try (final RowSequence slice = rs.getRowSequenceByKeyRange(keys[pos], keys[endPos - 1])) {
                assertEquals(what + " key-range slice at " + pos + " size", endPos - pos, slice.size());
                final StringBuilder actual = new StringBuilder();
                slice.forAllRowKeyRanges((s, e) -> actual.append(s).append('-').append(e).append(' '));
                assertEquals(what + " key-range slice at " + pos + " ranges", renderKeys(keys, pos, endPos),
                        actual.toString());
            }
        }
    }

    private static void checkIteratorAdvanceAndSlices(final String what, final RowSet rs, final long[] keys,
            final Random r) {
        try (final RowSequence.Iterator it = rs.getRowSequenceIterator()) {
            long target = 0;
            while (it.hasMore()) {
                target += 1 + r.nextInt(40);
                final int pos = lowerBound(keys, target);
                final boolean expectMore = pos < keys.length;
                assertEquals(what + " advance(" + target + ")", expectMore, it.advance(target));
                if (!expectMore) {
                    break;
                }
                assertEquals(what + " peekNextKey after advance(" + target + ")", keys[pos], it.peekNextKey());
                final long through = target + r.nextInt(60);
                final int endPos = lowerBound(keys, through + 1); // exclusive
                final RowSequence slice = it.getNextRowSequenceThrough(through);
                assertEquals(what + " slice through " + through, renderKeys(keys, pos, endPos),
                        render(rangesOf(slice.asRowSet())));
                if (endPos > pos) {
                    assertEquals(what + " slice first key", keys[pos], slice.firstRowKey());
                    assertEquals(what + " slice last key", keys[endPos - 1], slice.lastRowKey());
                }
                target = through;
            }
        }
    }

    /**
     * Windows are {@code {begin, end, delta}} triples in ascending order, chosen so that no key lands on a key that
     * stays put; {@code windowStride} is the distance between consecutive windows.
     */
    private static void checkShiftApply(final String what, final RowSet rs, final long[] keys, final long windowLength,
            final long windowStride, final long delta) {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        final long last = keys[keys.length - 1];
        for (long w = 0; w + windowLength - 1 <= last; w += windowStride) {
            b.shiftRange(w, w + windowLength - 1, delta);
        }
        final RowSetShiftData shiftData = b.build();
        final TreeSet<Long> expected = new TreeSet<>();
        for (final long k : keys) {
            final boolean inWindow =
                    (k % windowStride) < windowLength && k - (k % windowStride) + windowLength - 1 <= last;
            expected.add(inWindow ? k + delta : k);
        }
        assertEquals(what + " oracle keeps every key distinct", keys.length, expected.size());
        final List<Long> expectedList = new ArrayList<>(expected);
        try (final WritableRowSet shifted = rs.copy()) {
            shiftData.apply(shifted);
            assertEquals(what + " apply size", expectedList.size(), shifted.size());
            final List<Long> actual = new ArrayList<>();
            shifted.forAllRowKeys(actual::add);
            assertEquals(what + " apply keys", expectedList, actual);
        }
    }

    private static void checkForAllInvertedLongRanges(final String what, final RowSet rs, final long[] keys) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final StringBuilder expected = new StringBuilder();
        for (int i = 0; i < keys.length; i += 5) {
            b.appendKey(keys[i]);
            expected.append(i).append('-').append(i).append(' ');
        }
        final StringBuilder actual = new StringBuilder();
        try (final RowSet dest = b.build()) {
            RowSetUtils.forAllInvertedLongRanges(rs, dest,
                    (s, e) -> actual.append(s).append('-').append(e).append(' '));
        }
        assertEquals(what + " inverted position ranges", expected.toString(), actual.toString());
    }

    @Test
    public void testBitmapBlock() {
        final long[] keys = bitmapBlockKeys();
        try (final WritableRowSet rs = rowSetOf(keys, BitmapContainer.class)) {
            checkGetKeysForPositions("bitmap block", rs, keys);
            checkStandaloneSlices("bitmap block", rs, keys);
            checkSubSetForPositions("bitmap block", rs, keys);
            checkIteratorAdvanceAndSlices("bitmap block", rs, keys, new Random(11));
            // Windows [8i, 8i + 3] move by one: keys 8i and 8i + 2 land on the empty 8i + 1 and 8i + 3.
            checkShiftApply("bitmap block", rs, keys, 4, 8, 1);
            checkForAllInvertedLongRanges("bitmap block", rs, keys);
        }
    }

    @Test
    public void testRunBlock() {
        final long[] keys = runBlockKeys();
        try (final WritableRowSet rs = rowSetOf(keys, RunContainer.class)) {
            checkGetKeysForPositions("run block", rs, keys);
            checkStandaloneSlices("run block", rs, keys);
            checkSubSetForPositions("run block", rs, keys);
            checkIteratorAdvanceAndSlices("run block", rs, keys, new Random(12));
            // Each run of 16 keys moves by five into the empty stretch after it.
            checkShiftApply("run block", rs, keys, 16, 43, 5);
            checkForAllInvertedLongRanges("run block", rs, keys);
        }
    }
}
