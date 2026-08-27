//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link Long#MAX_VALUE} is a legal row key, and nothing lies past it. Code that walks to the end of something by
 * stepping one key beyond it, or by comparing against one key beyond it, wraps to a negative value there: the wrapped
 * bound reads as though the range were empty, or as though it still had further keys to give.
 *
 * <p>
 * Each test below reaches the top of the key space through a different walk. They are collected here because the
 * fixture -- a rowset whose last key is the last key -- is the whole setup in every case.
 */
public class RowSetAtMaxKeyTest {

    private static final long MAX = Long.MAX_VALUE;
    /** The first key of the last block: the key space is a whole number of blocks. */
    private static final long TOP_BLOCK = MAX - BLOCK_SIZE + 1;

    /**
     * Built as an RspBitmap on purpose: a couple of ranges would otherwise be backed by SortedRanges, whose walks never
     * reach the RSP span code.
     */
    private static WritableRowSet rspOf(final long[]... ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (final long[] r : ranges) {
            rb = rb.appendRangeUnsafe(r[0], r[1]);
        }
        rb.finishMutations();
        return new WritableRowSetImpl(rb);
    }

    private static WritableRowSet singleRangeOf(final long start, final long end) {
        return new WritableRowSetImpl(SingleRange.make(start, end));
    }

    /** A handful of ranges, which RowSetFactory backs with SortedRanges. */
    private static WritableRowSet sortedRangesOf(final long[]... ranges) {
        final WritableRowSet rs = RowSetFactory.empty();
        for (final long[] r : ranges) {
            rs.insertRange(r[0], r[1]);
        }
        return rs;
    }

    /** Collects keys, failing rather than hanging if the walk runs past what the rowset holds. */
    private static List<Long> keysOf(final RowSequence seq, final long limit) {
        final List<Long> keys = new ArrayList<>();
        seq.forEachRowKey(k -> {
            keys.add(k);
            if (keys.size() > limit) {
                fail("walk did not stop: " + keys.size() + " keys, last was " + k);
            }
            return true;
        });
        return keys;
    }

    private static List<Long> keysOf(final RowSet.Iterator it, final long limit) {
        final List<Long> keys = new ArrayList<>();
        while (it.hasNext()) {
            keys.add(it.nextLong());
            if (keys.size() > limit) {
                fail("iteration did not stop: " + keys.size() + " keys, last was " + keys.get(keys.size() - 1));
            }
        }
        return keys;
    }

    /** Counts and bounds instead of every key, for walks that cover a whole block. */
    private static void assertWalkCovers(final RowSet rs, final long expectedKeys, final long expectedLast) {
        final long[] count = {0};
        final long[] first = {-1}, last = {-1};
        rs.forAllRowKeys(k -> {
            if (count[0] == 0) {
                first[0] = k;
            }
            last[0] = k;
            if (++count[0] > expectedKeys + 4) {
                fail("walk did not stop: " + count[0] + " keys, last was " + k);
            }
        });
        assertEquals("keys visited", expectedKeys, count[0]);
        assertEquals("first key", rs.firstRowKey(), first[0]);
        assertEquals("last key", expectedLast, last[0]);
    }

    private static List<long[]> rangesOf(final RowSet rs) {
        final List<long[]> out = new ArrayList<>();
        rs.forEachRowKeyRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    private static String render(final List<long[]> ranges) {
        final StringBuilder sb = new StringBuilder();
        for (final long[] r : ranges) {
            sb.append(r[0]).append('-').append(r[1]).append(' ');
        }
        return sb.toString();
    }

    // A full block span walk bounded by one key past its end sees nothing at all when that wraps.

    @Test
    public void testFullBlockSpanWalkReachesTheTopBlock() {
        try (final WritableRowSet top = rspOf(new long[] {TOP_BLOCK, MAX})) {
            assertWalkCovers(top, BLOCK_SIZE, MAX);
            assertEquals("as one range", "" + TOP_BLOCK + "-" + MAX + " ", render(rangesOf(top)));
        }
        // A span of several blocks reaching the top, preceded by another the walk has to continue into.
        try (final WritableRowSet spanning = rspOf(new long[] {5, 9}, new long[] {MAX - 2 * BLOCK_SIZE + 1, MAX})) {
            assertWalkCovers(spanning, 5 + 2L * BLOCK_SIZE, MAX);
        }
    }

    @Test
    public void testFullBlockSpanFillsAKeyChunkToTheTopBlock() {
        try (final WritableRowSet rs = rspOf(new long[] {TOP_BLOCK, MAX});
                final WritableLongChunk<OrderedRowKeys> chunk =
                        WritableLongChunk.makeWritableChunk(BLOCK_SIZE + 8)) {
            chunk.setSize(0);
            rs.fillRowKeyChunk(chunk);
            assertEquals("keys filled", BLOCK_SIZE, chunk.size());
            assertEquals("first key", TOP_BLOCK, chunk.get(0));
            assertEquals("last key", MAX, chunk.get(chunk.size() - 1));
        }
    }

    // Forward iteration over a span reaching the top.

    @Test
    public void testForwardIterationStopsAtTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {5, 9}, new long[] {TOP_BLOCK, MAX});
                final RowSet.Iterator it = rs.iterator()) {
            final List<Long> keys = keysOf(it, rs.size() + 4);
            assertEquals("keys", 5 + BLOCK_SIZE, keys.size());
            assertEquals("last key", (Long) MAX, keys.get(keys.size() - 1));
        }
        try (final WritableRowSet rs = rspOf(new long[] {TOP_BLOCK, MAX});
                final RowSet.Iterator it = rs.iterator()) {
            final long[] count = {0};
            final long[] last = {-1};
            final long limit = rs.size() + 4;
            it.forEachLong(v -> {
                last[0] = v;
                return ++count[0] <= limit;
            });
            assertEquals("keys", BLOCK_SIZE, count[0]);
            assertEquals("last key", MAX, last[0]);
        }
    }

    // A search iterator tracks the key after the one it produced; at the top there is no key after it.

    @Test
    public void testSearchIterationStopsAtTheLastKey() {
        final long[][][] cases = {
                {{MAX, MAX}}, // a singleton at the very top
                {{MAX - 4, MAX}}, // a container ending at the top
                {{5, 9}, {TOP_BLOCK, MAX}}, // a full block span reaching the top, preceded by another span
        };
        for (final long[][] ranges : cases) {
            try (final WritableRowSet rs = rspOf(ranges);
                    final RowSet.SearchIterator it = rs.searchIterator()) {
                long count = 0, last = -1;
                while (it.hasNext()) {
                    last = it.nextLong();
                    if (++count > rs.size() + 4) {
                        fail("search iteration did not stop: " + count + " keys, last was " + last);
                    }
                }
                assertEquals("keys for " + render(rangesOf(rs)), rs.size(), count);
                assertEquals("last key for " + render(rangesOf(rs)), MAX, last);
                assertFalse("must be exhausted", it.hasNext());
            }
        }
    }

    @Test
    public void testSearchAdvanceToTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {5, 9}, new long[] {MAX - 2, MAX});
                final RowSet.SearchIterator it = rs.searchIterator()) {
            assertTrue("advance lands on the last key", it.advance(MAX));
            assertEquals("current value", MAX, it.currentValue());
            assertFalse("nothing follows the last key", it.hasNext());
        }
    }

    // A single range walked by stepping one past its end.

    @Test
    public void testSingleRangeWalkEndsAtTheLastKey() {
        try (final WritableRowSet rs = singleRangeOf(MAX - 4, MAX)) {
            assertEquals(List.of(MAX - 4, MAX - 3, MAX - 2, MAX - 1, MAX), keysOf(rs, 9));
            assertWalkCovers(rs, 5, MAX);
        }
        try (final WritableRowSet rs = singleRangeOf(MAX, MAX)) {
            assertEquals(List.of(MAX), keysOf(rs, 5));
        }
        // Through a row sequence, which shares the loop by way of the mixin.
        try (final WritableRowSet rs = singleRangeOf(MAX - 4, MAX);
                final RowSequence seq = rs.getRowSequenceByKeyRange(MAX - 3, MAX)) {
            assertEquals(List.of(MAX - 3, MAX - 2, MAX - 1, MAX), keysOf(seq, 8));
        }
    }

    @Test
    public void testSingleRangeReverseWalkStartsAtTheLastKey() {
        try (final WritableRowSet rs = singleRangeOf(MAX - 3, MAX)) {
            assertEquals(List.of(MAX, MAX - 1, MAX - 2, MAX - 3), reverseKeysOf(rs));
        }
        try (final WritableRowSet rs = singleRangeOf(MAX, MAX)) {
            assertEquals(List.of(MAX), reverseKeysOf(rs));
        }
        try (final WritableRowSet rs = singleRangeOf(MAX - 5, MAX);
                final RowSet.SearchIterator it = rs.reverseIterator()) {
            assertTrue("advance to a key inside the range", it.advance(MAX - 2));
            assertEquals("current value", MAX - 2, it.currentValue());
            assertEquals("next going down", MAX - 3, it.nextLong());
        }
        try (final WritableRowSet rs = singleRangeOf(MAX - 5, MAX);
                final RowSet.SearchIterator it = rs.reverseIterator()) {
            assertFalse("advance below the range reports nothing", it.advance(MAX - 100));
        }
    }

    private static List<Long> reverseKeysOf(final RowSet rs) {
        final List<Long> keys = new ArrayList<>();
        try (final RowSet.SearchIterator it = rs.reverseIterator()) {
            while (it.hasNext()) {
                keys.add(it.nextLong());
                if (keys.size() > rs.size() + 4) {
                    fail("reverse iteration did not stop: " + keys.size() + " keys");
                }
            }
        }
        return keys;
    }

    private static List<Long> keysOf(final RowSet rs, final long limit) {
        try (final RowSequence seq = rs.getRowSequenceByPosition(0, rs.size())) {
            return keysOf(seq, limit);
        }
    }

    // A sorted ranges slice, stepping through each range up to its end.

    @Test
    public void testSortedRangesSliceEndsAtTheLastKey() {
        try (final WritableRowSet rs = sortedRangesOf(new long[] {5, 5}, new long[] {MAX - 3, MAX});
                final RowSequence whole = rs.getRowSequenceByKeyRange(4, MAX)) {
            assertEquals(List.of(5L, MAX - 3, MAX - 2, MAX - 1, MAX), keysOf(whole, 12));
        }
        // Only the top range, so the walk both starts and ends there.
        try (final WritableRowSet rs = sortedRangesOf(new long[] {5, 5}, new long[] {MAX - 2, MAX});
                final RowSequence top = rs.getRowSequenceByKeyRange(MAX - 2, MAX)) {
            assertEquals(List.of(MAX - 2, MAX - 1, MAX), keysOf(top, 10));
        }
        // And through the whole-sequence walk, which is a separate loop.
        try (final WritableRowSet rs = sortedRangesOf(new long[] {5, 7}, new long[] {MAX - 1, MAX});
                final RowSequence seq = rs.getRowSequenceByPosition(0, rs.size())) {
            assertEquals(List.of(5L, 6L, 7L, MAX - 1, MAX), keysOf(seq, 12));
        }
    }

    // Unioning sorted ranges compares a range's end against the next range's start by looking one past the end.

    /** The union of two sets of ranges, coalescing what touches, computed independently of the rowset code. */
    private static String expectedUnion(final long[][] a, final long[][] b) {
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
                // Adjacent or overlapping; guard the +1 so a previous end of MAX does not wrap here either.
                if (prev[1] == MAX || r[0] <= prev[1] + 1) {
                    prev[1] = Math.max(prev[1], r[1]);
                    continue;
                }
            }
            out.add(new long[] {r[0], r[1]});
        }
        return render(out);
    }

    /**
     * Compares ranges rather than keys: a rowset holding MAX cannot be enumerated key by key, and ranges also pin how
     * the union coalesces.
     */
    private static void checkInsert(final long[][] receiver, final long[][] argument) {
        final String expected = expectedUnion(receiver, argument);
        try (final WritableRowSet rs = sortedRangesOf(receiver);
                final WritableRowSet other = sortedRangesOf(argument)) {
            rs.insert(other);
            assertEquals("insert " + render(rangesOf(other)), expected, render(rangesOf(rs)));
        }
        try (final WritableRowSet rs = sortedRangesOf(receiver);
                final WritableRowSet other = sortedRangesOf(argument);
                final RowSet united = rs.union(other)) {
            assertEquals("union " + render(rangesOf(other)), expected, render(rangesOf(united)));
        }
    }

    @Test
    public void testSortedRangesUnionAtTheLastKey() {
        checkInsert(new long[][] {{MAX - 2, MAX}}, new long[][] {{196606, 196610}});
        checkInsert(new long[][] {{196606, 196610}}, new long[][] {{MAX - 2, MAX}});
        checkInsert(new long[][] {{MAX - 5, MAX}}, new long[][] {{MAX - 2, MAX}});
        checkInsert(new long[][] {{100, 200}, {MAX - 4, MAX}}, new long[][] {{150, 260}, {MAX - 6, MAX - 2}});
        checkInsert(new long[][] {{MAX - 3, MAX}}, new long[][] {{MAX - 8, MAX - 4}});
        checkInsert(new long[][] {{MAX, MAX}}, new long[][] {{5, 9}});
        checkInsert(new long[][] {{5, 9}}, new long[][] {{MAX, MAX}});
    }

    /**
     * One side holds only a range ending at MAX while the other still has several lower ranges, so the comparison that
     * looks past MAX is reached with the other side genuinely behind.
     */
    @Test
    public void testSortedRangesUnionWithOnlyTheTopRangeOnOneSide() {
        final long[][] lower = {{100, 200}, {196606, 196610}, {1L << 40, (1L << 40) + 3}};
        checkInsert(new long[][] {{MAX - 2, MAX}}, lower);
        checkInsert(lower, new long[][] {{MAX - 2, MAX}});
    }
}
