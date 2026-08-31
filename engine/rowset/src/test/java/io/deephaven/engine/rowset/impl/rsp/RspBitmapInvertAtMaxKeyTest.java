//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Inverting keys that live in the last block of the key space. The block one past that block starts at
 * {@link Long#MAX_VALUE} plus one, and a span reaching the last key ends there too, so both bounds overflow when held
 * as an exclusive end. Read as signed values the overflowed bounds compare as though every key were past the end.
 *
 * <p>
 * The timeouts are deliberate: the failure this guards against is a loop that never terminates, and a timeout turns
 * that into a test failure rather than a hung build.
 */
public class RspBitmapInvertAtMaxKeyTest {

    private static final long MAX = Long.MAX_VALUE;
    /** The first key of the last block: the key space is a whole number of blocks. */
    private static final long TOP_BLOCK = MAX - BLOCK_SIZE + 1;

    private static WritableRowSet rspOf(final long[]... ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (final long[] r : ranges) {
            rb = rb.appendRangeUnsafe(r[0], r[1]);
        }
        rb.finishMutations();
        return new WritableRowSetImpl(rb);
    }

    private static List<String> invertRanges(final RowSet rs, final RowSet keys) {
        final List<String> out = new ArrayList<>();
        try (final RowSet positions = rs.invert(keys)) {
            positions.forEachRowKeyRange((s, e) -> {
                out.add(s + "-" + e);
                return true;
            });
        }
        return out;
    }

    /** A singleton span holding only the last key. */
    @Test(timeout = 30_000)
    public void testInvertSingletonAtTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {MAX, MAX});
                final RowSet keys = rs.copy()) {
            assertEquals(List.of("0-0"), invertRanges(rs, keys));
        }
    }

    /** A container span inside the last block. */
    @Test(timeout = 30_000)
    public void testInvertContainerInTheLastBlock() {
        try (final WritableRowSet rs = rspOf(new long[] {MAX - 5, MAX});
                final RowSet keys = rs.copy()) {
            assertEquals(List.of("0-5"), invertRanges(rs, keys));
        }
        // Keys spread across the bottom and the very top, so the walk has to reach the last block from elsewhere.
        try (final WritableRowSet rs = rspOf(new long[] {0, 0}, new long[] {MAX, MAX});
                final RowSet keys = rs.copy()) {
            assertEquals(List.of("0-1"), invertRanges(rs, keys));
        }
    }

    /** A full block span whose last block is the last block. */
    @Test(timeout = 30_000)
    public void testInvertFullBlockSpanReachingTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {TOP_BLOCK, MAX})) {
            try (final RowSet keys = rspOf(new long[] {MAX, MAX})) {
                assertEquals(List.of("65535-65535"), invertRanges(rs, keys));
            }
            try (final RowSet keys = rs.copy()) {
                assertEquals(List.of("0-" + (BLOCK_SIZE - 1)), invertRanges(rs, keys));
            }
        }
    }

    /** The keys argument on each backing: only a SingleRange argument takes the fast path that already worked. */
    @Test(timeout = 30_000)
    public void testInvertAtTheLastKeyForEveryKeysBacking() {
        try (final WritableRowSet rs = rspOf(new long[] {MAX - 5, MAX})) {
            try (final RowSet keys = new WritableRowSetImpl(SingleRange.make(MAX, MAX))) {
                assertEquals(List.of("5-5"), invertRanges(rs, keys));
            }
            try (final RowSet keys = new WritableRowSetImpl(SortedRanges.makeSingleRange(MAX - 1, MAX))) {
                assertEquals(List.of("4-5"), invertRanges(rs, keys));
            }
            try (final RowSet keys = new WritableRowSetImpl(RspBitmap.makeSingleRange(MAX - 1, MAX))) {
                assertEquals(List.of("4-5"), invertRanges(rs, keys));
            }
        }
    }

    /** Away from the top, for contrast: this path always worked and must keep working. */
    @Test(timeout = 30_000)
    public void testInvertAwayFromTheTop() {
        try (final WritableRowSet rs = rspOf(new long[] {5, 9}, new long[] {3 * BLOCK_SIZE, 3 * BLOCK_SIZE + 3});
                final RowSet keys = rs.copy()) {
            assertEquals(List.of("0-8"), invertRanges(rs, keys));
        }
        try (final WritableRowSet rs = rspOf(new long[] {2 * BLOCK_SIZE, 4 * BLOCK_SIZE - 1});
                final RowSet keys = rspOf(new long[] {3 * BLOCK_SIZE, 3 * BLOCK_SIZE})) {
            assertEquals(List.of("" + BLOCK_SIZE + "-" + BLOCK_SIZE), invertRanges(rs, keys));
        }
    }

    private static List<String> invertRanges(final RowSet rs, final RowSet keys, final long maxPosition) {
        final List<String> out = new ArrayList<>();
        try (final RowSet positions = rs.invert(keys, maxPosition)) {
            positions.forEachRowKeyRange((s, e) -> {
                out.add(s + "-" + e);
                return true;
            });
        }
        return out;
    }

    /**
     * Truncation reaches the container holding the keys, so it has to be exercised against each kind of container a
     * span can hold -- a rowset backed by one container type says nothing about the others.
     */
    @Test(timeout = 30_000)
    public void testInvertTruncatedForEverySpanKind() {
        for (final long[][] shape : new long[][][] {
                {{10, 10}, {20, 20}, {30, 30}}, // sparse keys: an array container
                {{10, 11}}, // two keys
                {{10, 10}}, // one key: a singleton span
                {{10, 20}}, // one run inside a block
                {{10, 12}, {20, 22}, {30, 32}}, // several runs
        }) {
            RspBitmap rb = RspBitmap.makeEmpty();
            for (final long[] r : shape) {
                rb = rb.addRange(r[0], r[1]);
            }
            rb.finishMutations();
            final String span = rb.spans[0] == null ? "singleton" : rb.spans[0].getClass().getSimpleName();
            try (final WritableRowSet rs = new WritableRowSetImpl(rb);
                    final RowSet keys = rs.copy()) {
                final long card = rs.size();
                for (long maxPos = 0; maxPos < card; ++maxPos) {
                    assertEquals(span + " span, maxPosition " + maxPos,
                            List.of("0-" + maxPos), invertRanges(rs, keys, maxPos));
                }
                assertEquals(span + ", maxPosition past the end", List.of("0-" + (card - 1)),
                        invertRanges(rs, keys, card + 10));
            }
        }
    }

    /** Enough scattered keys in one block that the span becomes a bitmap. */
    @Test(timeout = 30_000)
    public void testInvertTruncatedForABitmapSpan() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < 5000; ++i) {
            rb = rb.add(2L * i);
        }
        rb.finishMutations();
        assertEquals("the span is a bitmap", "BitmapContainer", rb.spans[0].getClass().getSimpleName());
        try (final WritableRowSet rs = new WritableRowSetImpl(rb);
                final RowSet keys = rs.copy()) {
            for (final long maxPos : new long[] {0, 1, 7, 4998, 4999, 6000}) {
                final long last = Math.min(maxPos, rs.size() - 1);
                assertEquals("bitmap span, maxPosition " + maxPos, List.of("0-" + last),
                        invertRanges(rs, keys, maxPos));
            }
        }
    }

    /** maxPosition is inclusive, and still applies at the top of the key space. */
    @Test(timeout = 30_000)
    public void testInvertTruncatedByMaxPositionAtTheTop() {
        try (final WritableRowSet rs = rspOf(new long[] {MAX - 5, MAX});
                final RowSet keys = rs.copy()) {
            assertEquals("six keys", 6, rs.size());
            // Positions 0 through maxPosition inclusive, and no more.
            assertEquals("maxPosition 0", List.of("0-0"), invertRanges(rs, keys, 0));
            assertEquals("maxPosition 2", List.of("0-2"), invertRanges(rs, keys, 2));
            assertEquals("maxPosition 4", List.of("0-4"), invertRanges(rs, keys, 4));
            // At and beyond the last position nothing is dropped.
            assertEquals("maxPosition 5", List.of("0-5"), invertRanges(rs, keys, 5));
            assertEquals("maxPosition past the end", List.of("0-5"), invertRanges(rs, keys, 100));
        }
        // The same over a full block span reaching the last key, where the truncation arithmetic uses the span length.
        try (final WritableRowSet rs = rspOf(new long[] {TOP_BLOCK, MAX});
                final RowSet keys = rs.copy()) {
            assertEquals("full block span, maxPosition 2", List.of("0-2"), invertRanges(rs, keys, 2));
            assertEquals("full block span, maxPosition at its end", List.of("0-" + (BLOCK_SIZE - 1)),
                    invertRanges(rs, keys, BLOCK_SIZE - 1));
        }
    }
}
