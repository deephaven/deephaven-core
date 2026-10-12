//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.renderRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Once the sequential builder holds a bitmap under construction, an RSP-backed row sequence or a chunk of keys is
 * spliced straight into that bitmap, leaving no block pending in the builder. A key or range appended afterwards may
 * fall in the block the bitmap now ends with; it joins that block's span rather than opening a second span for the same
 * block, so the built rowset validates and holds exactly the keys appended. Keys that fall behind the bitmap are still
 * rejected as out of order.
 */
public class SequentialBuilderAppendAfterBitmapSpliceTest {

    private static final long BLOCK = RspArray.BLOCK_SIZE;

    /**
     * Enough keys to outgrow sorted ranges, so the builder continues with a bitmap under construction. Returns the
     * ranges appended.
     */
    private static List<long[]> bitmapModePrefix(final RowSetBuilderSequential b) {
        final List<long[]> expected = new ArrayList<>();
        for (long k = 0; k < 4400; k += 2) {
            b.appendKey(k);
            expected.add(new long[] {k, k});
        }
        b.appendRange(2 * BLOCK, 2 * BLOCK + 10);
        expected.add(new long[] {2 * BLOCK, 2 * BLOCK + 10});
        // A key two blocks on flushes the pending range into the bitmap.
        b.appendKey(3 * BLOCK);
        expected.add(new long[] {3 * BLOCK, 3 * BLOCK});
        assertTrue("precondition: builder holds a bitmap", ((RspBitmapBuilderSequential) b).rb != null);
        return expected;
    }

    private static void assertBuilt(final List<long[]> expected, final RowSetBuilderSequential b) {
        try (final WritableRowSet built = b.build()) {
            assertTrue("expected a bitmap-backed rowset",
                    ((WritableRowSetImpl) built).getInnerSet() instanceof RspBitmap);
            built.validate("built rowset");
            assertEquals(render(expected), renderRanges(built));
        }
    }

    private static void appendRowSequence(final RowSetBuilderSequential b, final long start, final long end) {
        try (final WritableRowSet piece = rspOf(new long[] {start, end})) {
            b.appendRowSequence(piece);
        }
    }

    @Test
    public void testKeyInSameBlockAfterRowSequence() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final List<long[]> expected = bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        b.appendKey(base + 1);
        expected.add(new long[] {base + 1, base + 1});
        appendRowSequence(b, base + 3, base + 5);
        expected.add(new long[] {base + 3, base + 5});
        b.appendKey(base + 7);
        expected.add(new long[] {base + 7, base + 7});
        assertBuilt(expected, b);
    }

    @Test
    public void testRangeInSameBlockAfterRowSequence() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final List<long[]> expected = bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        appendRowSequence(b, base + 3, base + 5);
        expected.add(new long[] {base + 3, base + 5});
        b.appendRange(base + 7, base + 20);
        expected.add(new long[] {base + 7, base + 20});
        assertBuilt(expected, b);
    }

    @Test
    public void testAdjacentRangeInSameBlockAfterRowSequence() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final List<long[]> expected = bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        appendRowSequence(b, base + 3, base + 5);
        b.appendRange(base + 6, base + 20);
        expected.add(new long[] {base + 3, base + 20});
        assertBuilt(expected, b);
    }

    @Test
    public void testCrossBlockRangeAfterRowSequence() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final List<long[]> expected = bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        appendRowSequence(b, base + 3, base + 5);
        expected.add(new long[] {base + 3, base + 5});
        b.appendRange(base + 7, base + BLOCK + 3);
        expected.add(new long[] {base + 7, base + BLOCK + 3});
        assertBuilt(expected, b);
    }

    @Test
    public void testRangeFillingTheBlockAfterRowSequence() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final List<long[]> expected = bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        appendRowSequence(b, base, base + 5);
        b.appendRange(base + 6, base + 2 * BLOCK + 3);
        expected.add(new long[] {base, base + 2 * BLOCK + 3});
        assertBuilt(expected, b);
    }

    @Test
    public void testKeyInSameBlockAfterChunk() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final List<long[]> expected = bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        b.appendOrderedRowKeysChunk(LongChunk.chunkWrap(new long[] {base + 1, base + 3}));
        expected.add(new long[] {base + 1, base + 1});
        expected.add(new long[] {base + 3, base + 3});
        b.appendKey(base + 7);
        expected.add(new long[] {base + 7, base + 7});
        assertBuilt(expected, b);
    }

    @Test
    public void testCrossBlockRangeAfterChunk() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        final List<long[]> expected = bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        b.appendOrderedRowKeysChunk(LongChunk.chunkWrap(new long[] {base + 1, base + 3}));
        expected.add(new long[] {base + 1, base + 1});
        expected.add(new long[] {base + 3, base + 3});
        b.appendRange(base + 7, base + BLOCK + 3);
        expected.add(new long[] {base + 7, base + BLOCK + 3});
        assertBuilt(expected, b);
    }

    @Test
    public void testKeyBehindTheBitmapInSameBlockIsRejected() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        appendRowSequence(b, base + 3, base + 5);
        try {
            b.appendKey(base + 4);
            b.build().close();
            fail("expected the out of order key to be rejected");
        } catch (IllegalArgumentException | IllegalStateException expected) {
        }
    }

    @Test
    public void testKeyBehindTheBitmapInEarlierBlockIsRejected() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        bitmapModePrefix(b);
        final long base = 5 * BLOCK;
        appendRowSequence(b, base + 3, base + 5);
        try {
            b.appendKey(4 * BLOCK);
            b.build().close();
            fail("expected the out of order key to be rejected");
        } catch (IllegalArgumentException | IllegalStateException expected) {
        }
    }
}
