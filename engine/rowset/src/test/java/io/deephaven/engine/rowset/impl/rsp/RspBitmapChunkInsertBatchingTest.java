//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * {@link RspBitmap#addValuesUnsafeNoWriteCheck} collects the blocks it does not have yet and inserts them in one pass.
 * These exercise the cases where that has to interact with a block becoming a full block span: a promotion that merges
 * with a neighbour, two promotions for adjacent blocks (which have to end up as a single span), and promotions mixed in
 * among new blocks.
 */
public class RspBitmapChunkInsertBatchingTest {

    private static final long BS = BLOCK_SIZE;

    private static RspBitmap addValues(final RspBitmap rb, final long... values) {
        final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(values.length);
        for (int i = 0; i < values.length; ++i) {
            chunk.set(i, values[i]);
        }
        chunk.setSize(values.length);
        final RspBitmap w = rb.writeCheck();
        w.addValuesUnsafeNoWriteCheck(chunk, 0, values.length);
        w.finishMutations();
        w.validate("after addValues");
        return w;
    }

    private static void assertKeysAre(final TreeSet<Long> expected, final RspBitmap actual) {
        final List<Long> got = new ArrayList<>();
        actual.forEachLong(v -> {
            got.add(v);
            return true;
        });
        assertEquals(new ArrayList<>(expected), got);
    }

    /** All of one block's keys arriving at once, for a block we do not have, becomes a full block span. */
    @Test
    public void testNewBlockFilledCompletely() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(5, 40);
        rb = rb.appendRangeUnsafe(8L * BS + 5, 8L * BS + 40);
        rb.finishMutations();

        final TreeSet<Long> expected = new TreeSet<>();
        rb.forEachLong(v -> {
            expected.add(v);
            return true;
        });
        final long[] values = new long[(int) BS];
        for (int i = 0; i < BS; ++i) {
            values[i] = 4L * BS + i;
            expected.add(values[i]);
        }
        assertKeysAre(expected, addValues(rb, values));
    }

    /** Two adjacent blocks both filled completely: adjacent full block spans have to become one span. */
    @Test
    public void testTwoAdjacentNewBlocksFilledCompletely() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(5, 40);
        rb = rb.appendRangeUnsafe(10L * BS + 5, 10L * BS + 40);
        rb.finishMutations();

        final TreeSet<Long> expected = new TreeSet<>();
        rb.forEachLong(v -> {
            expected.add(v);
            return true;
        });
        final long[] values = new long[(int) (2 * BS)];
        for (int i = 0; i < 2 * BS; ++i) {
            values[i] = 4L * BS + i; // blocks 4 and 5, both entirely
            expected.add(values[i]);
        }
        final RspBitmap result = addValues(rb, values);
        assertKeysAre(expected, result);
    }

    /** A promotion that has to merge with a full block span already sitting to its left. */
    @Test
    public void testPromotionMergesWithFullBlockSpanOnLeft() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(0, 2L * BS - 1); // full block span over blocks 0 and 1
        rb = rb.appendRangeUnsafe(2L * BS + 5, 2L * BS + 40); // container in block 2
        rb = rb.appendRangeUnsafe(9L * BS + 1, 9L * BS + 3);
        rb.finishMutations();

        final TreeSet<Long> expected = new TreeSet<>();
        rb.forEachLong(v -> {
            expected.add(v);
            return true;
        });
        // Fill block 2 completely, so it merges left into the existing full block span.
        final long[] values = new long[(int) BS];
        for (int i = 0; i < BS; ++i) {
            values[i] = 2L * BS + i;
            expected.add(values[i]);
        }
        assertKeysAre(expected, addValues(rb, values));
    }

    /** New blocks arriving before and after a promotion that absorbs, which stops any further batching. */
    @Test
    public void testNewBlocksAroundAnAbsorbingPromotion() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(0, 2L * BS - 1); // full block span over blocks 0 and 1
        rb = rb.appendRangeUnsafe(2L * BS + 5, 2L * BS + 40); // container in block 2
        for (int i = 4; i < 12; i += 2) {
            rb = rb.appendRangeUnsafe(i * BS + 5, i * BS + 40);
        }
        rb.finishMutations();

        final TreeSet<Long> expected = new TreeSet<>();
        rb.forEachLong(v -> {
            expected.add(v);
            return true;
        });
        final List<Long> values = new ArrayList<>();
        // A new block before the promotion...
        values.add(3L * BS + 7);
        // ...the promotion of block 2, which merges left...
        for (int i = 0; i < BS; ++i) {
            values.add(2L * BS + (long) i);
        }
        // ...and new blocks after it.
        values.add(5L * BS + 9);
        values.add(7L * BS + 11);
        values.sort(null);
        expected.addAll(values);
        final long[] arr = new long[values.size()];
        for (int i = 0; i < arr.length; ++i) {
            arr[i] = values.get(i);
        }
        assertKeysAre(expected, addValues(rb, arr));
    }

    /**
     * New blocks queued both before and after a promotion that absorbs a span of ours. The queued positions were taken
     * against arrays that still hold the absorbed span, so they have to be reconciled with its removal: the one before
     * it keeps its position, the ones after it move down by one.
     */
    @Test
    public void testNewBlocksAroundAPromotionThatAbsorbs() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(1L * BS + 100, 1L * BS + 140); // container, block 1
        rb = rb.appendRangeUnsafe(4L * BS, 5L * BS - 1); // full block span, block 4
        rb = rb.appendRangeUnsafe(5L * BS + 100, 5L * BS + 140); // container, block 5
        rb = rb.appendRangeUnsafe(7L * BS + 100, 7L * BS + 140);
        rb = rb.appendRangeUnsafe(9L * BS + 100, 9L * BS + 140);
        rb.finishMutations();

        final TreeSet<Long> expected = new TreeSet<>();
        rb.forEachLong(v -> {
            expected.add(v);
            return true;
        });
        final List<Long> values = new ArrayList<>();
        values.add(0L * BS + 7); // new block 0, queued before the promotion
        for (int i = 0; i < BS; ++i) {
            values.add(5L * BS + i); // fills block 5, merging left into block 4 and absorbing its container
        }
        values.add(6L * BS + 7); // new blocks queued after the promotion
        values.add(8L * BS + 7);
        values.add(10L * BS + 7);
        expected.addAll(values);
        final long[] arr = new long[values.size()];
        for (int i = 0; i < arr.length; ++i) {
            arr[i] = values.get(i);
        }
        assertKeysAre(expected, addValues(rb, arr));
    }

    /** More than one absorbing promotion in the same chunk, with new blocks between them. */
    @Test
    public void testSeveralAbsorbingPromotions() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int base = 0; base < 24; base += 8) {
            rb = rb.appendRangeUnsafe(base * BS, (base + 1L) * BS - 1); // full block span
            rb = rb.appendRangeUnsafe((base + 1L) * BS + 100, (base + 1L) * BS + 140); // container next to it
            rb = rb.appendRangeUnsafe((base + 5L) * BS + 100, (base + 5L) * BS + 140);
        }
        rb.finishMutations();

        final TreeSet<Long> expected = new TreeSet<>();
        rb.forEachLong(v -> {
            expected.add(v);
            return true;
        });
        final List<Long> values = new ArrayList<>();
        for (int base = 0; base < 24; base += 8) {
            for (int i = 0; i < BS; ++i) {
                values.add((base + 1L) * BS + i); // fills the container block, merging left
            }
            values.add((base + 3L) * BS + 7); // a new block after each promotion
            values.add((base + 6L) * BS + 7);
        }
        expected.addAll(values);
        final long[] arr = new long[values.size()];
        for (int i = 0; i < arr.length; ++i) {
            arr[i] = values.get(i);
        }
        assertKeysAre(expected, addValues(rb, arr));
    }

    /** Randomized: new blocks, existing blocks and full-block promotions interleaved. */
    @Test
    public void testRandomMixOfNewBlocksAndPromotions() {
        final Random rand = new Random(1234);
        for (int trial = 0; trial < 60; ++trial) {
            RspBitmap rb = RspBitmap.makeEmpty();
            final TreeSet<Long> expected = new TreeSet<>();
            // A receiver with containers, singletons and a couple of full block spans.
            for (int block = 0; block < 40; ++block) {
                if (rand.nextInt(4) == 0) {
                    continue; // leave this block absent
                }
                final long base = block * BS;
                if (rand.nextInt(8) == 0) {
                    rb = rb.appendRangeUnsafe(base, base + BLOCK_LAST); // full block span
                    for (long v = base; v <= base + BLOCK_LAST; ++v) {
                        expected.add(v);
                    }
                } else {
                    final long start = base + rand.nextInt(100);
                    final long end = start + rand.nextInt(50);
                    rb = rb.appendRangeUnsafe(start, end);
                    for (long v = start; v <= end; ++v) {
                        expected.add(v);
                    }
                }
            }
            if (rb.isEmpty()) {
                continue;
            }
            rb.finishMutations();

            // Values spread over blocks that may be present, absent, or about to be filled entirely.
            final TreeSet<Long> toAdd = new TreeSet<>();
            for (int k = 0; k < 12; ++k) {
                final int block = rand.nextInt(44);
                final long base = block * BS;
                if (rand.nextInt(5) == 0) {
                    for (long v = base; v <= base + BLOCK_LAST; ++v) {
                        toAdd.add(v); // fill it completely -> promotion
                    }
                } else {
                    toAdd.add(base + rand.nextInt((int) BS));
                }
            }
            expected.addAll(toAdd);
            final long[] arr = new long[toAdd.size()];
            int i = 0;
            for (final long v : toAdd) {
                arr[i++] = v;
            }
            assertKeysAre(expected, addValues(rb, arr));
        }
    }
}
