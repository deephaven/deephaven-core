//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OrderedLongSetBuilderSequentialTest {

    /**
     * Once the builder holds an {@link io.deephaven.engine.rowset.impl.rsp.RspBitmap}, ordered chunks and shifted sets
     * go straight into it, past the scalar appends' checks.
     */
    @Test
    public void testNegativeKeysRejectedInBitmapMode() {
        final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
        long key = 0;
        while (builder.rb == null) {
            builder.appendKey(key);
            key += 2;
        }
        try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(2)) {
            chunk.set(0, -4);
            chunk.set(1, -2);
            assertThrows(AssertionFailure.class, () -> builder.appendOrderedRowKeysChunk(chunk, 0, 2));
        }
        try (final WritableRowSet sparse = RowSetFactory.fromKeys(key, key + 2, key + 100_000)) {
            final OrderedLongSet inner = ((WritableRowSetImpl) sparse).getInnerSet();
            final long shift = -key - 1;
            assertThrows(AssertionFailure.class, () -> builder.appendOrderedLongSet(shift, inner));
        }
        try (final WritableRowSet high = RowSetFactory.fromKeys(1L << 40, Long.MAX_VALUE)) {
            final OrderedLongSet inner = ((WritableRowSetImpl) high).getInnerSet();
            // The first key shifts fine, but the last wraps past Long.MAX_VALUE.
            assertThrows(AssertionFailure.class, () -> builder.appendOrderedLongSet(65536, inner));
            final RspBitmapBuilderSequential rspBuilder = new RspBitmapBuilderSequential();
            rspBuilder.appendKey(0);
            assertThrows(AssertionFailure.class, () -> rspBuilder.appendOrderedLongSet(65536, inner));
        }
        builder.getOrderedLongSet().ixRelease();
    }

    @Test
    public void testBuildIsSingleUse() {
        // SortedRanges result branch: the second build must fail rather than return a bogus result, and
        // must not have disturbed the set already returned.
        final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
        builder.appendKey(1);
        builder.appendKey(5);
        final OrderedLongSet result = builder.getOrderedLongSet();
        assertEquals(2, result.ixCardinality());
        try {
            builder.getOrderedLongSet();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
        assertEquals(2, result.ixCardinality());
        assertEquals(5, result.ixLastKey());
        result.ixValidate();
        result.ixRelease();

        // SingleRange result branch.
        final OrderedLongSetBuilderSequential builder2 = new OrderedLongSetBuilderSequential();
        builder2.appendRange(1, 3);
        final OrderedLongSet single = builder2.getOrderedLongSet();
        assertEquals(3, single.ixCardinality());
        try {
            builder2.getOrderedLongSet();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
        single.ixRelease();

        // Empty result branch.
        final OrderedLongSetBuilderSequential builder3 = new OrderedLongSetBuilderSequential();
        assertEquals(OrderedLongSet.EMPTY, builder3.getOrderedLongSet());
        try {
            builder3.getOrderedLongSet();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testPublicBuildersAreSingleUse() {
        final RowSetBuilderSequential sequential = RowSetFactory.builderSequential();
        sequential.appendKey(7);
        try (final WritableRowSet rowSet = sequential.build()) {
            assertEquals(1, rowSet.size());
        }
        try {
            sequential.build();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }

        final RowSetBuilderRandom random = RowSetFactory.builderRandom();
        random.addKey(7);
        try (final WritableRowSet rowSet = random.build()) {
            assertEquals(1, rowSet.size());
        }
        try {
            random.build();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
    }

    /**
     * Mix every way of appending, including bitmap-backed sets that start in the block the builder already ends with
     * and ones that start after it, over blocks dense enough to hold bitmap containers and sparse enough for arrays.
     */
    @Test
    public void testMixedAppendsMatchKeys() {
        for (int seed = 0; seed < 20; ++seed) {
            final Random random = new Random(seed);
            final long[] keys = mixedDensityKeys(random);
            checkMixedAppends(random, keys, OrderedLongSetBuilderSequential::new);
            checkMixedAppends(random, keys, RspBitmapBuilderSequential::new);
        }
    }

    private static long[] mixedDensityKeys(final Random random) {
        final int blocks = 8;
        final long[] keys = new long[blocks * (int) BLOCK_SIZE];
        int count = 0;
        for (int bi = 0; bi < blocks; ++bi) {
            final long blockStart = bi * BLOCK_SIZE;
            // Spacing 2 gives singleton ranges at bitmap density, a run length above 1 gives run containers, and a
            // wide spacing gives array containers.
            final int spacing = new int[] {2, 2, 3, 40}[random.nextInt(4)];
            final int runLength = spacing == 3 ? 2 : 1;
            for (long offset = random.nextInt(spacing); offset + runLength <= BLOCK_SIZE; offset += spacing) {
                for (int ri = 0; ri < runLength; ++ri) {
                    keys[count++] = blockStart + offset + ri;
                }
            }
        }
        final long[] result = new long[count];
        System.arraycopy(keys, 0, result, 0, count);
        return result;
    }

    private static void checkMixedAppends(final Random random, final long[] keys,
            final Supplier<RspBitmapBuilderSequential> builderFactory) {
        final RspBitmapBuilderSequential builder = builderFactory.get();
        int pos = 0;
        while (pos < keys.length) {
            final int end = Math.min(keys.length, pos + 1 + random.nextInt(random.nextBoolean() ? 8 : 3000));
            switch (random.nextInt(3)) {
                case 0:
                    for (int ki = pos; ki < end; ++ki) {
                        builder.appendKey(keys[ki]);
                    }
                    break;
                case 1:
                    try (final WritableLongChunk<OrderedRowKeys> chunk =
                            WritableLongChunk.makeWritableChunk(end - pos)) {
                        for (int ki = pos; ki < end; ++ki) {
                            chunk.set(ki - pos, keys[ki]);
                        }
                        builder.appendOrderedRowKeysChunk(chunk, 0, end - pos);
                    }
                    break;
                default:
                    // Store the keys lower down by a shift that is block aligned, unaligned, or zero, as a bitmap even
                    // when it is small, and have the builder shift them back up.
                    final long first = keys[pos];
                    final long shift;
                    switch (random.nextInt(3)) {
                        case 0:
                            shift = 0;
                            break;
                        case 1:
                            shift = (first / BLOCK_SIZE) * BLOCK_SIZE;
                            break;
                        default:
                            shift = random.nextInt((int) Math.min(first, BLOCK_SIZE) + 1);
                            break;
                    }
                    final RspBitmap piece = new RspBitmap();
                    for (int ki = pos; ki < end; ++ki) {
                        piece.appendRangeUnsafeNoWriteCheck(keys[ki] - shift, keys[ki] - shift);
                    }
                    piece.finishMutations();
                    builder.appendOrderedLongSet(shift, piece);
                    piece.ixRelease();
                    break;
            }
            pos = end;
        }
        final OrderedLongSet result = builder.getOrderedLongSet();
        result.ixValidate();
        assertEquals(keys.length, result.ixCardinality());
        final int[] index = {0};
        result.ixForEachLong((final long key) -> {
            assertEquals(keys[index[0]++], key);
            return true;
        });
        result.ixRelease();
    }

    /**
     * A bitmap-backed set that starts in the builder's last block, below a key already appended, is rejected, both when
     * that key came from sharing an earlier set's containers and when it came from an ordered chunk.
     */
    @Test
    public void testOutOfOrderSetInSharedBlockRejected() {
        for (final boolean afterSharedAppend : new boolean[] {true, false}) {
            final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
            // Enough separate ranges to commit the builder to a bitmap.
            long key = 0;
            while (builder.rb == null) {
                builder.appendKey(key);
                key += 2;
            }
            final long lastBlock = 4 * BLOCK_SIZE;
            if (afterSharedAppend) {
                final RspBitmap shared = new RspBitmap();
                shared.appendRangeUnsafeNoWriteCheck(lastBlock + 1000, lastBlock + 1000);
                shared.appendRangeUnsafeNoWriteCheck(lastBlock + 2000, lastBlock + 2000);
                shared.finishMutations();
                builder.appendOrderedLongSet(0, shared);
                shared.ixRelease();
            } else {
                builder.appendRange(lastBlock + 1000, lastBlock + 1001);
                try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(2)) {
                    chunk.set(0, lastBlock + 1900);
                    chunk.set(1, lastBlock + 2000);
                    builder.appendOrderedRowKeysChunk(chunk, 0, 2);
                }
            }
            final RspBitmap late = new RspBitmap();
            late.appendRangeUnsafeNoWriteCheck(lastBlock + 1500, lastBlock + 1500);
            late.finishMutations();
            assertThrows(IllegalArgumentException.class, () -> builder.appendOrderedLongSet(0, late));
            late.ixRelease();
        }
    }

    /**
     * An ordered chunk that starts at or below a key the builder already holds in its bitmap is rejected, both when
     * that key came from sharing an earlier set's containers and when it came from a range.
     */
    @Test
    public void testOutOfOrderChunkInBitmapModeRejected() {
        for (final boolean afterSharedAppend : new boolean[] {true, false}) {
            final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
            // Enough separate ranges to commit the builder to a bitmap.
            long key = 0;
            while (builder.rb == null) {
                builder.appendKey(key);
                key += 2;
            }
            final long lastBlock = 4 * BLOCK_SIZE;
            if (afterSharedAppend) {
                final RspBitmap shared = new RspBitmap();
                shared.appendRangeUnsafeNoWriteCheck(lastBlock + 1000, lastBlock + 1000);
                shared.appendRangeUnsafeNoWriteCheck(lastBlock + 2000, lastBlock + 2000);
                shared.finishMutations();
                builder.appendOrderedLongSet(0, shared);
                shared.ixRelease();
            } else {
                builder.appendRange(lastBlock + 1000, lastBlock + 2000);
            }
            try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(2)) {
                chunk.set(0, lastBlock + 2000);
                chunk.set(1, lastBlock + 3000);
                assertThrows(IllegalArgumentException.class, () -> builder.appendOrderedRowKeysChunk(chunk, 0, 2));
            }
        }
    }

    /**
     * Appending a set can leave its last range in the pending container with no pending range, which is where a key or
     * range appended next is checked: one that falls inside that container is rejected rather than appended out of
     * order.
     */
    @Test
    public void testOutOfOrderAppendAfterSetRejected() {
        for (final Supplier<RspBitmapBuilderSequential> factory : List.<Supplier<RspBitmapBuilderSequential>>of(
                RspBitmapBuilderSequential::new, OrderedLongSetBuilderSequential::new)) {
            for (final boolean appendKey : new boolean[] {true, false}) {
                final RspBitmapBuilderSequential builder = factory.get();
                // Enough separate ranges to commit the builder to a bitmap.
                long key = 0;
                while (builder.rb == null) {
                    builder.appendKey(key);
                    key += 2;
                }
                final long lastBlock = 4 * BLOCK_SIZE;
                builder.appendRange(lastBlock + 1, lastBlock + 2);
                builder.appendOrderedLongSet(0, SingleRange.make(lastBlock + 10, lastBlock + 20));
                if (appendKey) {
                    builder.appendKey(lastBlock + 15);
                } else {
                    builder.appendRange(lastBlock + 15, lastBlock + 16);
                }
                assertThrows(IllegalStateException.class, builder::getOrderedLongSet);
            }
        }
    }

    /**
     * Converting a {@link SortedRanges} sees every range before it fills a container, so each container is created at
     * its final size and has no capacity to spare.
     */
    @Test
    public void testSortedRangesToRspIsCompact() {
        SortedRanges sr = SortedRanges.makeEmpty();
        for (int ki = 0; ki < 1000; ++ki) {
            sr = sr.append(2L * ki);
            assertNotNull(sr);
        }
        final RspBitmap rsp = sr.toRsp();
        rsp.ixValidate();
        assertEquals(1000, rsp.getCardinality());
        assertTrue("containerOverhead=" + rsp.containerOverhead(), rsp.containerOverhead() < 0.01);
        rsp.ixRelease();
        sr.ixRelease();
    }

    /**
     * Ranges appended to a block the builder's bitmap ends with, after a set was shared into it, leave that block's
     * container with no capacity to spare once the builder is done.
     */
    @Test
    public void testAppendsAfterSharedSetLeaveContainerCompact() {
        final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
        // Enough separate ranges to commit the builder to a bitmap.
        long key = 0;
        long count = 0;
        while (builder.rb == null) {
            builder.appendKey(key);
            key += 2;
            ++count;
        }
        final long lastBlock = 4 * BLOCK_SIZE;
        final RspBitmap shared = new RspBitmap();
        shared.appendRangeUnsafeNoWriteCheck(lastBlock, lastBlock);
        shared.finishMutations();
        builder.appendOrderedLongSet(0, shared);
        shared.ixRelease();
        for (int ki = 1; ki < 1000; ++ki) {
            builder.appendRange(lastBlock + 2L * ki, lastBlock + 2L * ki);
        }
        final OrderedLongSet result = builder.getOrderedLongSet();
        result.ixValidate();
        assertEquals(count + 1000, result.ixCardinality());
        final double overhead = ((RspBitmap) result).containerOverhead();
        assertTrue("containerOverhead=" + overhead, overhead < 0.01);
        result.ixRelease();
    }

    /**
     * Alternating ordered chunks and ranges within one block moves its container between the bitmap and the pending
     * container on every append. The result holds every key, and a block filled this way joins the full block before
     * it.
     */
    @Test
    public void testAlternatingChunksAndRangesInOneBlock() {
        for (final Supplier<RspBitmapBuilderSequential> factory : List.<Supplier<RspBitmapBuilderSequential>>of(
                RspBitmapBuilderSequential::new, OrderedLongSetBuilderSequential::new)) {
            final RspBitmapBuilderSequential builder = factory.get();
            // Enough separate ranges to commit the builder to a bitmap.
            long key = 0;
            long count = 0;
            while (builder.rb == null) {
                builder.appendKey(key);
                key += 2;
                ++count;
            }
            final long fullBlock = 4 * BLOCK_SIZE;
            builder.appendRange(fullBlock, fullBlock + BLOCK_SIZE - 1);
            final long block = fullBlock + BLOCK_SIZE;
            try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(1)) {
                for (long blockKey = block; blockKey < block + BLOCK_SIZE; blockKey += 2) {
                    chunk.set(0, blockKey);
                    builder.appendOrderedRowKeysChunk(chunk, 0, 1);
                    builder.appendRange(blockKey + 1, blockKey + 1);
                }
            }
            final OrderedLongSet result = builder.getOrderedLongSet();
            result.ixValidate();
            assertEquals(count + 2 * BLOCK_SIZE, result.ixCardinality());
            assertTrue(result.ixContainsRange(fullBlock, block + BLOCK_SIZE - 1));
            result.ixRelease();
        }
    }

    private static RspBitmap bitmapOf(final long... keys) {
        final RspBitmap rsp = new RspBitmap();
        for (final long key : keys) {
            rsp.appendRangeUnsafeNoWriteCheck(key, key);
        }
        rsp.finishMutations();
        return rsp;
    }

    private static void assertKeys(final OrderedLongSet result, final long... keys) {
        result.ixValidate();
        assertEquals(keys.length, result.ixCardinality());
        for (final long key : keys) {
            assertTrue("missing " + key, result.ixContainsRange(key, key));
        }
    }

    /**
     * A bitmap set shifted down is checked against its first block for keys below zero, and against its exact first key
     * only when that block would move below zero.
     */
    @Test
    public void testNegativeShiftOfBitmapSet() {
        for (final Supplier<RspBitmapBuilderSequential> factory : List.<Supplier<RspBitmapBuilderSequential>>of(
                RspBitmapBuilderSequential::new, OrderedLongSetBuilderSequential::new)) {
            // The block moves below zero, but the keys do not.
            final RspBitmap high = bitmapOf(BLOCK_SIZE + 100, BLOCK_SIZE + 200);
            final RspBitmapBuilderSequential fits = factory.get();
            fits.appendOrderedLongSet(-(BLOCK_SIZE + 50), high);
            final OrderedLongSet fitsResult = fits.getOrderedLongSet();
            assertKeys(fitsResult, 50, 150);
            fitsResult.ixRelease();

            // The first key moves below zero.
            final RspBitmapBuilderSequential below = factory.get();
            assertThrows(AssertionFailure.class, () -> below.appendOrderedLongSet(-(BLOCK_SIZE + 150), high));
            high.ixRelease();

            // A block-aligned shift down to a block after the builder's keys, which shares the set's spans.
            final RspBitmapBuilderSequential shared = factory.get();
            long key = 0;
            while (shared.rb == null) {
                shared.appendKey(key);
                key += 2;
            }
            final long heldKeys = key / 2;
            final RspBitmap far = bitmapOf(9 * BLOCK_SIZE + 7, 10 * BLOCK_SIZE + 8);
            shared.appendOrderedLongSet(-4 * BLOCK_SIZE, far);
            far.ixRelease();
            final OrderedLongSet sharedResult = shared.getOrderedLongSet();
            sharedResult.ixValidate();
            assertEquals(heldKeys + 2, sharedResult.ixCardinality());
            assertTrue(sharedResult.ixContainsRange(5 * BLOCK_SIZE + 7, 5 * BLOCK_SIZE + 7));
            assertTrue(sharedResult.ixContainsRange(6 * BLOCK_SIZE + 8, 6 * BLOCK_SIZE + 8));
            sharedResult.ixRelease();
        }
    }

    /**
     * A bitmap set that starts after everything the builder holds is shared whatever the builder has pending: a range,
     * a container with no range, or nothing.
     */
    @Test
    public void testShareSetAfterEachPendingState() {
        for (final Supplier<RspBitmapBuilderSequential> factory : List.<Supplier<RspBitmapBuilderSequential>>of(
                RspBitmapBuilderSequential::new, OrderedLongSetBuilderSequential::new)) {
            for (int state = 0; state < 3; ++state) {
                final RspBitmapBuilderSequential builder = factory.get();
                // Enough separate ranges to commit the builder to a bitmap.
                long key = 0;
                while (builder.rb == null) {
                    builder.appendKey(key);
                    key += 2;
                }
                final long heldKeys = key / 2;
                final long block = 4 * BLOCK_SIZE;
                switch (state) {
                    case 0:
                        // A pending range.
                        builder.appendRange(block + 10, block + 20);
                        break;
                    case 1:
                        // A set that is not a bitmap goes straight to the pending container, leaving no range.
                        builder.appendOrderedLongSet(0, SingleRange.make(block + 10, block + 20));
                        break;
                    default:
                        // An ordered chunk goes straight into the bitmap, leaving nothing pending.
                        try (final WritableLongChunk<OrderedRowKeys> chunk =
                                WritableLongChunk.makeWritableChunk(11)) {
                            for (int ki = 0; ki < 11; ++ki) {
                                chunk.set(ki, block + 10 + ki);
                            }
                            builder.appendOrderedRowKeysChunk(chunk, 0, 11);
                        }
                        break;
                }
                final RspBitmap later = bitmapOf(6 * BLOCK_SIZE + 3, 6 * BLOCK_SIZE + 5, 7 * BLOCK_SIZE + 1);
                builder.appendOrderedLongSet(0, later);
                later.ixRelease();
                // A key after the shared set extends its last block.
                builder.appendKey(7 * BLOCK_SIZE + 9);
                final OrderedLongSet result = builder.getOrderedLongSet();
                result.ixValidate();
                assertEquals("state=" + state, heldKeys + 11 + 4, result.ixCardinality());
                assertTrue(result.ixContainsRange(block + 10, block + 20));
                for (final long expected : new long[] {
                        6 * BLOCK_SIZE + 3, 6 * BLOCK_SIZE + 5, 7 * BLOCK_SIZE + 1, 7 * BLOCK_SIZE + 9}) {
                    assertTrue("state=" + state + " missing " + expected,
                            result.ixContainsRange(expected, expected));
                }
                result.ixRelease();
            }
        }
    }

    /**
     * A bitmap-only builder's result is a bitmap whatever it holds, so it shares a block-aligned bitmap set's spans
     * before it has a bitmap of its own, whatever it has pending; an unaligned shift is appended a range at a time.
     */
    @Test
    public void testBitmapOnlyBuilderSharesSetBeforeItHasABitmap() {
        for (int state = 0; state < 3; ++state) {
            for (final long shift : new long[] {2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 7}) {
                final RspBitmapBuilderSequential builder = new RspBitmapBuilderSequential();
                final List<Long> expected = new java.util.ArrayList<>();
                if (state >= 1) {
                    // A pending range.
                    builder.appendRange(10, 12);
                    expected.addAll(List.of(10L, 11L, 12L));
                }
                if (state == 2) {
                    // The range before goes to the pending container, and this one is pending.
                    builder.appendRange(30, 31);
                    expected.addAll(List.of(30L, 31L));
                }
                final RspBitmap set = bitmapOf(BLOCK_SIZE + 3, BLOCK_SIZE + 5, 2 * BLOCK_SIZE + 1);
                builder.appendOrderedLongSet(shift, set);
                set.ixRelease();
                expected.addAll(List.of(BLOCK_SIZE + 3 + shift, BLOCK_SIZE + 5 + shift, 2 * BLOCK_SIZE + 1 + shift));
                final OrderedLongSet result = builder.getOrderedLongSet();
                assertKeys(result, expected.stream().mapToLong(Long::longValue).toArray());
                result.ixRelease();
            }
        }
    }

    /**
     * The ordered builder keeps a bitmap set that would fit in a {@link SortedRanges} from turning its result into a
     * bitmap, and commits to one for a set with more ranges than a SortedRanges holds, moving the ranges it collected
     * before into the bitmap first.
     */
    @Test
    public void testOrderedBuilderCommitsToBitmapOnlyForLargeSets() {
        final OrderedLongSetBuilderSequential small = new OrderedLongSetBuilderSequential();
        small.appendRange(1, 2);
        final RspBitmap smallSet = bitmapOf(10, 20, 3 * BLOCK_SIZE + 5);
        small.appendOrderedLongSet(0, smallSet);
        smallSet.ixRelease();
        final OrderedLongSet smallResult = small.getOrderedLongSet();
        assertTrue(smallResult.getClass().getName(), smallResult instanceof SortedRanges);
        assertKeys(smallResult, 1, 2, 10, 20, 3 * BLOCK_SIZE + 5);
        smallResult.ixRelease();

        // A singleton in every other block: as many ranges as spans, none joined across blocks.
        final long[] keys = new long[SortedRanges.MAX_CAPACITY + 1];
        for (int ki = 0; ki < keys.length; ++ki) {
            keys[ki] = 2 * BLOCK_SIZE * (ki + 1) + 7;
        }
        final RspBitmap largeSet = bitmapOf(keys);
        assertTrue(largeSet.hasMoreRangesThan(SortedRanges.MAX_CAPACITY));
        final OrderedLongSetBuilderSequential large = new OrderedLongSetBuilderSequential();
        // A collected SortedRanges and a pending range.
        large.appendRange(1, 2);
        large.appendKey(5);
        large.appendOrderedLongSet(0, largeSet);
        largeSet.ixRelease();
        final OrderedLongSet largeResult = large.getOrderedLongSet();
        assertTrue(largeResult instanceof RspBitmap);
        final long[] expected = new long[keys.length + 3];
        expected[0] = 1;
        expected[1] = 2;
        expected[2] = 5;
        System.arraycopy(keys, 0, expected, 3, keys.length);
        assertKeys(largeResult, expected);
        largeResult.ixRelease();
    }
}
