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
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
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
}
