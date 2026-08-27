//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * A full block span at the very top of the key space ends at {@link Long#MAX_VALUE}. Walking it by comparing against
 * one key past its end wraps to a negative bound, which reads as though the span were empty.
 */
public class RspArrayForEachAtMaxKeyTest {

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

    /** forAllRowKeys must visit every key of a full block span that ends at the last key. */
    @Test
    public void testForAllRowKeysOverTheTopBlock() {
        try (final WritableRowSet rs = rspOf(new long[] {TOP_BLOCK, MAX})) {
            final long[] count = {0};
            final long[] first = {-1}, last = {-1};
            rs.forAllRowKeys(k -> {
                if (count[0] == 0) {
                    first[0] = k;
                }
                last[0] = k;
                ++count[0];
            });
            assertEquals("keys visited", BLOCK_SIZE, count[0]);
            assertEquals("first key", TOP_BLOCK, first[0]);
            assertEquals("last key", MAX, last[0]);
        }
    }

    /** The same span reached through a chunk fill. */
    @Test
    public void testFillRowKeyChunkOverTheTopBlock() {
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

    /** A multi-block span reaching the top, plus an earlier span so the walk has to continue into it. */
    @Test
    public void testForAllRowKeysOverAMultiBlockTopSpan() {
        try (final WritableRowSet rs = rspOf(new long[] {5, 9},
                new long[] {MAX - 2 * BLOCK_SIZE + 1, MAX})) {
            final long[] count = {0};
            final long[] last = {-1};
            rs.forAllRowKeys(k -> {
                last[0] = k;
                ++count[0];
            });
            assertEquals("keys visited", 5 + 2L * BLOCK_SIZE, count[0]);
            assertEquals("last key", MAX, last[0]);
        }
    }

    /** Ranges, which take a different path, must agree about the same span. */
    @Test
    public void testForEachRowKeyRangeOverTheTopBlock() {
        try (final WritableRowSet rs = rspOf(new long[] {TOP_BLOCK, MAX})) {
            final long[] seen = {0};
            final long[] bounds = {-1, -1};
            rs.forEachRowKeyRange((s, e) -> {
                bounds[0] = s;
                bounds[1] = e;
                ++seen[0];
                return true;
            });
            assertEquals("one range", 1, seen[0]);
            assertEquals("range start", TOP_BLOCK, bounds[0]);
            assertEquals("range end", MAX, bounds[1]);
        }
    }
}
