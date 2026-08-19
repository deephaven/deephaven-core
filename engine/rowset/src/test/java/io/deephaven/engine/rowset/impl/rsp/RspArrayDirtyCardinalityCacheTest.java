//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Position lookups while the cardinality accumulator is allocated but stale, which is the state between unsafe
 * mutations and {@code finishMutations()}. {@link RspArray#get(long)} supports it by design; its bulk sibling
 * {@link RspArray#getKeysForPositions} must agree rather than reading the stale accumulator.
 */
public class RspArrayDirtyCardinalityCacheTest {

    /** 12 singleton spans (enough for an allocated acc), then an interior insert that leaves the acc stale. */
    private static RspBitmap makeWithStaleAcc() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int b = 0; b < 12; ++b) {
            rb = rb.appendUnsafe((2L * b) * BLOCK_SIZE + 3);
        }
        rb.finishMutations();
        rb = rb.addUnsafe(BLOCK_SIZE + 5);
        return rb;
    }

    private static List<Long> keysForPositions(final RspBitmap rb, final long... positions) {
        final List<Long> out = new ArrayList<>();
        rb.getKeysForPositions(LongStream.of(positions).iterator(), out::add);
        return out;
    }

    @Test
    public void testGetKeysForPositionsWithStaleAcc() {
        final RspBitmap dirty = makeWithStaleAcc();

        // The expected keys, from the same bitmap once its cache is clean.
        final RspBitmap clean = makeWithStaleAcc();
        clean.finishMutations();
        final long cardinality = clean.getCardinality();
        final List<Long> expected = new ArrayList<>();
        for (long p = 0; p < cardinality; ++p) {
            expected.add(clean.get(p));
        }

        // get() already supports the dirty state; it is the reference for what the bulk call must produce.
        for (long p = 0; p < cardinality; ++p) {
            assertEquals("get(" + p + ") with a stale acc", expected.get((int) p).longValue(), dirty.get(p));
        }

        final long[] allPositions = LongStream.range(0, cardinality).toArray();
        assertEquals("all positions", expected, keysForPositions(dirty, allPositions));

        // A subset, and a non-monotonic-friendly ascending subset spanning the insert.
        assertEquals("first three", expected.subList(0, 3), keysForPositions(dirty, 0, 1, 2));
        assertEquals("around the insert", List.of(expected.get(1), expected.get(2)), keysForPositions(dirty, 1, 2));
    }

    @Test
    public void testGetKeysForPositionsOutOfRangeWithStaleAcc() {
        final RspBitmap dirty = makeWithStaleAcc();
        final RspBitmap clean = makeWithStaleAcc();
        clean.finishMutations();
        final long cardinality = clean.getCardinality();

        // Positions at or past the cardinality yield NULL_ROW_KEY, and everything after them does too.
        assertEquals(List.of(-1L), keysForPositions(dirty, cardinality));
        assertEquals(List.of(-1L, -1L), keysForPositions(dirty, cardinality, cardinality + 5));
        assertEquals(List.of(-1L, -1L), keysForPositions(dirty, -1, 0));
        final List<Long> mixed = keysForPositions(dirty, 0, cardinality + 1, 1);
        assertEquals(3, mixed.size());
        assertEquals(clean.get(0), mixed.get(0).longValue());
        assertEquals(-1L, mixed.get(1).longValue());
        assertEquals(-1L, mixed.get(2).longValue());
    }

    @Test
    public void testGetKeysForPositionsWithCleanAccStillWorks() {
        final RspBitmap rb = makeWithStaleAcc();
        rb.finishMutations();
        final long cardinality = rb.getCardinality();
        final List<Long> expected = new ArrayList<>();
        for (long p = 0; p < cardinality; ++p) {
            expected.add(rb.get(p));
        }
        assertEquals(expected, keysForPositions(rb, LongStream.range(0, cardinality).toArray()));
    }
}
