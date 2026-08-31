//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.assertBackedBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A position at or beyond the cardinality has no key. Every implementation must say so with
 * {@link RowSequence#NULL_ROW_KEY}: walking on instead runs past the ranges the set holds and reads whatever the
 * backing array's unused tail happens to contain, which is a stale key from a range that was removed.
 */
public class GetKeysForPositionsOutOfRangeTest {

    private static List<Long> keysAt(final WritableRowSet rs, final long... positions) {
        final List<Long> out = new ArrayList<>();
        final PrimitiveIterator.OfLong it = java.util.Arrays.stream(positions).iterator();
        rs.getKeysForPositions(it, out::add);
        return out;
    }

    @Test
    public void testPositionsBeyondTheCardinality() {
        final long none = RowSequence.NULL_ROW_KEY;
        try (final WritableRowSet single = new WritableRowSetImpl(SingleRange.make(10, 20));
                final WritableRowSet sorted =
                        new WritableRowSetImpl(SortedRanges.makeSingleRange(10, 20).addRange(30, 40));
                final WritableRowSet rsp = new WritableRowSetImpl(RspBitmap.makeSingleRange(10, 20))) {
            rsp.insertRange(30, 40);
            assertBackedBy("single range", single, "SingleRange");
            assertBackedBy("sorted ranges", sorted, "SortedRanges");
            assertBackedBy("rsp", rsp, "Rsp");

            // Cardinality 11 for the single range, 22 for the other two.
            assertEquals("single range", List.of(10L, 20L, none, none),
                    keysAt(single, 0, single.size() - 1, single.size(), single.size() + 5));
            assertEquals("sorted ranges", List.of(10L, 40L, none, none),
                    keysAt(sorted, 0, sorted.size() - 1, sorted.size(), sorted.size() + 5));
            assertEquals("rsp", List.of(10L, 40L, none, none),
                    keysAt(rsp, 0, rsp.size() - 1, rsp.size(), rsp.size() + 5));
        }
    }

    /**
     * The array's unused tail still holds the entries of a range that was removed, so a walk that runs off the end
     * reports one of those stale keys rather than reporting no key at all.
     */
    @Test
    public void testPositionBeyondTheCardinalityWithAStaleTail() {
        SortedRanges sr = SortedRanges.makeSingleRange(10, 20);
        sr = sr.addRange(30, 40);
        sr = sr.addRange(50, 60);
        sr = sr.removeRange(50, 60);
        try (final WritableRowSet rs = new WritableRowSetImpl(sr)) {
            assertEquals("cardinality", 22, rs.size());
            assertEquals("no key at the cardinality", List.of(10L, RowSequence.NULL_ROW_KEY),
                    keysAt(rs, 0, rs.size()));
        }
    }

    /** Positions inside the set must keep working, and a request for none must stay empty. */
    @Test
    public void testPositionsWithinTheCardinality() {
        SortedRanges sr = SortedRanges.makeSingleRange(10, 12);
        sr = sr.addRange(30, 32);
        try (final WritableRowSet rs = new WritableRowSetImpl(sr)) {
            assertEquals(List.of(10L, 11L, 12L, 30L, 31L, 32L), keysAt(rs, 0, 1, 2, 3, 4, 5));
            assertEquals(List.of(), keysAt(rs));
        }
    }
}
