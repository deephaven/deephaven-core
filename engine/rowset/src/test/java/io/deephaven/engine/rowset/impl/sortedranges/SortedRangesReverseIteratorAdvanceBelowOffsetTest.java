//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.assertBackedBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A packed SortedRanges stores keys relative to an offset. Its reverse iterator, asked to advance to a key below that
 * offset, has passed every key it holds: {@code advance} returns false and {@code hasNext} must agree, or the next
 * {@code nextLong} dereferences the released array.
 */
public class SortedRangesReverseIteratorAdvanceBelowOffsetTest {

    private static WritableRowSet packed() {
        final WritableRowSet rs = new WritableRowSetImpl(OrderedLongSet.twoRanges(100, 100, 200_000, 200_000));
        assertBackedBy("two far apart keys", rs, "Int");
        return rs;
    }

    @Test
    public void testAdvanceBelowTheOffsetExhausts() {
        try (final WritableRowSet rs = packed(); final RowSet.SearchIterator it = rs.reverseIterator()) {
            assertFalse(it.advance(50));
            assertFalse("hasNext after an advance that returned false", it.hasNext());
        }
    }

    @Test
    public void testAdvanceBelowTheOffsetAfterAKeyWasReadExhausts() {
        try (final WritableRowSet rs = packed(); final RowSet.SearchIterator it = rs.reverseIterator()) {
            assertEquals(200_000, it.nextLong());
            assertFalse(it.advance(50));
            assertFalse("hasNext after an advance that returned false", it.hasNext());
        }
    }

    @Test
    public void testAdvanceBetweenTheKeysLandsOnTheLowerOne() {
        try (final WritableRowSet rs = packed(); final RowSet.SearchIterator it = rs.reverseIterator()) {
            assertTrue(it.advance(150));
            assertEquals(100, it.currentValue());
            assertFalse(it.hasNext());
        }
    }
}
