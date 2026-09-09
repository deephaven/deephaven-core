//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Rowsets compare by content, so equal rowsets must hash alike however they are backed. Without that, a rowset used as
 * a key in a hash-based collection is found only by the very instance that was put there.
 */
public class RowSetHashCodeTest {

    /** The same two ranges, on each of the three implementations. */
    private static WritableRowSet[] equivalentRowSets() {
        final RspBitmap rsp = RspBitmap.makeSingleRange(10, 14);
        rsp.addRangeUnsafeNoWriteCheck(100, 104);
        rsp.finishMutations();
        return new WritableRowSet[] {
                new WritableRowSetImpl(SortedRanges.makeSingleRange(10, 14).addRange(100, 104)),
                new WritableRowSetImpl(rsp),
        };
    }

    @Test
    public void testEqualRowSetsHashAlike() {
        final WritableRowSet[] sets = equivalentRowSets();
        try (final WritableRowSet a = sets[0];
                final WritableRowSet b = sets[1]) {
            assertEquals("the fixtures are equal", a, b);
            assertEquals("so they must hash alike", a.hashCode(), b.hashCode());
        }
    }

    @Test
    public void testASingleRangeHashesLikeItsEquivalents() {
        try (final WritableRowSet single = new WritableRowSetImpl(SingleRange.make(10, 14));
                final WritableRowSet sorted = new WritableRowSetImpl(SortedRanges.makeSingleRange(10, 14));
                final WritableRowSet rsp = new WritableRowSetImpl(RspBitmap.makeSingleRange(10, 14))) {
            assertEquals(single, sorted);
            assertEquals(single, rsp);
            assertEquals("single vs sorted", single.hashCode(), sorted.hashCode());
            assertEquals("single vs rsp", single.hashCode(), rsp.hashCode());
        }
    }

    @Test
    public void testEmptyRowSetsHashAlike() {
        try (final WritableRowSet a = new WritableRowSetImpl(OrderedLongSet.EMPTY);
                final WritableRowSet b = new WritableRowSetImpl(OrderedLongSet.EMPTY)) {
            assertEquals(a, b);
            assertEquals("empty rowsets hash alike", a.hashCode(), b.hashCode());
        }
    }

    /** The point of the exercise: a rowset works as a key in a hash-based collection. */
    @Test
    public void testUsableAsAHashKey() {
        final WritableRowSet[] sets = equivalentRowSets();
        try (final WritableRowSet key = sets[0];
                final WritableRowSet lookalike = sets[1]) {
            final Set<RowSet> seen = new HashSet<>();
            seen.add(key);
            assertTrue("an equal rowset is found", seen.contains(lookalike));
            assertTrue("and adding it changes nothing", !seen.add(lookalike));
            assertEquals("one entry", 1, seen.size());

            final Map<RowSet, String> byRowSet = new HashMap<>();
            byRowSet.put(key, "value");
            assertEquals("looked up by an equal rowset", "value", byRowSet.get(lookalike));
        }
    }

    /** Rowsets differing in size, first key, or last key are expected to hash apart. */
    @Test
    public void testDifferingRowSetsHashApart() {
        try (final WritableRowSet a = new WritableRowSetImpl(SortedRanges.makeSingleRange(10, 14));
                final WritableRowSet differentSize = new WritableRowSetImpl(SortedRanges.makeSingleRange(10, 15));
                final WritableRowSet differentFirst = new WritableRowSetImpl(SortedRanges.makeSingleRange(11, 15));
                final WritableRowSet differentLast = new WritableRowSetImpl(
                        SortedRanges.makeSingleRange(10, 13).addRange(20, 20))) {
            assertTrue("size differs", a.hashCode() != differentSize.hashCode());
            assertTrue("first key differs", a.hashCode() != differentFirst.hashCode());
            assertTrue("last key differs", a.hashCode() != differentLast.hashCode());
        }
    }
}
