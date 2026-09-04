//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.assertBackedBy;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.keysOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A key range whose end precedes its start holds no keys. {@code [start, start + n - 1]} with {@code n == 0} is the
 * natural way to arrive at one, so every operation must treat it as empty rather than walk it as if it ran forward.
 */
public class RowSetInvertedKeyRangeTest {

    private static WritableRowSet packedSortedRangesOf(final long[]... ranges) {
        final WritableRowSet rs = sortedRangesOf(ranges);
        rs.compact();
        assertBackedBy("compacted sorted ranges", rs, "Short");
        return rs;
    }

    private static Supplier<?>[] rowSets() {
        return new Supplier<?>[] {
                () -> singleRangeOf(20, 30),
                () -> sortedRangesOf(new long[] {10, 10}, new long[] {20, 30}, new long[] {40, 40}),
                () -> packedSortedRangesOf(new long[] {10, 10}, new long[] {20, 30}, new long[] {40, 40}),
                () -> rspOf(new long[] {10, 10}, new long[] {20, 30}, new long[] {40, 40}),
        };
    }

    private static String nameOf(final WritableRowSet rs) {
        return ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
    }

    /** Inverted ranges inside a range of the set, in a gap, and past the end. */
    private static final long[][] INVERTED = {{25, 24}, {25, 22}, {35, 33}, {41, 39}};

    @Test
    public void testQueries() {
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get()) {
                final String name = nameOf(rs);
                for (final long[] range : INVERTED) {
                    final String what = name + " [" + range[0] + "," + range[1] + "]";
                    assertFalse(what + " overlapsRange", rs.overlapsRange(range[0], range[1]));
                    try (final WritableRowSet sub = rs.subSetByKeyRange(range[0], range[1])) {
                        assertTrue(what + " subSetByKeyRange", sub.isEmpty());
                    }
                    try (final RowSequence seq = rs.getRowSequenceByKeyRange(range[0], range[1])) {
                        assertTrue(what + " getRowSequenceByKeyRange", seq.isEmpty());
                    }
                }
            }
        }
    }

    @Test
    public void testMutations() {
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get()) {
                final String name = nameOf(rs);
                for (final long[] range : INVERTED) {
                    final String what = name + " [" + range[0] + "," + range[1] + "]";
                    try (final WritableRowSet removed = rs.copy()) {
                        removed.removeRange(range[0], range[1]);
                        removed.validate();
                        assertEquals(what + " removeRange", keysOf(rs), keysOf(removed));
                    }
                    try (final WritableRowSet retained = rs.copy()) {
                        retained.retainRange(range[0], range[1]);
                        retained.validate();
                        assertTrue(what + " retainRange", retained.isEmpty());
                        assertEquals(what + " retainRange size", 0, retained.size());
                    }
                    try (final WritableRowSet inserted = rs.copy()) {
                        inserted.insertRange(range[0], range[1]);
                        inserted.validate();
                        assertEquals(what + " insertRange", keysOf(rs), keysOf(inserted));
                    }
                }
            }
        }
    }

    /** {@code insertRange(0, size - 1)} on an empty rowset for an empty table is a common way to arrive at one. */
    @Test
    public void testInsertingAnEmptyRangeIntoAnEmptyRowSet() {
        try (final WritableRowSet rs = RowSetFactory.empty()) {
            rs.insertRange(0, -1);
            rs.validate();
            assertTrue(rs.isEmpty());
            assertEquals(0, rs.size());
        }
        try (final WritableRowSet rs = RowSetFactory.fromRange(0, -1)) {
            rs.validate();
            assertTrue(rs.isEmpty());
            assertEquals(0, rs.size());
        }
    }
}
