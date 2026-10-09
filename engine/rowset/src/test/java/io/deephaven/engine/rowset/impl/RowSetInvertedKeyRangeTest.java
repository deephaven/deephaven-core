//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * A key range whose end precedes its start holds no keys. {@code [start, start + n - 1]} with {@code n == 0} is the
 * natural way to arrive at one, so queries and removals must treat it as empty rather than walk it as if it ran
 * forward. Inserting one is rejected.
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
    public void testBuildersRejectInvertedRanges() {
        assertThrows(IllegalArgumentException.class, () -> RowSetFactory.builderRandom().addRange(5, 3));
        assertThrows(IllegalArgumentException.class, () -> RowSetFactory.builderSequential().appendRange(5, 3));
        // Adjacent to the pending range, so the order check alone would accept it.
        assertThrows(IllegalArgumentException.class, () -> {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            builder.appendKey(4);
            builder.appendRange(5, 4);
        });
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
                        assertThrows(what + " insertRange", IllegalArgumentException.class,
                                () -> inserted.insertRange(range[0], range[1]));
                        inserted.validate();
                        assertEquals(what + " insertRange", keysOf(rs), keysOf(inserted));
                    }
                }
            }
        }
    }

    /**
     * {@code insertRange(0, size - 1)} for an empty table bounds an empty range with a negative key, which is rejected
     * even though the range holds no keys.
     */
    @Test
    public void testNegativeBoundOfAnEmptyRangeIsRejected() {
        try (final WritableRowSet rs = RowSetFactory.empty()) {
            assertThrows(IllegalArgumentException.class, () -> rs.insertRange(0, -1));
            rs.validate();
            assertTrue(rs.isEmpty());
        }
        assertThrows(IllegalArgumentException.class, () -> RowSetFactory.fromRange(0, -1));
    }
}
