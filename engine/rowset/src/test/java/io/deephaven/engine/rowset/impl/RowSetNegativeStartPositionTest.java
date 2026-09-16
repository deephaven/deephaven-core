//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.keysOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Positions below zero hold no keys. A position range that starts below zero asks for whatever part of it lies at or
 * above zero, on every backing and on every row sequence view; it must never produce a key the set does not hold, a
 * size that disagrees with the keys, or an exception.
 */
public class RowSetNegativeStartPositionTest {

    private static Supplier<?>[] rowSets() {
        return new Supplier<?>[] {
                () -> singleRangeOf(10, 12),
                () -> sortedRangesOf(new long[] {10, 10}, new long[] {20, 20}, new long[] {30, 30}),
                () -> rspOf(new long[] {10, 10}, new long[] {20, 20}, new long[] {70000, 70000}),
        };
    }

    private static void assertKeys(final String what, final List<Long> expected, final RowSequence seq) {
        assertEquals(what, expected, keysOf(seq));
        assertEquals(what + " size", expected.size(), seq.size());
    }

    @Test
    public void testRowSetQueries() {
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get()) {
                final String name = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
                final List<Long> first = List.of(rs.firstRowKey());
                try (final RowSequence seq = rs.getRowSequenceByPosition(-1, 2)) {
                    assertKeys(name + " getRowSequenceByPosition(-1, 2)", first, seq);
                }
                try (final RowSequence seq = rs.getRowSequenceByPosition(-5, 2)) {
                    assertTrue(name + " getRowSequenceByPosition(-5, 2)", seq.isEmpty());
                }
                try (final WritableRowSet sub = rs.subSetByPositionRange(-1, 100)) {
                    assertEquals(name + " subSetByPositionRange(-1, 100)", keysOf(rs), keysOf(sub));
                }
                try (final WritableRowSet sub = rs.subSetByPositionRange(-3, 1)) {
                    assertEquals(name + " subSetByPositionRange(-3, 1)", first, keysOf(sub));
                }
                try (final WritableRowSet sub = rs.subSetByPositionRange(-3, 0)) {
                    assertTrue(name + " subSetByPositionRange(-3, 0)", sub.isEmpty());
                }
                // An inverted range is empty however far the end lies below the start.
                for (final long end : new long[] {Long.MIN_VALUE, -5, 0}) {
                    try (final WritableRowSet sub = rs.subSetByPositionRange(-1, end)) {
                        assertTrue(name + " subSetByPositionRange(-1, " + end + ")", sub.isEmpty());
                    }
                    try (final WritableRowSet sub = rs.subSetByPositionRange(1, end)) {
                        assertTrue(name + " subSetByPositionRange(1, " + end + ")", sub.isEmpty());
                    }
                }
                try (final WritableRowSet sub = rs.subSetByPositionRange(1, 1)) {
                    assertTrue(name + " subSetByPositionRange(1, 1)", sub.isEmpty());
                }
                assertExtremeLengthsAreEmpty(name, rs);
            }
        }
    }

    /**
     * A non-positive length asks for nothing, however far below zero the start is; the two must not be summed first.
     */
    private static void assertExtremeLengthsAreEmpty(final String name, final RowSequence seq) {
        try (final RowSequence sub = seq.getRowSequenceByPosition(-1, Long.MIN_VALUE)) {
            assertTrue(name + " (-1, MIN_VALUE)", sub.isEmpty());
        }
        try (final RowSequence sub = seq.getRowSequenceByPosition(-1, 0)) {
            assertTrue(name + " (-1, 0)", sub.isEmpty());
        }
        try (final RowSequence sub = seq.getRowSequenceByPosition(-1, 1)) {
            assertTrue(name + " (-1, 1)", sub.isEmpty());
        }
        try (final RowSequence sub = seq.getRowSequenceByPosition(Long.MIN_VALUE, 5)) {
            assertTrue(name + " (MIN_VALUE, 5)", sub.isEmpty());
        }
        try (final RowSequence sub = seq.getRowSequenceByPosition(Long.MIN_VALUE, Long.MAX_VALUE)) {
            assertTrue(name + " (MIN_VALUE, MAX_VALUE)", sub.isEmpty());
        }
    }

    @Test
    public void testChunkBackedRowSequences() {
        final long[] keys = {10, 20, 70000};
        final long[] ranges = {10, 10, 20, 20, 70000, 70000};
        final RowSequence[] sequences = {
                RowSequenceFactory.wrapRowKeysChunkAsRowSequence(LongChunk.chunkWrap(keys)),
                RowSequenceFactory.wrapKeyRangesChunkAsRowSequence(LongChunk.chunkWrap(ranges)),
        };
        for (final RowSequence seq : sequences) {
            final String name = seq.getClass().getSimpleName();
            try (final RowSequence sub = seq.getRowSequenceByPosition(-1, 2)) {
                assertKeys(name + " (-1, 2)", List.of(10L), sub);
            }
            try (final RowSequence sub = seq.getRowSequenceByPosition(-5, 2)) {
                assertTrue(name + " (-5, 2)", sub.isEmpty());
            }
            assertExtremeLengthsAreEmpty(name, seq);
        }
    }

    @Test
    public void testRowSequenceViews() {
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get()) {
                final String name = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
                final List<Long> first = List.of(rs.firstRowKey());
                try (final RowSequence view = rs.getRowSequenceByKeyRange(rs.firstRowKey(), rs.lastRowKey());
                        final RowSequence seq = view.getRowSequenceByPosition(-1, 2)) {
                    assertKeys(name + " key range view", first, seq);
                }
                try (final RowSequence view = rs.getRowSequenceByPosition(0, rs.size());
                        final RowSequence seq = view.getRowSequenceByPosition(-1, 2)) {
                    assertKeys(name + " position view", first, seq);
                }
                try (final RowSequence view = rs.getRowSequenceByPosition(0, rs.size());
                        final RowSequence seq = view.getRowSequenceByPosition(-5, 2)) {
                    assertTrue(name + " position view, all below zero", seq.isEmpty());
                }
                try (final RowSequence view = rs.getRowSequenceByPosition(0, rs.size())) {
                    assertExtremeLengthsAreEmpty(name + " position view", view);
                }
            }
        }
    }
}
