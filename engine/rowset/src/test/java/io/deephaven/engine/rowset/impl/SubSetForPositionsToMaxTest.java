//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.keysOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;

/**
 * A contiguous position range running to {@code Long.MAX_VALUE} is the natural way to ask for "everything from here
 * on". Its exclusive end does not fit in a long, and a wrapped end must not turn the request into an empty one.
 */
public class SubSetForPositionsToMaxTest {

    private static Supplier<?>[] rowSets() {
        return new Supplier<?>[] {
                () -> singleRangeOf(1, 20),
                () -> sortedRangesOf(new long[] {1, 1}, new long[] {5, 5}, new long[] {10, 10}, new long[] {20, 20}),
                () -> rspOf(new long[] {1, 1}, new long[] {5, 5}, new long[] {10, 10}, new long[] {20, 20}),
        };
    }

    @Test
    public void testTailPositionRangeToMax() {
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get();
                    final WritableRowSet expected = rs.subSetByPositionRange(1, rs.size());
                    final WritableRowSet tail = rs.subSetForPositions(RowSequenceFactory.forRange(1, Long.MAX_VALUE));
                    final WritableRowSet all = rs.subSetForPositions(RowSequenceFactory.forRange(0, Long.MAX_VALUE))) {
                final String name = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
                assertEquals(name + " from position 1", keysOf(expected), keysOf(tail));
                assertEquals(name + " from position 0", keysOf(rs), keysOf(all));
            }
        }
    }

    @Test
    public void testNonContiguousPositionsEndingAtMax() {
        // Position 0, then everything from position 2 on: the general path, not the contiguous shortcut.
        final RowSetBuilderSequential positionsBuilder = RowSetFactory.builderSequential();
        positionsBuilder.appendKey(0);
        positionsBuilder.appendRange(2, Long.MAX_VALUE);
        try (final WritableRowSet positions = positionsBuilder.build()) {
            for (final Supplier<?> supplier : rowSets()) {
                try (final WritableRowSet rs = (WritableRowSet) supplier.get();
                        final WritableRowSet expectedTail = rs.subSetByPositionRange(2, rs.size());
                        final WritableRowSet selected = rs.subSetForPositions(positions)) {
                    final String name = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
                    expectedTail.insert(rs.firstRowKey());
                    assertEquals(name, keysOf(expectedTail), keysOf(selected));
                }
            }
        }
    }
}
