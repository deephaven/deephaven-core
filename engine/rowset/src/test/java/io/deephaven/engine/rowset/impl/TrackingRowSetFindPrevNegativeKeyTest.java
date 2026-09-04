//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;

/**
 * {@link io.deephaven.engine.rowset.RowSet#find(long)} follows the {@code Arrays.binarySearch} convention: a key below
 * every key in the set is not found, and would be inserted at position 0, so the answer is {@code -1}. A negative key
 * is below every key. {@code findPrev} answers the same question against the previous rowset, and agrees with
 * {@code find} on the same content.
 */
public class TrackingRowSetFindPrevNegativeKeyTest {

    @Rule
    public final EngineCleanup engineCleanup = new EngineCleanup();

    private static Supplier<?>[] rowSets() {
        return new Supplier<?>[] {
                () -> singleRangeOf(100, 110),
                () -> sortedRangesOf(new long[] {100, 110}, new long[] {200, 210}),
                () -> rspOf(new long[] {100, 110}, new long[] {200, 210}),
        };
    }

    @Test
    public void testFindPrevOfNegativeKeyAgreesWithFind() {
        for (final Supplier<?> supplier : rowSets()) {
            final WritableRowSet rs = (WritableRowSet) supplier.get();
            final String name = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
            final TrackingWritableRowSet tracking = rs.toTracking();
            try {
                for (final long key : new long[] {-1, -2, -100, Long.MIN_VALUE}) {
                    final long found = tracking.find(key);
                    assertEquals(name + " find(" + key + ")", -1, found);
                    assertEquals(name + " findPrev(" + key + ") must agree with find", found, tracking.findPrev(key));
                }
            } finally {
                tracking.close();
            }
        }
    }
}
