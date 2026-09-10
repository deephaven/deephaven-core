//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.keysOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * {@link RowSet#invert} requires every key to be present: a position past the end of the set is as meaningless as one
 * before its start, and every backing must reject both the same way.
 */
public class RowSetInvertMissingKeyTest {

    private static Supplier<?>[] rowSets() {
        return new Supplier<?>[] {
                () -> singleRangeOf(0, 10),
                () -> sortedRangesOf(new long[] {0, 10}),
                () -> rspOf(new long[] {0, 10}),
        };
    }

    private static void assertRejected(final String what, final WritableRowSet rs, final RowSet keys) {
        try (final WritableRowSet positions = rs.invert(keys, Long.MAX_VALUE)) {
            fail(what + " accepted a key that is not in the set: " + positions);
        } catch (IllegalArgumentException expected) {
            // The rowset-level checks.
        } catch (IllegalStateException expected) {
            // An RSP walking a multi-range argument finds the missing key inside a container, which reports it this
            // way.
        }
    }

    @Test
    public void testKeysBeyondTheEndAreRejected() {
        for (final Supplier<?> supplier : rowSets()) {
            try (final WritableRowSet rs = (WritableRowSet) supplier.get();
                    final RowSet beyond = RowSetFactory.fromKeys(20);
                    final RowSet straddling = RowSetFactory.fromRange(5, 20);
                    final RowSet twoKeys = RowSetFactory.fromKeys(5, 20);
                    final RowSet present = RowSetFactory.fromKeys(5)) {
                final String name = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
                assertRejected(name + " lone key beyond the end", rs, beyond);
                assertRejected(name + " range past the end", rs, straddling);
                assertRejected(name + " second of two keys beyond the end", rs, twoKeys);
                try (final WritableRowSet positions = rs.invert(present, Long.MAX_VALUE)) {
                    assertEquals(name + " present key", List.of(5L), keysOf(positions));
                }
            }
        }
    }
}
