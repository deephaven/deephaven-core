//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * {@link RowSet#invert} requires every key asked for to be present. The empty rowset holds no key, so inverting any
 * non-empty key set against it is rejected with the same {@link IllegalArgumentException} the other representations
 * throw for a missing key, rather than answering with an empty position set. Inverting an empty key set is a no-op for
 * every representation.
 */
public class EmptyRowSetInvertTest {

    @Test
    public void testEmptyRowSetRejectsMissingKey() {
        try (final RowSet keys = RowSetFactory.fromKeys(10); final WritableRowSet empty = RowSetFactory.empty()) {
            assertThrows(IllegalArgumentException.class, () -> empty.invert(keys, Long.MAX_VALUE).close());
        }
    }

    @Test
    public void testOtherRepresentationsRejectMissingKeyTheSameWay() {
        try (final RowSet keys = RowSetFactory.fromKeys(10);
                final WritableRowSet single = singleRangeOf(20, 30);
                final WritableRowSet sorted = sortedRangesOf(new long[] {20, 30}, new long[] {40, 41});
                final WritableRowSet bitmap = rspOf(new long[] {20, 30}, new long[] {40, 41})) {
            assertThrows(IllegalArgumentException.class, () -> single.invert(keys, Long.MAX_VALUE).close());
            assertThrows(IllegalArgumentException.class, () -> sorted.invert(keys, Long.MAX_VALUE).close());
            assertThrows(IllegalArgumentException.class, () -> bitmap.invert(keys, Long.MAX_VALUE).close());
        }
    }

    @Test
    public void testEmptyKeysInvertToEmptyPositions() {
        try (final RowSet keys = RowSetFactory.empty();
                final WritableRowSet empty = RowSetFactory.empty();
                final WritableRowSet positions = empty.invert(keys, Long.MAX_VALUE)) {
            assertTrue(positions.isEmpty());
        }
    }
}
