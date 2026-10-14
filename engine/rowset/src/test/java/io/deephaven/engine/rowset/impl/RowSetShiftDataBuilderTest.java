//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowSetShiftDataBuilderTest {

    @Test
    public void testSmartCoalescingBuilderCloseIsIdempotent() {
        // build() closes internally; a subsequent close() (e.g. try-with-resources) must not throw.
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(0, 99)) {
            final RowSetShiftData.SmartCoalescingBuilder builder =
                    new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
            builder.shiftRange(0, 10, 100);
            final RowSetShiftData shiftData = builder.build();
            builder.close();
            builder.close();
            assertEquals(1, shiftData.size());
        }
        // close() without build(), twice.
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(0, 99)) {
            final RowSetShiftData.SmartCoalescingBuilder builder =
                    new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
            builder.close();
            builder.close();
        }
    }
}
