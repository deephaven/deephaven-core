//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.List;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.keysOf;
import static org.junit.Assert.assertEquals;

/**
 * The smart coalescing builder drops a shift range that moves none of the pre-shift keys. When that range is the first
 * of a run of positive shifts, the run's next range is still the first one stored for the run, and must not be checked
 * for ordering against the last range of the negative run before it.
 */
public class RowSetShiftDataSmartCoalescingBuilderSkippedRangeTest {

    @Test
    public void testFirstRangeOfAPositiveRunHoldsNoKey() {
        try (final RowSet preShiftKeys = RowSetFactory.fromKeys(50, 100);
                final RowSetShiftData.SmartCoalescingBuilder builder =
                        new RowSetShiftData.SmartCoalescingBuilder(preShiftKeys.copy())) {
            // Shift iteration order: negative shifts ascending, then positive shifts descending.
            builder.shiftRange(40, 60, -10);
            builder.shiftRange(200, 210, 5); // holds no key: dropped
            builder.shiftRange(90, 110, 5);
            final RowSetShiftData shiftData = builder.build();
            shiftData.validate();
            try (final WritableRowSet shifted = preShiftKeys.copy()) {
                shiftData.apply(shifted);
                assertEquals(List.of(40L, 105L), keysOf(shifted));
            }
        }
    }
}
