/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.impl.RefreshingTableTestCase;
import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;

public class RowRedirectionTest extends RefreshingTableTestCase {
    private final Logger log = LoggerFactory.getLogger(RowRedirectionTest.class);

    public void testBasic() {
        final WritableRowRedirection rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(8);
        for (int i = 0; i < 3; i++) {
            rowRedirection.put(i, i * 2);
        }
        final WritableRowRedirection rowRedirection1 = WritableRowRedirection.FACTORY.createRowRedirection(8);
        for (int i = 0; i < 3; i++) {
            rowRedirection1.put(i * 2, i * 4);
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(rowRedirection.get(i), i * 2);
            assertEquals(rowRedirection1.get(i * 2), i * 4);
        }
        rowRedirection.startTrackingPrevValues();
        rowRedirection1.startTrackingPrevValues();
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            for (int i = 0; i < 3; i++) {
                rowRedirection1.put(i * 2, i * 3);
            }
            for (int i = 0; i < 3; i++) {
                assertEquals(i * 2, rowRedirection.get(i));
                assertEquals(i * 2, rowRedirection.getPrev(i));

                assertEquals(i * 3, rowRedirection1.get(i * 2));
                assertEquals(rowRedirection1.getPrev(i * 2), i * 4);
            }
        });

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            for (int i = 0; i < 3; i++) {
                rowRedirection.put((i + 1) % 3, i * 2);
            }
        });
    }

    public void testContiguous() {
        final WritableRowRedirection rowRedirection = new ContiguousWritableRowRedirection(10);

        // Fill row redirection with values 100 + ii * 2
        for (int ii = 0; ii < 100; ++ii) {
            rowRedirection.put(ii, 100 + ii * 2);
        }

        // Check that 100 + ii * 2 comes back from get()
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(100 + ii * 2, rowRedirection.get(ii));
        }

        assertEquals(-1, rowRedirection.get(100));
        assertEquals(-1, rowRedirection.get(-1));

        // As of startTrackingPrevValues, get() and getPrev() should both be returning 100 + ii * 2
        rowRedirection.startTrackingPrevValues();

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            for (int ii = 0; ii < 100; ++ii) {
                assertEquals(100 + ii * 2, rowRedirection.get(ii));
            }
            for (int ii = 0; ii < 100; ++ii) {
                assertEquals(100 + ii * 2, rowRedirection.getPrev(ii));
            }

            // Now set current values to 200 + ii * 3
            for (int ii = 0; ii < 100; ++ii) {
                rowRedirection.put(ii, 200 + ii * 3);
            }

            // Confirm that get() returns 200 + ii * 3; meanwhile getPrev() still returns 100 + ii * 2
            for (int ii = 0; ii < 100; ++ii) {
                assertEquals(200 + ii * 3, rowRedirection.get(ii));
            }
            for (int ii = 0; ii < 100; ++ii) {
                assertEquals(100 + ii * 2, rowRedirection.getPrev(ii));
            }
        });

        // After end of cycle, both should return 200 + ii * 3
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(200 + ii * 3, rowRedirection.get(ii));
        }
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(200 + ii * 3, rowRedirection.getPrev(ii));
        }
    }
}
