/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.LiveTableTestCase;
import io.deephaven.internal.log.LoggerFactory;

public class RedirectionIndexTest extends LiveTableTestCase {
    private final Logger log = LoggerFactory.getLogger(RedirectionIndexTest.class);

    public void testBasic() {
        final RedirectionIndex redirectionIndex =
            RedirectionIndex.FACTORY.createRedirectionIndex(8);
        for (int i = 0; i < 3; i++) {
            redirectionIndex.put(i, i * 2);
        }
        final RedirectionIndex redirectionIndex1 =
            RedirectionIndex.FACTORY.createRedirectionIndex(8);
        for (int i = 0; i < 3; i++) {
            redirectionIndex1.put(i * 2, i * 4);
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(redirectionIndex.get(i), i * 2);
            assertEquals(redirectionIndex1.get(i * 2), i * 4);
        }
        redirectionIndex.startTrackingPrevValues();
        redirectionIndex1.startTrackingPrevValues();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            for (int i = 0; i < 3; i++) {
                redirectionIndex1.put(i * 2, i * 3);
            }
            for (int i = 0; i < 3; i++) {
                assertEquals(i * 2, redirectionIndex.get(i));
                assertEquals(i * 2, redirectionIndex.getPrev(i));

                assertEquals(i * 3, redirectionIndex1.get(i * 2));
                assertEquals(redirectionIndex1.getPrev(i * 2), i * 4);
            }
        });

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            for (int i = 0; i < 3; i++) {
                redirectionIndex.put((i + 1) % 3, i * 2);
            }
        });
    }

    public void testContiguous() {
        final RedirectionIndex redirectionIndex = new ContiguousRedirectionIndexImpl(10);

        // Fill redirection index with values 100 + ii * 2
        for (int ii = 0; ii < 100; ++ii) {
            redirectionIndex.put(ii, 100 + ii * 2);
        }

        // Check that 100 + ii * 2 comes back from get()
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(100 + ii * 2, redirectionIndex.get(ii));
        }

        assertEquals(-1, redirectionIndex.get(100));
        assertEquals(-1, redirectionIndex.get(-1));

        // As of startTrackingPrevValues, get() and getPrev() should both be returning 100 + ii * 2
        redirectionIndex.startTrackingPrevValues();

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            for (int ii = 0; ii < 100; ++ii) {
                assertEquals(100 + ii * 2, redirectionIndex.get(ii));
            }
            for (int ii = 0; ii < 100; ++ii) {
                assertEquals(100 + ii * 2, redirectionIndex.getPrev(ii));
            }

            // Now set current values to 200 + ii * 3
            for (int ii = 0; ii < 100; ++ii) {
                redirectionIndex.put(ii, 200 + ii * 3);
            }

            // Confirm that get() returns 200 + ii * 3; meanwhile getPrev() still returns 100 + ii *
            // 2
            for (int ii = 0; ii < 100; ++ii) {
                assertEquals(200 + ii * 3, redirectionIndex.get(ii));
            }
            for (int ii = 0; ii < 100; ++ii) {
                assertEquals(100 + ii * 2, redirectionIndex.getPrev(ii));
            }
        });

        // After end of cycle, both should return 200 + ii * 3
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(200 + ii * 3, redirectionIndex.get(ii));
        }
        for (int ii = 0; ii < 100; ++ii) {
            assertEquals(200 + ii * 3, redirectionIndex.getPrev(ii));
        }
    }
}
