//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SoftRecyclerWindowsTest {
    private static final long SECOND = 1_000_000_000L;
    private static final long MINIMUM = SECOND;
    private static final long MAXIMUM = 60 * SECOND;

    private final AtomicInteger windowsEnded = new AtomicInteger();
    private final SoftRecyclerWindows windows =
            new SoftRecyclerWindows(MINIMUM, MAXIMUM, 1.5, windowsEnded::incrementAndGet);

    /** Report cycles of {@code graphName} every {@code interval}, from {@code start}, through {@code end}. */
    private void cycles(final String graphName, final long start, final long interval, final long end) {
        for (long time = start; time <= end; time += interval) {
            windows.cycleStarted(graphName, time);
        }
    }

    @Test
    public void testWindowIsTheMinimumWithoutReportingGraphs() {
        assertEquals(MINIMUM, windows.windowNanos(0));
        // a fast graph does not shorten it
        cycles("fast", 0, SECOND / 10, SECOND);
        assertEquals(MINIMUM, windows.windowNanos(SECOND));
    }

    @Test
    public void testWindowSpansTheSlowestGraphWithAMargin() {
        cycles("fast", 0, SECOND / 10, 20 * SECOND);
        cycles("slow", 0, 4 * SECOND, 20 * SECOND);
        assertEquals(6 * SECOND, windows.windowNanos(20 * SECOND));
    }

    @Test
    public void testWindowIsCappedAtTheMaximum() {
        cycles("slow", 0, 50 * SECOND, 100 * SECOND);
        assertEquals(MAXIMUM, windows.windowNanos(100 * SECOND));
    }

    @Test
    public void testAGraphCountsOnlyItsLatestInterval() {
        // one slow cycle lengthens the window until the graph's next cycle start
        windows.cycleStarted("graph", 0);
        windows.cycleStarted("graph", 10 * SECOND);
        assertEquals(15 * SECOND, windows.windowNanos(10 * SECOND));
        windows.cycleStarted("graph", 11 * SECOND);
        assertEquals(1500 * SECOND / 1000, windows.windowNanos(11 * SECOND));
    }

    @Test
    public void testAnIdleGraphIsIgnored() {
        cycles("slow", 0, 20 * SECOND, 40 * SECOND);
        assertEquals(30 * SECOND, windows.windowNanos(40 * SECOND));
        // no cycle started within the longest window
        assertEquals(MINIMUM, windows.windowNanos(40 * SECOND + MAXIMUM + 1));
    }

    @Test
    public void testAForgottenGraphIsIgnored() {
        cycles("slow", 0, 10 * SECOND, 10 * SECOND);
        assertEquals(15 * SECOND, windows.windowNanos(10 * SECOND));
        windows.graphForgotten("slow");
        assertEquals(MINIMUM, windows.windowNanos(10 * SECOND));
    }

    @Test
    public void testEndsTheWindowOnlyOnceItHasElapsed() {
        cycles("slow", 0, 4 * SECOND, 8 * SECOND);
        // the first call starts the first window
        assertFalse(windows.maybeEndWindow(8 * SECOND));
        assertFalse(windows.maybeEndWindow(13 * SECOND));
        assertEquals(0, windowsEnded.get());
        assertTrue(windows.maybeEndWindow(14 * SECOND));
        assertEquals(1, windowsEnded.get());
        // the next window starts where that one ended
        assertFalse(windows.maybeEndWindow(19 * SECOND));
        assertTrue(windows.maybeEndWindow(20 * SECOND));
        assertEquals(2, windowsEnded.get());
    }
}
