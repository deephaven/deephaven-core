//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SoftRecyclerWindowTest {
    private static final long SECOND = 1_000_000_000L;
    private static final long MINIMUM = SECOND;
    private static final long MAXIMUM = 60 * SECOND;

    private final AtomicInteger windowsEnded = new AtomicInteger();
    private final SoftRecyclerWindow window =
            new SoftRecyclerWindow(MINIMUM, MAXIMUM, 1.5, windowsEnded::incrementAndGet);

    /** Report cycles of {@code graphName} every {@code interval}, from {@code start}, through {@code end}. */
    private void cycles(final String graphName, final long start, final long interval, final long end) {
        for (long time = start; time <= end; time += interval) {
            window.cycleStarted(graphName, time);
        }
    }

    @Test
    public void testWindowIsTheMinimumWithoutReportingGraphs() {
        assertEquals(MINIMUM, window.windowNanos(0));
        // a fast graph does not shorten it
        cycles("fast", 0, SECOND / 10, SECOND);
        assertEquals(MINIMUM, window.windowNanos(SECOND));
    }

    @Test
    public void testWindowSpansTheSlowestGraphWithAMargin() {
        cycles("fast", 0, SECOND / 10, 20 * SECOND);
        cycles("slow", 0, 4 * SECOND, 20 * SECOND);
        assertEquals(6 * SECOND, window.windowNanos(20 * SECOND));
    }

    @Test
    public void testWindowIsCappedAtTheMaximum() {
        cycles("slow", 0, 50 * SECOND, 100 * SECOND);
        assertEquals(MAXIMUM, window.windowNanos(100 * SECOND));
    }

    @Test
    public void testAGraphCountsOnlyItsLatestInterval() {
        // one slow cycle lengthens the window until the graph's next cycle start
        window.cycleStarted("graph", 0);
        window.cycleStarted("graph", 10 * SECOND);
        assertEquals(15 * SECOND, window.windowNanos(10 * SECOND));
        window.cycleStarted("graph", 11 * SECOND);
        assertEquals(1500 * SECOND / 1000, window.windowNanos(11 * SECOND));
    }

    @Test
    public void testAStartOutOfOrderCountsAsNoInterval() {
        window.cycleStarted("graph", 0);
        window.cycleStarted("graph", 10 * SECOND);
        window.cycleStarted("graph", 9 * SECOND);
        assertEquals(MINIMUM, window.windowNanos(10 * SECOND));
    }

    @Test
    public void testAnIdleGraphIsIgnored() {
        cycles("slow", 0, 20 * SECOND, 40 * SECOND);
        assertEquals(30 * SECOND, window.windowNanos(40 * SECOND));
        // no cycle started within the longest window
        assertEquals(MINIMUM, window.windowNanos(40 * SECOND + MAXIMUM + 1));
    }

    @Test
    public void testAForgottenGraphIsIgnored() {
        cycles("slow", 0, 10 * SECOND, 10 * SECOND);
        assertEquals(15 * SECOND, window.windowNanos(10 * SECOND));
        window.graphForgotten("slow");
        assertEquals(MINIMUM, window.windowNanos(10 * SECOND));
    }

    @Test
    public void testEndsTheWindowOnlyOnceItHasElapsed() {
        cycles("slow", 0, 4 * SECOND, 8 * SECOND);
        // the first call starts the first window
        assertFalse(window.maybeEndWindow(8 * SECOND));
        assertFalse(window.maybeEndWindow(13 * SECOND));
        assertEquals(0, windowsEnded.get());
        assertTrue(window.maybeEndWindow(14 * SECOND));
        assertEquals(1, windowsEnded.get());
        // the next window starts where that one ended
        assertFalse(window.maybeEndWindow(19 * SECOND));
        assertTrue(window.maybeEndWindow(20 * SECOND));
        assertEquals(2, windowsEnded.get());
    }
}
