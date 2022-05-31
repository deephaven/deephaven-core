package io.deephaven.util;

import io.deephaven.base.clock.Clock;
import io.deephaven.util.clock.RealTimeClock;

/**
 * This is a hook to enable testing and simulation. Users of Clock should use this factory to get an instance. When they
 * do, it is possible to change the class behavior from a test harness.
 *
 * Note: This would be better housed as a member of Clock.
 */
public class ClockFactory {
    /** The normal way of getting the system time. */
    static Clock DEFAULT_CLOCK = new RealTimeClock();

    /** The current active Clock. */
    private static Clock timesource = DEFAULT_CLOCK;

    /**
     * Get the current Clock.
     *
     * @return the current Clock
     */
    public static Clock get() {
        return timesource;
    }

    /**
     * Get the default (System) Clock.
     *
     * @return the default Clock
     */
    public static Clock getDefault() {
        return DEFAULT_CLOCK;
    }

    /**
     * Set the Clock that will be returned by {@link #get()}.
     */
    public static void set(Clock newTimesource) {
        timesource = newTimesource;
    }

    /**
     * Reset the Clock that will be returned by {@link #get()} to the default value.
     */
    public static void reset() {
        timesource = getDefault();
    }
}
