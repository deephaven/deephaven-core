//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import io.deephaven.configuration.Configuration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decides when the windows of every {@link SoftRecycler} end, from the update cycles reported to it, so that no
 * recycler reads a clock.
 *
 * <p>
 * An update graph may report the start of each cycle through {@link #reportCycleStart(String, long)}; a graph that does
 * not report has no say in the window. A window is long enough to span a whole cycle of each reporting graph: the
 * largest of the graphs' latest intervals between cycle starts, times {@value #WINDOW_MARGIN_PROPERTY} (1.5 by default)
 * for jitter, but no shorter than {@value SoftRecycler#WINDOW_MILLIS_PROPERTY} milliseconds (1000 by default) and no
 * longer than {@value #MAXIMUM_WINDOW_MILLIS_PROPERTY} milliseconds (60000 by default). Each graph contributes only its
 * latest interval, so a graph that slowed for a cycle stops lengthening the window at its next cycle, and a graph that
 * has not started a cycle within the longest window, because it is idle or gone, is ignored. An interval longer than
 * the longest window, as an event-driven graph's idle time might be, counts as the longest window.
 * </p>
 *
 * <p>
 * One caller, the DEFAULT update graph, calls {@link #endWindowIfElapsed(long)} once per cycle with the time it read
 * when the cycle started; no other code reads a clock for the recyclers. A process whose DEFAULT graph does not report
 * never ends a window, so its recyclers never shrink, and the garbage collector reclaims their items under memory
 * pressure as it always may.
 * </p>
 */
public final class SoftRecyclerWindows {
    /** The property for the most milliseconds a window may last, however slow a reporting graph is. */
    public static final String MAXIMUM_WINDOW_MILLIS_PROPERTY = "SoftRecycler.maximumWindowMillis";
    /** The property for the multiple of the longest interval between cycle starts that a window lasts. */
    public static final String WINDOW_MARGIN_PROPERTY = "SoftRecycler.windowMargin";

    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final long MINIMUM_WINDOW_NANOS =
            NANOS_PER_MILLI * Configuration.getInstance().getLongWithDefault(SoftRecycler.WINDOW_MILLIS_PROPERTY, 1000);
    private static final long MAXIMUM_WINDOW_NANOS = Math.max(MINIMUM_WINDOW_NANOS,
            NANOS_PER_MILLI * Configuration.getInstance().getLongWithDefault(MAXIMUM_WINDOW_MILLIS_PROPERTY, 60_000));
    private static final double WINDOW_MARGIN =
            Configuration.getInstance().getDoubleWithDefault(WINDOW_MARGIN_PROPERTY, 1.5);

    private static final SoftRecyclerWindows INSTANCE =
            new SoftRecyclerWindows(MINIMUM_WINDOW_NANOS, MAXIMUM_WINDOW_NANOS, WINDOW_MARGIN,
                    SoftRecycler::endWindows);

    /** The cycles of one reporting graph, written only by that graph's update thread. */
    private static final class GraphCycles {
        private volatile long lastStartNanos;
        /** The latest interval between cycle starts, at most the longest window; 0 until the second cycle. */
        private volatile long intervalNanos;

        private GraphCycles(final long startNanos) {
            lastStartNanos = startNanos;
        }
    }

    private final long minimumWindowNanos;
    private final long maximumWindowNanos;
    private final double margin;
    private final Runnable endWindows;
    private final Map<String, GraphCycles> graphs = new ConcurrentHashMap<>();

    /** When the current window started; written only by the one caller of {@link #endWindowIfElapsed(long)}. */
    private long windowStartNanos;
    private boolean windowStarted;

    @VisibleForTesting
    SoftRecyclerWindows(final long minimumWindowNanos, final long maximumWindowNanos, final double margin,
            @NotNull final Runnable endWindows) {
        this.minimumWindowNanos = minimumWindowNanos;
        this.maximumWindowNanos = maximumWindowNanos;
        this.margin = margin;
        this.endWindows = endWindows;
    }

    /**
     * Report that an update graph started a cycle. Called by the graph's update thread, with a time the graph has
     * already read.
     *
     * @param graphName the name of the graph
     * @param startNanos when the cycle started, from {@link System#nanoTime()}
     */
    public static void reportCycleStart(@NotNull final String graphName, final long startNanos) {
        INSTANCE.cycleStarted(graphName, startNanos);
    }

    /**
     * Stop counting a graph's cycles toward the window, as when the graph is removed.
     *
     * @param graphName the name of the graph
     */
    public static void forgetGraph(@NotNull final String graphName) {
        INSTANCE.graphForgotten(graphName);
    }

    /**
     * End every recycler's window if it has lasted long enough. Called once per cycle by the DEFAULT update graph.
     *
     * @param nowNanos the time the caller has already read, from {@link System#nanoTime()}
     */
    public static void endWindowIfElapsed(final long nowNanos) {
        INSTANCE.maybeEndWindow(nowNanos);
    }

    @VisibleForTesting
    void cycleStarted(@NotNull final String graphName, final long startNanos) {
        final GraphCycles cycles = graphs.get(graphName);
        if (cycles == null) {
            graphs.put(graphName, new GraphCycles(startNanos));
            return;
        }
        cycles.intervalNanos = Math.max(0, Math.min(maximumWindowNanos, startNanos - cycles.lastStartNanos));
        cycles.lastStartNanos = startNanos;
    }

    @VisibleForTesting
    void graphForgotten(@NotNull final String graphName) {
        graphs.remove(graphName);
    }

    /**
     * @return whether the window ended
     */
    @VisibleForTesting
    synchronized boolean maybeEndWindow(final long nowNanos) {
        if (!windowStarted) {
            windowStarted = true;
            windowStartNanos = nowNanos;
            return false;
        }
        if (nowNanos - windowStartNanos < windowNanos(nowNanos)) {
            return false;
        }
        windowStartNanos = nowNanos;
        endWindows.run();
        return true;
    }

    /**
     * @return how long a window lasts, given the graphs that have started a cycle within the longest window
     */
    @VisibleForTesting
    long windowNanos(final long nowNanos) {
        long longestInterval = 0;
        for (final GraphCycles cycles : graphs.values()) {
            if (nowNanos - cycles.lastStartNanos <= maximumWindowNanos) {
                longestInterval = Math.max(longestInterval, cycles.intervalNanos);
            }
        }
        final long window = (long) Math.ceil(longestInterval * margin);
        return Math.min(maximumWindowNanos, Math.max(minimumWindowNanos, window));
    }
}
