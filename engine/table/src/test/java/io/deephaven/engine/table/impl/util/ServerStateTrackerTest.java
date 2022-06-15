/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.*;

public class ServerStateTrackerTest {
    @Test
    public void testStats0() {
        final long[] samples = new long[] {};
        ServerStateTracker.Stats stats = new ServerStateTracker.Stats();
        ServerStateTracker.calcStats(stats, samples, samples.length);
        assertEquals(NULL_LONG, stats.max);
        assertEquals(NULL_LONG, stats.median);
        assertEquals(NULL_LONG, stats.mean);
        assertEquals(NULL_LONG, stats.p90);
    }

    @Test
    public void testStats1() {
        final long[] samples = new long[] {42};
        ServerStateTracker.Stats stats = new ServerStateTracker.Stats();
        ServerStateTracker.calcStats(stats, samples, samples.length);
        assertEquals(42L, stats.max);
        assertEquals(42L, stats.median);
        assertEquals(42L, stats.mean);
        assertEquals(42L, stats.p90);
    }

    @Test
    public void testStats6() {
        final long[] samples = new long[] {10, 20, 30, 40, 50, 80};
        ServerStateTracker.Stats stats = new ServerStateTracker.Stats();
        ServerStateTracker.calcStats(stats, samples, samples.length);
        assertEquals(80L, stats.max);
        assertEquals(35L, stats.median);
        assertEquals(Math.round(230.0 / 6.0), stats.mean);
        assertEquals(80L, stats.p90);
    }

    @Test
    public void testStats10() {
        final long[] samples = new long[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 200};
        ServerStateTracker.Stats stats = new ServerStateTracker.Stats();
        ServerStateTracker.calcStats(stats, samples, samples.length);
        assertEquals(200L, stats.max);
        assertEquals(55L, stats.median);
        assertEquals(65L, stats.mean);
        assertEquals(90L, stats.p90);
    }

    @Test
    public void testStats15() {
        final long[] samples = new long[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 300};
        ServerStateTracker.Stats stats = new ServerStateTracker.Stats();
        ServerStateTracker.calcStats(stats, samples, samples.length);
        assertEquals(300L, stats.max);
        assertEquals(80L, stats.median);
        assertEquals(90L, stats.mean);
        assertEquals(140L, stats.p90);
    }
}
