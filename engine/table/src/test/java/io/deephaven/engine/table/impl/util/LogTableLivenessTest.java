//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * A singleton with in-memory log tables must keep the tables live after the release of the caller's liveness scope at
 * first creation. Before DH-24214, releasing the caller's scope destroyed the blink tables, and every later call to a
 * table getter threw {@link io.deephaven.engine.liveness.LivenessStateException}.
 */
public class LogTableLivenessTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Before
    public void setUp() {
        // Each test needs the first creation of a singleton to happen inside the test, so discard any existing
        // instance. The fixture creates the AsyncErrorLogger instance before chunk tracking starts, and an earlier test
        // may have created the other singletons, so the strict release tracker never saw an existing instance acquire
        // the chunks. Releasing the chunks under tracking would fail, so discard the instances untracked.
        ChunkPoolReleaseTracking.untracked(LogTableLivenessTest::resetSingletons);
    }

    @After
    public void tearDown() {
        // The instances created by the test hold tracked chunks, and the fixture checks for the release of every
        // tracked chunk.
        resetSingletons();
    }

    private static void resetSingletons() {
        EngineMetrics.resetForUnitTests();
        ServerStateTracker.resetForUnitTests();
        AsyncErrorLogger.resetForUnitTests();
    }

    private static void assertRefreshing(final Table table) {
        assertTrue(table.isRefreshing());
    }

    @Test
    public void testEngineMetricsTablesOutliveCreatingScope() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            EngineMetrics.getInstance();
        }
        assertRefreshing(TableLoggers.queryPerformanceLog());
        assertRefreshing(TableLoggers.queryOperationPerformanceLog());
        assertRefreshing(TableLoggers.processMetricsLog());
    }

    @Test
    public void testServerStateLogOutlivesCreatingScope() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            ServerStateTracker.getInstance();
        }
        assertRefreshing(TableLoggers.serverStateLog());
    }

    @Test
    public void testAsyncErrorLogOutlivesCreatingScope() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            AsyncErrorLogger.init();
        }
        assertRefreshing(AsyncErrorLogger.getErrorLog());
    }
}
