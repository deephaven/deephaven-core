//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.extensions.barrage.BarragePerformanceLog.SnapshotMetricsHelper;
import io.deephaven.extensions.barrage.BarrageSubscriptionPerformanceLogger.StatType;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.SafeCloseable;
import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@link BarragePerformanceLog} singleton must keep the blink tables live after the release of the caller's
 * liveness scope at first creation. When a DoGet is the first Barrage activity on a server, the singleton is created
 * inside the export's scope. Before DH-24214, releasing the export's scope destroyed both blink tables: each table
 * getter threw {@link io.deephaven.engine.liveness.LivenessStateException}, and logged rows never reached the tables.
 */
public class BarragePerformanceLogTest extends RefreshingTableTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Each test needs the first creation of the singleton to happen inside the test, so discard any existing
        // instance. The strict release tracker never saw an instance from an earlier test acquire the chunks, and
        // releasing the chunks under tracking would fail, so discard the instance untracked.
        ChunkPoolReleaseTracking.untracked(BarragePerformanceLog::resetForUnitTests);
    }

    @Override
    public void tearDown() throws Exception {
        // The instance created by the test holds tracked chunks, and the base class checks for the release of every
        // tracked chunk.
        BarragePerformanceLog.resetForUnitTests();
        super.tearDown();
    }

    /**
     * Create the singleton inside a liveness scope, then release the scope.
     */
    private static BarragePerformanceLog createInsideReleasedScope() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            return BarragePerformanceLog.getInstance();
        }
    }

    /**
     * Run one update cycle so that rows the publisher has flushed reach the blink table.
     */
    private static void runUpdateCycle(final StreamToBlinkTableAdapter adapter) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(adapter::run);
    }

    @Test
    public void testSnapshotTableOutlivesCreatingScope() {
        final BarragePerformanceLog log = createInsideReleasedScope();
        final Table snapshots = log.getSnapshotTable();

        final SnapshotMetricsHelper helper = new SnapshotMetricsHelper();
        helper.tableId = "cafe1234";
        helper.tableKey = "MyTable";
        log.getSnapshotLogger().log(helper, 12_345L, 4_096L);
        runUpdateCycle(((BarrageSnapshotPerformanceLoggerImpl) log.getSnapshotLogger()).adapter());

        assertThat(snapshots.size()).isEqualTo(1L);
    }

    @Test
    public void testSubscriptionTableOutlivesCreatingScope() {
        final BarragePerformanceLog log = createInsideReleasedScope();
        final Table subscriptions = log.getSubscriptionTable();

        final Histogram hist = new Histogram(3);
        hist.recordValue(3_123L);
        log.getSubscriptionLogger().log("cafe1234", "MyTable", StatType.WRITE_NANOS, Instant.EPOCH, hist);
        runUpdateCycle(((BarrageSubscriptionPerformanceLoggerImpl) log.getSubscriptionLogger()).adapter());

        assertThat(subscriptions.size()).isEqualTo(1L);
    }
}
