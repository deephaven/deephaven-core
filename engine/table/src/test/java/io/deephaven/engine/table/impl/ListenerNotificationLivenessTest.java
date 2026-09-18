//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the liveness guarantees made by {@link InstrumentedTableListenerBase} and {@link MergedListener}
 * notifications: a notification must do no work on behalf of a listener that is no longer live, and update processing
 * must not be interleaved with a concurrent destroy.
 */
public class ListenerNotificationLivenessTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private ControlledUpdateGraph updateGraph;
    private QueryTable source;
    private long nextRowKey;

    @Before
    public void setUp() {
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        source = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Sentinel", 0));
        nextRowKey = 1;
    }

    /**
     * Add a row to {@link #source} and notify its listeners. Must be called from within an update cycle.
     */
    private void addRowAndNotify() {
        final RowSet added = i(nextRowKey++);
        TstUtils.addToTable(source, added, intCol("Sentinel", 1));
        source.notifyListeners(added, i(), i());
    }

    @Test
    public void testUpdateNotificationIsSkippedWhenListenerIsNotLive() {
        final AtomicBoolean updated = new AtomicBoolean();
        final LivenessScope scope = new LivenessScope();
        final InstrumentedTableUpdateListenerAdapter listener;
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            listener = new InstrumentedTableUpdateListenerAdapter(source, false) {
                @Override
                public void onUpdate(@NotNull final TableUpdate upstream) {
                    updated.set(true);
                }
            };
            source.addUpdateListener(listener);
        }

        updateGraph.startCycleForUnitTests();
        final long step = updateGraph.clock().currentStep();
        try {
            addRowAndNotify();
            // Drop the listener's last reference after its notification has been enqueued, but before the update
            // graph runs it.
            scope.release();
        } finally {
            updateGraph.completeCycleForUnitTests();
        }

        assertFalse("onUpdate ran for a listener that was no longer live", updated.get());
        // A skipped notification must still record its completed step. satisfied(step) never becomes true for a
        // notification that was enqueued for a step and did not record its completion, so a dependent of this
        // listener would wait forever and the cycle would fail to drain its notification queue.
        assertTrue("skipped notification did not record its completed step", listener.satisfied(step));
    }

    @Test
    public void testMergedNotificationIsSkippedWhenListenerIsNotLive() {
        final AtomicBoolean processed = new AtomicBoolean();
        final LivenessScope scope = new LivenessScope();
        final MergedListener listener;
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            final ListenerRecorder recorder = new ListenerRecorder("test recorder", source, null);
            source.addUpdateListener(recorder);
            listener = new TestMergedListener(recorder, null, () -> processed.set(true));
            recorder.setMergedListener(listener);
        }

        updateGraph.startCycleForUnitTests();
        final long step = updateGraph.clock().currentStep();
        try {
            addRowAndNotify();
            scope.release();
        } finally {
            updateGraph.completeCycleForUnitTests();
        }

        assertFalse("process ran for a merged listener that was no longer live", processed.get());
        assertTrue("skipped notification did not record its completed step", listener.satisfied(step));
    }

    /**
     * An exception escaping {@code process()} must be routed through {@code handleUncaughtException}, failing the
     * result table, rather than escaping the notification into the update graph.
     */
    @Test
    public void testMergedListenerProcessFailureFailsResult() {
        final ListenerRecorder recorder = new ListenerRecorder("test recorder", source, null);
        source.addUpdateListener(recorder);
        final QueryTable result = TstUtils.testRefreshingTable(i().toTracking(), intCol("Sentinel"));
        final MergedListener listener = new TestMergedListener(recorder, result, () -> {
            throw new IllegalStateException("process failure");
        });
        recorder.setMergedListener(listener);
        result.addParentReference(listener);

        framework.allowingError(
                () -> updateGraph.runWithinUnitTestCycle(this::addRowAndNotify),
                errors -> errors.size() == 1);

        assertTrue("result table was not failed by the exception from process()", result.isFailed());
    }

    private static final class TestMergedListener extends MergedListener {

        private final Runnable onProcess;

        private TestMergedListener(
                @NotNull final ListenerRecorder recorder,
                @Nullable final QueryTable result,
                @NotNull final Runnable onProcess) {
            super(List.of(recorder), List.of(), "TestMergedListener", result);
            this.onProcess = onProcess;
        }

        @Override
        protected void process() {
            onProcess.run();
        }
    }
}
