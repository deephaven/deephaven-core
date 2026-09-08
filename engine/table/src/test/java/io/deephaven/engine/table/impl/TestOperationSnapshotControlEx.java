//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

/**
 * Tests for the way {@link OperationSnapshotControlEx} distinguishes {@link NotificationAwareDependency notification
 * aware} extra dependencies from notification oblivious ones, and for the state change that {@link DynamicWhereFilter}
 * reports as such a dependency.
 */
public class TestOperationSnapshotControlEx {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    /**
     * An extra dependency that is never satisfied, and whose recorded step the test sets directly.
     */
    private abstract static class TestDependency implements NotificationQueue.Dependency {

        private final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();

        long recordedStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;

        @Override
        public boolean satisfied(final long step) {
            return false;
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return updateGraph;
        }

        @Override
        public LogOutput append(final LogOutput logOutput) {
            return logOutput.append(getClass().getSimpleName());
        }
    }

    /**
     * An extra whose previous values are stable for the cycle, so its notifications must be ignored. This is what
     * sorts, aggregations and tree filters pass.
     */
    private static class TestObliviousStepSource extends TestDependency implements NotificationStepSource {

        @Override
        public long getLastNotificationStep() {
            return recordedStep;
        }
    }

    /**
     * An extra that keeps no previous version of the state it guards. Deliberately not a
     * {@link NotificationStepSource}, matching {@link DynamicWhereFilter}, so that the snapshot control's satisfaction
     * fast path cannot mistake a mid-change state for a completed notification.
     */
    private static class TestAwareDependency extends TestDependency implements NotificationAwareDependency {

        @Override
        public boolean stateChangedOnStep(final long step) {
            return recordedStep == step;
        }
    }

    private static QueryTable refreshingSource() {
        return TstUtils.testRefreshingTable(i(0).toTracking(), intCol("X", 1));
    }

    /**
     * A state change by a notification aware extra invalidates a snapshot that used previous values, because that extra
     * has no previous values to have read.
     */
    @Test
    public void testAwareExtraInvalidatesPreviousValuesSnapshot() {
        final QueryTable source = refreshingSource();
        final TestAwareDependency aware = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, aware);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();

            // Neither the source nor the extra is satisfied, so the snapshot must use previous values.
            assertEquals(Boolean.TRUE, control.usePreviousValues(clockValue));

            // The extra has not changed its state on this step, so reading previous values was consistent.
            assertTrue(control.snapshotConsistent(clockValue, true));

            // Once it changes state on the step being snapshotted, the read is no longer consistent and the attempt
            // must be retried, both while the snapshot is running and when it completes.
            aware.recordedStep = LogicalClock.getStep(clockValue);
            assertFalse(control.snapshotConsistent(clockValue, true));
            assertFalse(control.snapshotCompletedConsistently(clockValue, true));
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * A snapshot that read current values is unaffected by a notification aware extra, because the extra must already
     * have been satisfied for that snapshot to have used current values.
     */
    @Test
    public void testAwareExtraDoesNotAffectCurrentValuesSnapshot() {
        final QueryTable source = refreshingSource();
        final TestAwareDependency aware = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, aware);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            assertEquals(Boolean.TRUE, control.usePreviousValues(clockValue));

            aware.recordedStep = LogicalClock.getStep(clockValue);
            assertTrue(control.snapshotConsistent(clockValue, false));
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * A notification oblivious extra, which is what sorts, aggregations and tree filters pass, must not invalidate a
     * snapshot that used previous values. Its previous values are stable for the whole cycle, so making it aware would
     * only cause needless retries.
     */
    @Test
    public void testObliviousExtraDoesNotInvalidatePreviousValuesSnapshot() {
        final QueryTable source = refreshingSource();
        final TestObliviousStepSource oblivious = new TestObliviousStepSource();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, oblivious);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            assertEquals(Boolean.TRUE, control.usePreviousValues(clockValue));

            // Notifying on the step being snapshotted must change nothing for an oblivious extra.
            oblivious.recordedStep = LogicalClock.getStep(clockValue);
            assertTrue(control.snapshotConsistent(clockValue, true));
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * A {@link DynamicWhereFilter} over a refreshing set table reports that its set kernel changed on a given step,
     * which is what lets {@link OperationSnapshotControlEx} detect the conflict above.
     */
    @Test
    public void testDynamicWhereFilterReportsKernelChangeStep() {
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Z", 1));
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("Z"));

        assertTrue(filter instanceof NotificationAwareDependency);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        assertFalse(filter.stateChangedOnStep(updateGraph.clock().currentStep()));

        // Changing the set table changes the kernel, which must be reported against that step.
        final long[] changedStep = new long[1];
        updateGraph.runWithinUnitTestCycle(() -> {
            changedStep[0] = updateGraph.clock().currentStep();
            TstUtils.addToTable(setTable, i(1), intCol("Z", 2));
            setTable.notifyListeners(i(1), i(), i());
        });
        assertTrue(filter.stateChangedOnStep(changedStep[0]));

        // A cycle that leaves the set table alone must report no change, so that snapshots are not retried for a
        // kernel that did not change.
        final long[] quietStep = new long[1];
        updateGraph.runWithinUnitTestCycle(() -> {
            quietStep[0] = updateGraph.clock().currentStep();
        });
        assertFalse(filter.stateChangedOnStep(quietStep[0]));

        // The earlier change is still reported against its own step.
        assertTrue(filter.stateChangedOnStep(changedStep[0]));
    }

    /**
     * The state change step is published before the kernel changes, so it must never be mistaken for a notification
     * step. {@link OperationSnapshotControlEx} treats a {@link NotificationStepSource} whose notification step is the
     * current step as satisfied without asking it, which for this filter would mean reporting "done" while the kernel
     * is still being rewritten, and letting a snapshot read it with current values.
     */
    @Test
    public void testDynamicWhereFilterIsNotANotificationStepSource() {
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Z", 1));
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("Z"));

        assertFalse("A notification aware dependency must not also be a NotificationStepSource, because the snapshot"
                + " control's satisfaction fast path would then treat a mid-change kernel as complete",
                filter instanceof NotificationStepSource);
    }

    /**
     * A {@link DynamicWhereFilter} nested inside a composed filter must still reach the snapshot control as a
     * notification aware dependency, so that {@code where(Filter.and(...))} is protected in the same way as a bare
     * dynamic filter.
     */
    @Test
    public void testNestedDynamicWhereFilterIsDetectedAsAware() {
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Z", 1));
        final DynamicWhereFilter dynamicFilter =
                new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("Z"));
        final WhereFilter composed = ConjunctiveFilter.of(
                new MatchFilter(MatchOptions.REGULAR, "X", (Object) 1),
                dynamicFilter);

        final List<NotificationQueue.Dependency> dependencies =
                WhereListener.extractDependencies(new WhereFilter[] {composed});

        assertTrue(dependencies.contains(dynamicFilter));
        assertEquals(1, dependencies.stream().filter(NotificationAwareDependency.class::isInstance).count());
    }
}
