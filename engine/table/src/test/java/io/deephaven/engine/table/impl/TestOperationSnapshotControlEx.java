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
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

        /** Whether this dependency reports itself satisfied; never, unless a test says otherwise. */
        boolean satisfied = false;

        @Override
        public boolean satisfied(final long step) {
            return satisfied;
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

        /** How many operations this dependency currently delivers state changes to. */
        int subscriptions;

        @Override
        public long lastStateChangeStep() {
            return recordedStep;
        }

        @Override
        public boolean subscribe(final long requiredLastStateChangeStep) {
            if (recordedStep != requiredLastStateChangeStep) {
                return false;
            }
            ++subscriptions;
            return true;
        }

        @Override
        public void unsubscribe() {
            --subscriptions;
        }
    }

    private static QueryTable refreshingSource() {
        return TstUtils.testRefreshingTable(i(0).toTracking(), intCol("X", 1));
    }

    /**
     * Once the source and every extra are satisfied on the step, the control reads current values and does not wait.
     * The source is satisfied by having notified on this step, the oblivious extra by its notification step, and the
     * aware extra by saying so, which are the three ways a dependency satisfies the control.
     */
    @Test
    public void testAllSatisfiedUsesCurrentValuesWithoutWaiting() throws Exception {
        final QueryTable source = refreshingSource();
        final TestAwareDependency aware = new TestAwareDependency();
        final TestObliviousStepSource oblivious = new TestObliviousStepSource();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, aware, oblivious);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutorService pool = Executors.newSingleThreadExecutor();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            final long step = LogicalClock.getStep(clockValue);
            source.setLastNotificationStep(step);
            oblivious.recordedStep = step;
            aware.satisfied = true;
            aware.recordedStep = step;

            // Decided off-thread with a timeout, so that a wait shows up as a failure rather than a hang.
            final Future<Boolean> decision = pool.submit(() -> control.usePreviousValues(clockValue));
            assertEquals(Boolean.FALSE, decision.get(5, TimeUnit.SECONDS));

            // The aware extra changed on this step, which is irrelevant to a current values read.
            assertTrue(control.snapshotConsistent(clockValue, false));
        } finally {
            pool.shutdownNow();
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * A successful commit subscribes every aware extra, each requiring the state it guards to be as the attempt found
     * it, so that from then on the extra's changes reach the result the attempt is handing out.
     */
    @Test
    public void testCommitSubscribesEveryAwareExtra() {
        final QueryTable source = refreshingSource();
        final TestAwareDependency first = new TestAwareDependency();
        final TestAwareDependency second = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, first, second);
        control.setListenerAndResult(null, refreshingSource());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            assertEquals(Boolean.TRUE, control.usePreviousValues(clockValue));

            assertTrue(control.snapshotCompletedConsistently(clockValue, true));
            assertEquals(1, first.subscriptions);
            assertEquals(1, second.subscriptions);
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * An aware extra that has changed since the attempt read it refuses at the commit, which rejects the attempt and
     * undoes the extras subscribed before it. Nothing is left following anything, so the change cannot reach a result
     * that will never be handed out, and the retry reads the changed state directly.
     * <p>
     * The change here is on an earlier step than the one being snapshotted, which is the case the completion check
     * cannot see: it asks only whether an extra changed on this step.
     */
    @Test
    public void testCommitUndoesEveryAwareExtraWhenOneRefuses() {
        final QueryTable source = refreshingSource();
        final TestAwareDependency first = new TestAwareDependency();
        final TestAwareDependency second = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, first, second);
        control.setListenerAndResult(null, refreshingSource());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            assertEquals(Boolean.TRUE, control.usePreviousValues(clockValue));

            second.recordedStep = LogicalClock.getStep(clockValue) - 1;
            assertTrue("a change on an earlier step is not what the completion check looks for",
                    control.snapshotConsistent(clockValue, true));

            assertFalse(control.snapshotCompletedConsistently(clockValue, true));
            assertEquals("the extra subscribed before the refusal was undone", 0, first.subscriptions);
            assertEquals(0, second.subscriptions);
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
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
     * A completion that the aware check rejects is about to be retried, so it must leave no trace: in particular the
     * eventual listener must not be subscribed to the source, or the retry would subscribe it a second time. Once the
     * completion is accepted, the listener is subscribed as usual.
     */
    @Test
    public void testRejectedCompletionDoesNotSubscribeListener() {
        final QueryTable source = refreshingSource();
        final QueryTable result = refreshingSource();
        final TestAwareDependency aware = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, aware);
        final ListenerRecorder listener = new ListenerRecorder("test", source, result);
        control.setListenerAndResult(listener, result);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            assertEquals(Boolean.TRUE, control.usePreviousValues(clockValue));

            aware.recordedStep = LogicalClock.getStep(clockValue);
            assertFalse(control.snapshotCompletedConsistently(clockValue, true));
            assertFalse(source.hasListeners());

            aware.recordedStep = NotificationStepReceiver.NULL_NOTIFICATION_STEP;
            assertTrue(control.snapshotCompletedConsistently(clockValue, true));
            assertTrue(source.hasListeners());
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
     * A dependency that has already been satisfied on a later step refuses to answer for an earlier one. The control
     * cannot judge such an attempt at all, and reports that rather than guessing.
     */
    @Test
    public void testStaleClockValueCannotBeJudged() {
        final QueryTable source = refreshingSource();
        final TestAwareDependency neverSatisfied = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, neverSatisfied);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        final long staleClockValue = updateGraph.clock().currentValue();
        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.completeCycleForUnitTests();

        updateGraph.startCycleForUnitTests(false);
        try {
            updateGraph.markSourcesRefreshedForUnitTests();
            // Asking the source about the current step records it, so the stale step is now older than what it knows.
            assertTrue(source.satisfied(updateGraph.clock().currentStep()));
            assertNull(control.usePreviousValues(staleClockValue));
        } finally {
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
        assertNotEquals(updateGraph.clock().currentStep(), filter.lastStateChangeStep());

        // Changing the set table changes the kernel, which must be reported against that step.
        final long[] changedStep = new long[1];
        updateGraph.runWithinUnitTestCycle(() -> {
            changedStep[0] = updateGraph.clock().currentStep();
            TstUtils.addToTable(setTable, i(1), intCol("Z", 2));
            setTable.notifyListeners(i(1), i(), i());
        });
        assertEquals(changedStep[0], filter.lastStateChangeStep());

        // A cycle that leaves the set table alone must report no change, so that snapshots are not retried for a
        // kernel that did not change.
        final long[] quietStep = new long[1];
        updateGraph.runWithinUnitTestCycle(() -> {
            quietStep[0] = updateGraph.clock().currentStep();
        });
        assertNotEquals(quietStep[0], filter.lastStateChangeStep());

        // The earlier change is still the last one reported.
        assertEquals(changedStep[0], filter.lastStateChangeStep());
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

    private static QueryTable staticSource() {
        return TstUtils.testTable(i(0).toTracking(), intCol("X", 1));
    }

    /**
     * Asserts that, within a cycle, {@code control} waits for the {@code pending} extras rather than deciding at once,
     * and decides on current values once every extra is satisfied. {@code completed} is satisfied on the step before
     * the decision is requested, so it is not waited for.
     */
    private static void assertWaitsForExtras(
            final OperationSnapshotControlEx control,
            final TestAwareDependency completed,
            final TestAwareDependency... pending) throws Exception {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutorService pool = Executors.newSingleThreadExecutor();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            final long step = LogicalClock.getStep(clockValue);
            completed.satisfied = true;
            completed.recordedStep = step;

            final Future<Boolean> decision = pool.submit(() -> control.usePreviousValues(clockValue));
            try {
                final Boolean early = decision.get(500, TimeUnit.MILLISECONDS);
                fail("expected the control to wait for the pending extras, but it decided " + early);
            } catch (TimeoutException expected) {
                // Waiting for the pending extras, as it should.
            }

            // Satisfy the pending extras and let the wait notification fire; current values are then consistent.
            for (final TestAwareDependency extra : pending) {
                extra.satisfied = true;
            }
            final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!decision.isDone()) {
                if (System.nanoTime() > deadlineNanos) {
                    fail("the control did not resume once every extra was satisfied");
                }
                if (!updateGraph.flushOneNotificationForUnitTests()) {
                    // noinspection BusyWait
                    Thread.sleep(1);
                }
            }
            assertEquals(Boolean.FALSE, decision.get());
            assertTrue(control.snapshotConsistent(clockValue, false));
        } finally {
            pool.shutdownNow();
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * A static source cannot be unsatisfied, so it does not count. With no extra satisfied, nothing has changed yet on
     * the step, and the control uses previous values at once rather than waiting, exactly as it does for a refreshing
     * source in the same state. An extra that changes during the read is caught by the aware check as usual.
     */
    @Test
    public void testStaticSourceUsesPreviousValuesWhenNoExtraIsSatisfied() {
        final QueryTable source = staticSource();
        final TestAwareDependency first = new TestAwareDependency();
        final TestAwareDependency second = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, first, second);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            assertEquals(Boolean.TRUE, control.usePreviousValues(clockValue));
            assertTrue(control.snapshotConsistent(clockValue, true));

            first.recordedStep = LogicalClock.getStep(clockValue);
            assertFalse(control.snapshotConsistent(clockValue, true));
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * An extra that has already completed on the step is no reason to skip the wait for one that has not. The control
     * waits for the unsatisfied extra and then reads current values, which the completed extra is consistent with.
     */
    @Test
    public void testStaticSourceWaitsWhenSomeExtrasAreSatisfied() throws Exception {
        final QueryTable source = staticSource();
        final TestAwareDependency completed = new TestAwareDependency();
        final TestAwareDependency pending = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, completed, pending);
        assertWaitsForExtras(control, completed, pending);
    }

    /**
     * The source is counted like any other dependency. When every extra is satisfied but the source has not been, the
     * control is partially satisfied and waits for the source, then reads current values.
     */
    @Test
    public void testRefreshingSourceIsWaitedForWhenExtrasAreSatisfied() throws Exception {
        final QueryTable source = refreshingSource();
        final TestAwareDependency completed = new TestAwareDependency();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, completed);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutorService pool = Executors.newSingleThreadExecutor();
        updateGraph.startCycleForUnitTests(false);
        boolean sourcesRefreshed = false;
        try {
            final long clockValue = updateGraph.clock().currentValue();
            final long step = LogicalClock.getStep(clockValue);
            completed.satisfied = true;
            completed.recordedStep = step;
            assertFalse(source.satisfied(step));

            final Future<Boolean> decision = pool.submit(() -> control.usePreviousValues(clockValue));
            try {
                final Boolean early = decision.get(500, TimeUnit.MILLISECONDS);
                fail("expected the control to wait for the source, but it decided " + early);
            } catch (TimeoutException expected) {
                // Waiting for the source, as it should.
            }

            // A root table is satisfied once the graph's sources have refreshed; let the wait notification fire.
            updateGraph.markSourcesRefreshedForUnitTests();
            sourcesRefreshed = true;
            assertTrue(source.satisfied(step));
            final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!decision.isDone()) {
                if (System.nanoTime() > deadlineNanos) {
                    fail("the control did not resume once the source was satisfied");
                }
                if (!updateGraph.flushOneNotificationForUnitTests()) {
                    // noinspection BusyWait
                    Thread.sleep(1);
                }
            }
            assertEquals(Boolean.FALSE, decision.get());
            assertTrue(control.snapshotConsistent(clockValue, false));
        } finally {
            pool.shutdownNow();
            if (!sourcesRefreshed) {
                updateGraph.markSourcesRefreshedForUnitTests();
            }
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * On an update thread nothing can be waited for, and a locked snapshot never uses previous values. A set filter
     * whose set has not been processed on this step therefore cannot be read at all: the operation must be ordered
     * after the set listener by a dependency, and the control says so instead of guessing.
     */
    @Test
    public void testStaticSourceWithUnsatisfiedSetFilterOnUpdateThread() {
        final ExecutionContext context = ExecutionContext.getContext();
        final ControlledUpdateGraph updateGraph = context.getUpdateGraph().cast();
        final QueryTable source = TstUtils.testTable(i(2, 4, 6).toTracking(), intCol("Z", 1, 2, 3));
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Z", 1));
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("Z"));

        final MutableObject<Throwable> failure = new MutableObject<>();
        updateGraph.startCycleForUnitTests(false);
        try {
            assertFalse(filter.satisfied(updateGraph.clock().currentStep()));
            updateGraph.refreshUpdateSourceForUnitTests(() -> {
                assertTrue(updateGraph.currentThreadProcessesUpdates());
                try (final SafeCloseable ignored = context.open()) {
                    source.where(filter);
                    fail("where on an update thread with an unsatisfied set filter must not succeed");
                } catch (IllegalStateException e) {
                    failure.setValue(e);
                }
            });
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
        assertNotNull(failure.getValue());
        assertTrue(failure.getValue().getMessage(), failure.getValue().getMessage().contains("DynamicWhereFilter"));
    }

    /**
     * The same where on an update thread, once the set has been processed for the step. A caller that depends on the
     * set listener sees exactly this: every input is satisfied, current values are read, and no wait is needed.
     */
    @Test
    public void testStaticSourceWithSatisfiedSetFilterOnUpdateThread() {
        final ExecutionContext context = ExecutionContext.getContext();
        final ControlledUpdateGraph updateGraph = context.getUpdateGraph().cast();
        final QueryTable source = TstUtils.testTable(i(2, 4, 6).toTracking(), intCol("Z", 1, 2, 3));
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Z", 1));
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("Z"));

        final MutableObject<Table> result = new MutableObject<>();
        updateGraph.startCycleForUnitTests(false);
        try {
            updateGraph.markSourcesRefreshedForUnitTests();
            assertTrue(filter.satisfied(updateGraph.clock().currentStep()));
            updateGraph.refreshUpdateSourceForUnitTests(() -> {
                assertTrue(updateGraph.currentThreadProcessesUpdates());
                try (final SafeCloseable ignored = context.open()) {
                    result.setValue(source.where(filter));
                }
            });
        } finally {
            updateGraph.completeCycleForUnitTests();
        }
        assertNotNull(result.getValue());
        assertTableEquals(TableTools.newTable(intCol("Z", 1)), result.getValue());
    }

    /**
     * The update thread rule does not depend on the source being static or the extra being notification aware: any
     * unsatisfied dependency is a missing ordering, and previous values are never an answer there.
     */
    @Test
    public void testRefreshingSourceWithUnsatisfiedObliviousExtraOnUpdateThread() {
        final QueryTable source = refreshingSource();
        final TestObliviousStepSource pending = new TestObliviousStepSource();
        final OperationSnapshotControlEx control = new OperationSnapshotControlEx(source, pending);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final MutableObject<Throwable> failure = new MutableObject<>();
        updateGraph.startCycleForUnitTests(false);
        try {
            final long clockValue = updateGraph.clock().currentValue();
            updateGraph.refreshUpdateSourceForUnitTests(() -> {
                try {
                    control.usePreviousValues(clockValue);
                    fail("an unsatisfied extra on an update thread must not be snapshotted");
                } catch (IllegalStateException e) {
                    failure.setValue(e);
                }
            });
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
        assertNotNull(failure.getValue());
    }
}
