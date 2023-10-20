package io.deephaven.engine.updategraph.impl;

import io.deephaven.base.SleepUtil;
import io.deephaven.configuration.DataDir;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.sources.LongSingleValueSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import junit.framework.TestCase;
import org.junit.*;

import java.nio.file.Path;
import java.util.Collections;

public class TestEventDrivenUpdateGraph {
    @Before
    public void before() {
        // the default update is necessary for the update performance tracker
        final UpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)
                .numUpdateThreads(PeriodicUpdateGraph.NUM_THREADS_DEFAULT_UPDATE_GRAPH)
                .existingOrBuild();
        final PeriodicUpdateGraph pug = updateGraph.cast();
        pug.enableUnitTestMode();
        pug.resetForUnitTests(false);
    }

    @After
    public void after() {
        PeriodicUpdateGraph.getInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME).resetForUnitTests(true);
    }


    final static class SourceThatRefreshes extends QueryTable implements Runnable {
        public SourceThatRefreshes(UpdateGraph updateGraph) {
            super(RowSetFactory.empty().toTracking(), Collections.emptyMap());
            setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
            updateGraph.addSource(this);
        }

        @Override
        public void run() {
            final RowSet added;
            if (getRowSet().isEmpty()) {
                added = RowSetFactory.fromKeys(0);
            } else {
                added = RowSetFactory.fromKeys(getRowSet().lastRowKey() + 1);
            }
            getRowSet().writableCast().insert(added);
            notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }
    }

    final static class SourceThatModifiesItself extends QueryTable implements Runnable {
        final LongSingleValueSource svcs;

        public SourceThatModifiesItself(UpdateGraph updateGraph) {
            super(RowSetFactory.fromKeys(42).toTracking(), Collections.singletonMap("V", new LongSingleValueSource()));
            svcs = (LongSingleValueSource)getColumnSource("V", long.class);
            svcs.startTrackingPrevValues();
            updateGraph.addSource(this);
            svcs.set(0L);
        }

        @Override
        public void run() {
            svcs.set(svcs.getLong(0) + 1);
            notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(), getRowSet().copy());
        }
    }

    private QueryCompiler compilerForUnitTests() {
        final Path queryCompilerDir = DataDir.get()
                .resolve("io.deephaven.engine.updategraph.impl.TestEventDrivenUpdateGraph.compilerForUnitTests");

        return QueryCompiler.create(queryCompilerDir.toFile(), getClass().getClassLoader());
    }

    @Test
    public void testSimpleAdd() {

        final EventDrivenUpdateGraph eventDrivenUpdateGraph = new EventDrivenUpdateGraph("TestEDUG",
                BaseUpdateGraph.DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS);

        final ExecutionContext context = ExecutionContext.newBuilder().setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope().newQueryLibrary().setQueryCompiler(compilerForUnitTests()).build();
        try (final SafeCloseable ignored = context.open()) {
            final SourceThatRefreshes sourceThatRefreshes = new SourceThatRefreshes(eventDrivenUpdateGraph);
            final Table updated =
                    eventDrivenUpdateGraph.sharedLock().computeLocked(() -> sourceThatRefreshes.update("X=i"));

            int steps = 0;
            do {
                TestCase.assertEquals(steps, updated.size());
                eventDrivenUpdateGraph.requestRefresh();
            } while (steps++ < 100);
            TestCase.assertEquals(steps, updated.size());
        }
    }

    @Test
    public void testSimpleModify() {

        final EventDrivenUpdateGraph eventDrivenUpdateGraph = new EventDrivenUpdateGraph("TestEDUG",
                BaseUpdateGraph.DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS);

        final ExecutionContext context = ExecutionContext.newBuilder().setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope().newQueryLibrary().setQueryCompiler(compilerForUnitTests()).build();
        try (final SafeCloseable ignored = context.open()) {
            final SourceThatModifiesItself modifySource = new SourceThatModifiesItself(eventDrivenUpdateGraph);
            final Table updated =
                    eventDrivenUpdateGraph.sharedLock().computeLocked(() -> modifySource.update("X=2 * V"));

            final ColumnSource<Long> xcs = updated.getColumnSource("X");

            int steps = 0;
            do {
                TestCase.assertEquals(1, updated.size());
                eventDrivenUpdateGraph.requestRefresh();

                TableTools.showWithRowSet(modifySource);

                final TrackingRowSet rowSet = updated.getRowSet();
                System.out.println("Step = " + steps);
                final long xv = xcs.getLong (rowSet.firstRowKey());
                TestCase.assertEquals(2L * (steps + 1), xv);

                Thread.sleep(1000);
            } while (steps++ < 100);
            TestCase.assertEquals(1, updated.size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testUpdatePerformanceTracker() {
        final Table upt = UpdatePerformanceTracker.getQueryTable();
        TableTools.showWithRowSet(upt);

        final long start = System.currentTimeMillis();

        final EventDrivenUpdateGraph eventDrivenUpdateGraph1 = new EventDrivenUpdateGraph("TestEDUG1",
                BaseUpdateGraph.DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS);
        final EventDrivenUpdateGraph eventDrivenUpdateGraph2 = new EventDrivenUpdateGraph("TestEDUG2",
                BaseUpdateGraph.DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS);

        doWork(eventDrivenUpdateGraph1, 100, 10);
        doWork(eventDrivenUpdateGraph2, 200, 5);

        do {
            final long now = System.currentTimeMillis();
            final long end = start + UpdatePerformanceTracker.REPORT_INTERVAL_MILLIS;
            if (end < now) {
                break;
            }
            System.out.println("Did work, waiting for performance cycle to complete: " + (end - now) + " ms");
            SleepUtil.sleep(end - now);
        } while (true);

        doWork(eventDrivenUpdateGraph1, 100, 1);
        doWork(eventDrivenUpdateGraph2, 2, 100);

        PeriodicUpdateGraph instance = PeriodicUpdateGraph.getInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME).cast();
        instance.requestRefresh();

        SleepUtil.sleep(1000);

        TableTools.showWithRowSet(upt);
    }

    @ReflexiveUse(referrers = "TestEventDrivenUpdateGraph")
    static public <T> T sleepValue(long duration, T retVal) {
        final Object blech = new Object();
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (blech) {
            try {
                final long milliSeconds = duration / 1_000_000L;
                final int nanos = (int) (duration % 1_000_000L);
                blech.wait(milliSeconds, nanos);
            } catch (InterruptedException ignored) {
            }
        }
        return retVal;
    }

    private void doWork(final EventDrivenUpdateGraph eventDrivenUpdateGraph, final int durationMillis, final int steps) {
        final ExecutionContext context = ExecutionContext.newBuilder().setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope().newQueryLibrary().setQueryCompiler(compilerForUnitTests()).build();
        try (final SafeCloseable ignored = context.open()) {
            final SourceThatModifiesItself modifySource = new SourceThatModifiesItself(eventDrivenUpdateGraph);
            final Table updated =
                    eventDrivenUpdateGraph.sharedLock().computeLocked(() -> modifySource.update("X=" + getClass().getName() + ".sleepValue(" + (1000L * 1000L * durationMillis) + ", 2 * V)"));

            int step = 0;
            do {
                TestCase.assertEquals(1, updated.size());
                eventDrivenUpdateGraph.requestRefresh();
            } while (step++ < steps);
            TestCase.assertEquals(1, updated.size());
        }
    }
}
