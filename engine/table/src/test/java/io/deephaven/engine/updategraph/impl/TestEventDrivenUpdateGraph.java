//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.configuration.DataDir;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.sources.LongSingleValueSource;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import junit.framework.TestCase;
import org.junit.*;

import java.nio.file.Path;
import java.util.Collections;

import static io.deephaven.engine.context.TestExecutionContext.OPERATION_INITIALIZATION;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.assertEquals;

public class TestEventDrivenUpdateGraph {
    EventDrivenUpdateGraph defaultUpdateGraph;

    @Before
    public void before() {
        // the default update is necessary for the update performance tracker
        clearUpdateGraphInstances();
        UpdatePerformanceTracker.resetForUnitTests();
        defaultUpdateGraph = EventDrivenUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME).build();
    }

    @After
    public void after() {
        clearUpdateGraphInstances();
        UpdatePerformanceTracker.resetForUnitTests();
    }

    private static void clearUpdateGraphInstances() {
        BaseUpdateGraph.removeInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME);
        BaseUpdateGraph.removeInstance("TestEDUG");
        BaseUpdateGraph.removeInstance("TestEDUG1");
        BaseUpdateGraph.removeInstance("TestEDUG2");
    }

    /**
     * QueryTable that adds one row per cycle.
     */
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

    /**
     * QueryTable that modifies its single row on each cycle.
     */
    final static class SourceThatModifiesItself extends QueryTable implements Runnable {
        final LongSingleValueSource svcs;

        public SourceThatModifiesItself(UpdateGraph updateGraph) {
            super(RowSetFactory.fromKeys(42).toTracking(), Collections.singletonMap("V", new LongSingleValueSource()));
            svcs = (LongSingleValueSource) getColumnSource("V", long.class);
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

        return QueryCompilerImpl.create(queryCompilerDir.toFile(), getClass().getClassLoader());
    }

    @Test
    public void testSimpleAdd() {
        final EventDrivenUpdateGraph eventDrivenUpdateGraph = EventDrivenUpdateGraph.newBuilder("TestEDUG").build();

        final ExecutionContext context = ExecutionContext.newBuilder()
                .setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope()
                .newQueryLibrary()
                .setOperationInitializer(OPERATION_INITIALIZATION)
                .setQueryCompiler(compilerForUnitTests())
                .build();
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
        final EventDrivenUpdateGraph eventDrivenUpdateGraph = new EventDrivenUpdateGraph.Builder("TestEDUG").build();

        final ExecutionContext context = ExecutionContext.newBuilder()
                .setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope()
                .newQueryLibrary()
                .setOperationInitializer(OPERATION_INITIALIZATION)
                .setQueryCompiler(compilerForUnitTests())
                .build();
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
                final long xv = xcs.getLong(rowSet.firstRowKey());
                TestCase.assertEquals(2L * (steps + 1), xv);
            } while (steps++ < 100);
            TestCase.assertEquals(1, updated.size());
        }
    }

    @Test
    public void testUpdatePerformanceTracker() {
        final Table upt = UpdatePerformanceTracker.getQueryTable();

        final EventDrivenUpdateGraph eventDrivenUpdateGraph1 = EventDrivenUpdateGraph.newBuilder("TestEDUG1").build();
        final EventDrivenUpdateGraph eventDrivenUpdateGraph2 = EventDrivenUpdateGraph.newBuilder("TestEDUG2").build();

        // first empty flush
        eventDrivenUpdateGraph1.requestRefresh();
        eventDrivenUpdateGraph2.requestRefresh();

        final int count1 = 10;
        final int count2 = 20;
        final int time1 = 10;
        final int time2 = 5;

        // the work we care about
        final Object ref1 = doWork(eventDrivenUpdateGraph1, time1, count1 - 1);
        final Object ref2 = doWork(eventDrivenUpdateGraph2, time2, count2 - 1);

        // force a flush
        eventDrivenUpdateGraph1.resetNextFlushTime();
        eventDrivenUpdateGraph2.resetNextFlushTime();
        eventDrivenUpdateGraph1.requestRefresh();
        eventDrivenUpdateGraph2.requestRefresh();

        defaultUpdateGraph.requestRefresh();

        final Table inRange;
        final ExecutionContext context = ExecutionContext.newBuilder()
                .setUpdateGraph(defaultUpdateGraph)
                .emptyQueryScope()
                .newQueryLibrary()
                .setQueryCompiler(compilerForUnitTests())
                .setOperationInitializer(OPERATION_INITIALIZATION)
                .build();
        try (final SafeCloseable ignored = context.open()) {
            final Table uptAgged = upt.where("!isNull(EntryId)").aggBy(
                    Aggregation.AggSum("UsageNanos", "InvocationCount", "RowsModified"),
                    "UpdateGraph", "EntryId");
            assertEquals(defaultUpdateGraph, uptAgged.getUpdateGraph());
            inRange = defaultUpdateGraph.sharedLock().computeLocked(() -> uptAgged.update(
                    "EIUExpectedMillis = UpdateGraph==`TestEDUG1` ? " + time1 + " : " + time2,
                    "TotalExpectedTime=InvocationCount * EIUExpectedMillis * 1_000_000L",
                    "InRange=(UsageNanos > 0.9 * TotalExpectedTime) && (UsageNanos < 2.5 * TotalExpectedTime)"));
        }
        TableTools.show(inRange);

        final Table compare =
                inRange.dropColumns("EntryId", "UsageNanos", "EIUExpectedMillis", "TotalExpectedTime");
        TableTools.show(compare);

        final Table expect = TableTools.newTable(stringCol("UpdateGraph", "TestEDUG1", "TestEDUG2"),
                longCol("InvocationCount", count1, count2),
                longCol("RowsModified", count1, count2), booleanCol("InRange", true, true));
        TstUtils.assertTableEquals(expect, compare);
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

    private Object doWork(final EventDrivenUpdateGraph eventDrivenUpdateGraph, final int durationMillis,
            final int steps) {
        final ExecutionContext context = ExecutionContext.newBuilder()
                .setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope()
                .newQueryLibrary()
                .setQueryCompiler(compilerForUnitTests())
                .setOperationInitializer(OPERATION_INITIALIZATION)
                .build();
        try (final SafeCloseable ignored = context.open()) {
            final SourceThatModifiesItself modifySource = new SourceThatModifiesItself(eventDrivenUpdateGraph);
            final Table updated =
                    eventDrivenUpdateGraph.sharedLock().computeLocked(() -> modifySource.update("X="
                            + getClass().getName() + ".sleepValue(" + (1000L * 1000L * durationMillis) + ", 2 * V)"));

            int step = 0;
            do {
                TestCase.assertEquals(1, updated.size());
                eventDrivenUpdateGraph.requestRefresh();
            } while (++step < steps);
            TestCase.assertEquals(1, updated.size());

            // so that we do not lose the reference
            return updated;
        }
    }
}
