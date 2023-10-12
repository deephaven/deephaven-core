package io.deephaven.engine.updategraph.impl;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.configuration.DataDir;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.LongSingleValueSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
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
        }

        @Override
        public void run() {
            svcs.set(svcs.getLong(0) + 1);
            notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(), getRowSet());
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
                AbstractUpdateGraph.DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS);

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
                AbstractUpdateGraph.DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS);

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
                final long xpv = xcs.getPrevLong (updated.getRowSet().firstRowKey());
                final long xv = xcs.getLong (updated.getRowSet().firstRowKey());
                TestCase.assertEquals(2L * (steps - 1), xpv);
                TestCase.assertEquals(2L * steps, xv);
            } while (steps++ < 100);
            TestCase.assertEquals(steps, updated.size());
        }
    }
}
