package io.deephaven.engine.updategraph.impl;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.DataDir;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.context.TestQueryCompiler;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;

import java.nio.file.Path;
import java.util.Collections;

public class TestEventDrivenUpdateGraph extends BaseArrayTestCase {
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

    private QueryCompiler compilerForUnitTests() {
        final Path queryCompilerDir = DataDir.get()
                .resolve("io.deephaven.engine.updategraph.impl.TestEventDrivenUpdateGraph.compilerForUnitTests");

        return QueryCompiler.create(queryCompilerDir.toFile(), getClass().getClassLoader());
    }

    public void testSimple() {

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
}
