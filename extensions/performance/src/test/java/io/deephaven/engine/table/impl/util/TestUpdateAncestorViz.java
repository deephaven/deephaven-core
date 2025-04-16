//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.*;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.ForkJoinPoolOperationInitializer;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.updategraph.impl.BaseUpdateGraph;
import io.deephaven.engine.updategraph.impl.EventDrivenUpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.function.ThrowingSupplier;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static io.deephaven.engine.util.TableTools.timeTable;
import static org.junit.jupiter.api.Assertions.*;

public class TestUpdateAncestorViz {
    EventDrivenUpdateGraph defaultUpdateGraph;
    File cacheDir;

    @BeforeEach
    public void before() throws IOException {
        // the default update is necessary for the update performance tracker
        clearUpdateGraphInstances();
        UpdatePerformanceTracker.resetForUnitTests();
        defaultUpdateGraph = EventDrivenUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME).build();
        cacheDir = Files.createTempDirectory("TestUpdateAncestorViz").toFile();
        cacheDir.deleteOnExit();
    }

    @AfterEach
    public void after() {
        FileUtils.cleanDirectory(cacheDir);
        clearUpdateGraphInstances();
        UpdatePerformanceTracker.resetForUnitTests();
    }

    private static void clearUpdateGraphInstances() {
        BaseUpdateGraph.removeInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME);
    }

    @Test
    public void testNaturalJoin() {
        testGraphGen("where([!isNull(Z)])",
                List.of("where([!isNull(Z)])", "naturalJoin([X], [Z, TS2=Timestamp])", "Update([X])", "Update([X, Z])",
                        "TimeTable(null,1000000000)"),
                () -> {
                    final Table ttr = timeTable("PT1s");
                    final Table tt1 = ttr.update("X=ii").update("Y=2").where("X % 2 == 1");
                    final Table tt2 = ttr.update("X=ii", "Z=2").where("X % 3 == 0");
                    return tt1.naturalJoin(tt2, "X", "Z,TS2=Timestamp").where("!isNull(Z)");
                });
    }

    @Test
    public void testMerge() {
        testGraphGen("PartitionedTable.merge()",
                List.of("PartitionedTable.merge()", "TimeTable(null,1000000000)"),
                () -> {
                    final Table ttr = timeTable("PT1s");
                    final Table tt1 = ttr.update("X=ii");
                    final Table tt2 = ttr.update("X=2*ii");
                    return TableTools.merge(tt1, tt2);
                });
    }

    private void testGraphGen(final String terminalOperation,
            final List<String> expectedNodes,
            final ThrowingSupplier<Table, RuntimeException> testSnippet) {
        final Table upl = TableLoggers.updatePerformanceLog();
        final Table ua = TableLoggers.updatePerformanceAncestorsLog();

        ExecutionContext executionContext = ExecutionContext.newBuilder().newQueryLibrary().newQueryScope()
                .setQueryCompiler(QueryCompilerImpl.create(
                        cacheDir, TestUpdateAncestorViz.class.getClassLoader()))
                .setOperationInitializer(ForkJoinPoolOperationInitializer.fromCommonPool())
                .setUpdateGraph(defaultUpdateGraph).build();

        try (final SafeCloseable ignored = executionContext.open();
                final SafeCloseable ignored2 = LivenessScopeStack.open()) {
            // noinspection unused, referential integrity
            final Table result = defaultUpdateGraph.sharedLock().computeLocked(testSnippet);

            defaultUpdateGraph.resetNextFlushTime();
            defaultUpdateGraph.requestRefresh();
            // we need to have two refreshes to flush our performance data
            defaultUpdateGraph.requestRefresh();

            TableTools.show(ua);
            TableTools.show(upl);

            long entry;
            try (final CloseablePrimitiveIteratorOfLong entryId =
                    ua.where("EntryDescription=`" + terminalOperation + "`").longColumnIterator("EntryId")) {
                assertTrue(entryId.hasNext());
                entry = entryId.nextLong();
                assertFalse(entryId.hasNext());
            }

            final MutableGraph graph = UpdateAncestorViz.graph(new long[] {entry}, upl, ua).toMutable();

            System.out.println(Graphviz.fromGraph(graph).render(Format.DOT));

            for (final String expectedNode : expectedNodes) {
                long count = graph.nodes().stream().filter(n -> safeContains(n.get("label"), expectedNode)).count();
                assertEquals(1, count, () -> "Expected one node for " + expectedNode);
            }
        }
    }

    private static boolean safeContains(Object s, String pattern) {
        if (s instanceof String) {
            return ((String) s).contains(pattern);
        }
        return false;
    }
}
