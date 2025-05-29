//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.table.impl.util.TableLoggers;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.impl.BaseUpdateGraph;
import io.deephaven.engine.updategraph.impl.EventDrivenUpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TestPartitionAwareSourceTableUpdateAncestor {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private EventDrivenUpdateGraph updateGraph;

    File cacheDir;

    @Before
    public void setUp() throws IOException {
        // the default update is necessary for the update performance tracker
        clearUpdateGraphInstances();
        UpdatePerformanceTracker.resetForUnitTests();
        updateGraph = EventDrivenUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME).build();
        cacheDir = Files.createTempDirectory("TestUpdateAncestorViz").toFile();
        cacheDir.deleteOnExit();
    }

    @After
    public void tearDown() {
        FileUtils.cleanDirectory(cacheDir);
        clearUpdateGraphInstances();
        UpdatePerformanceTracker.resetForUnitTests();
    }

    private static void clearUpdateGraphInstances() {
        BaseUpdateGraph.removeInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME);
    }

    @Test
    public void testUncoalescedMerge() {
        // noinspection unused
        final Table upl = TableLoggers.updatePerformanceLog();
        // noinspection unused
        final Table ua = TableLoggers.updatePerformanceAncestorsLog();

        final PartitionAwareSourceTableTestUtils.TestTDS tds =
                new PartitionAwareSourceTableTestUtils.TestTDS();
        final TableKey tableKey = new PartitionAwareSourceTableTestUtils.TableKeyImpl();

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II")),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tds.getTableLocationProvider(tableKey),
                ExecutionContext.getContext().getUpdateGraph());

        final Table m = updateGraph.sharedLock().computeLocked(() -> TableTools.merge(source));
        TstUtils.assertTableEquals(source, m);
    }
}
