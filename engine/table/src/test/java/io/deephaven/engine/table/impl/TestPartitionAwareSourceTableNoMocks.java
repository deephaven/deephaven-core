//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestPartitionAwareSourceTableNoMocks {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private CapturingUpdateGraph updateGraph;
    private SafeCloseable executionContextCloseable;

    @Before
    public void setUp() {
        updateGraph = new CapturingUpdateGraph(ExecutionContext.getContext().getUpdateGraph().cast());
        executionContextCloseable = updateGraph.getContext().open();
    }

    @After
    public void tearDown() {
        executionContextCloseable.close();
    }

    @Test
    public void testConcurrentInstantiationWithSameCycleNotification() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds =
                new PartitionAwareSourceTableTestUtils.TestTDS();
        final TableKey tableKey = new PartitionAwareSourceTableTestUtils.TableKeyImpl();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                (PartitionAwareSourceTableTestUtils.TableLocationProviderImpl) tds
                        .getTableLocationProvider(tableKey);
        tableLocationProvider.appendLocation(new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl("A"));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II")),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                ExecutionContext.getContext().getUpdateGraph());

        updateGraph.getDelegate().startCycleForUnitTests(false);
        final Table table = source.coalesce();
        tableLocationProvider.appendLocation(new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl("B"));
        updateGraph.refreshSources();
        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.getDelegate().completeCycleForUnitTests();

        Assert.eqFalse(table.isFailed(), "table.isFailed()");
        Assert.eq(table.size(), "table.size()", 2);
        TableTools.show(table);
    }

    @Test
    public void testSizeChangeGeneratesModify() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds =
                new PartitionAwareSourceTableTestUtils.TestTDS();
        final TableKey tableKey = new PartitionAwareSourceTableTestUtils.TableKeyImpl();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                (PartitionAwareSourceTableTestUtils.TableLocationProviderImpl) tds
                        .getTableLocationProvider(tableKey);
        tableLocationProvider.appendLocation(new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl("A"));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II")),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                ExecutionContext.getContext().getUpdateGraph());

        updateGraph.getDelegate().startCycleForUnitTests(false);
        final Table table = source.coalesce();

        final DataIndex dataIndex = DataIndexer.getDataIndex(table, "partition");
        Assert.neqNull(dataIndex, "dataIndex");
        final TableUpdateValidator tuv = TableUpdateValidator.make((QueryTable) dataIndex.table());

        tableLocationProvider.locations.values().forEach(location -> location.setSize(128));
        updateGraph.refreshSources();
        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.getDelegate().completeCycleForUnitTests();
        Assert.eqFalse(tuv.hasFailed(), "tuv.hasFailed()");

        Assert.eqFalse(table.isFailed(), "table.isFailed()");
        Assert.eq(table.size(), "table.size()", 128);
        TableTools.show(table);
    }
}
