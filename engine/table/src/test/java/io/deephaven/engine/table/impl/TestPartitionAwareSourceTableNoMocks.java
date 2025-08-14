//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.literal.Literal;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

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
    }

    @Test
    public void testPartitioningFilterRespectsSerial() {
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final Table res0 = testFilterVisibility(partitionSize, filter0, filter1);

        // ensure the inner filter sees only the two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        filter0.reset();
        filter1.reset();

        // do the same test, but make the inner filter a serial filter; partitioning filter checks all rows
        final Table res1 = testFilterVisibility(partitionSize, filter0.withSerial(), filter1);

        // ensure the inner filter sees all four partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 4 * partitionSize);
        // ensure the partitioning filter sees only the rows that match filter0
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4 * (partitionSize / 2));

        TstUtils.assertTableEquals(res0, res1);
        Assert.eq(res0.size(), "res0.size()", 2 * (partitionSize / 2));
    }

    @Test
    public void testPartitioningFilterWithRespectsBarrier() {
        final Object barrier = new Object();
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final Table res0 = testFilterVisibility(partitionSize, filter0.withBarriers(barrier), filter1);

        // ensure the inner filter sees only the two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        filter0.reset();
        filter1.reset();

        // do the same test, but make the outer filter respect the barrier on the inner filter
        final Table res1 = testFilterVisibility(
                partitionSize, filter0.withBarriers(barrier), filter1.respectsBarriers(barrier));

        // ensure the inner filter sees all four partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 4 * partitionSize);
        // ensure the partitioning filter sees only the rows that match filter0
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4 * (partitionSize / 2));

        TstUtils.assertTableEquals(res0, res1);
        Assert.eq(res0.size(), "res0.size()", 2 * (partitionSize / 2));
    }

    @Test
    public void testPartitioningFiltersFullBarrier() {
        // this test ensures that two partitioning filters with a respects barrier also works when lifted
        final Object barrier = new Object();
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final RowSetCapturingFilter filter2 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("B")));
        final Table res0 = testFilterVisibility(
                partitionSize, filter0, filter1.withBarriers(barrier), filter2.respectsBarriers(barrier));

        // ensure the inner filter sees only the one partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", partitionSize);
        // ensure we see the barrier partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);
        // ensure we see the respects-barrier partition filter as further filtering the partitioned rows
        Assert.eq(filter2.numRowsProcessed(), "filter2.numRowsProcessed()", 2);

        Assert.eq(res0.size(), "res0.size()", partitionSize / 2);
    }

    @Test
    public void testPartitioningFilterSplitsBarrier() {
        // this test ensures that two partitioning filters with a respects barrier also works when lifted
        final Object barrier = new Object();
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final RowSetCapturingFilter filter2 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("B")));
        final Table res0 = testFilterVisibility(
                partitionSize, filter1.withBarriers(barrier), filter0.withSerial(), filter2.respectsBarriers(barrier));

        // ensure the inner filter sees two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the barrier partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);
        // however, the respects barrier could not be lifted, so it should match result of filter0
        Assert.eq(filter2.numRowsProcessed(), "filter2.numRowsProcessed()", 2 * (partitionSize / 2));

        Assert.eq(res0.size(), "res0.size()", partitionSize / 2);
    }

    private Table testFilterVisibility(final long partitionSize, final Filter... filters) {
        final PartitionAwareSourceTableTestUtils.TestTDS tds =
                new PartitionAwareSourceTableTestUtils.TestTDS();
        final TableKey tableKey = new PartitionAwareSourceTableTestUtils.TableKeyImpl();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                (PartitionAwareSourceTableTestUtils.TableLocationProviderImpl) tds
                        .getTableLocationProvider(tableKey);

        // create 4 partitions;
        for (char partition = 'A'; partition <= 'D'; partition++) {
            tableLocationProvider.appendLocation(
                    new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl(String.valueOf(partition)));
        }
        tableLocationProvider.locations.values().forEach(location -> location.setSize(partitionSize));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II")),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                ExecutionContext.getContext().getUpdateGraph());
        final DataIndex dataIndex = DataIndexer.getDataIndex(source, "partition");
        Assert.neqNull(dataIndex, "dataIndex");

        return source.where(Filter.and(filters)).coalesce();
    }

    @Test
    public void testDeferredPartitioningFilterRespectsSerial() {
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final Table res0 = testDeferredFilterVisibility(partitionSize, filter0, filter1);

        // ensure the inner filter sees only the two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        filter0.reset();
        filter1.reset();

        // do the same test, but make the inner filter a serial filter; partitioning filter checks all rows
        final Table res1 = testDeferredFilterVisibility(partitionSize, filter0.withSerial(), filter1);

        // ensure the inner filter sees all four partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 4 * partitionSize);
        // ensure the partitioning filter sees only the rows that match filter0
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4 * (partitionSize / 2));

        TstUtils.assertTableEquals(res0, res1);
        Assert.eq(res0.size(), "res0.size()", 2 * (partitionSize / 2));
    }

    @Test
    public void testDeferredPartitioningFilterWithRespectsBarrier() {
        final Object barrier = new Object();
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final Table res0 = testDeferredFilterVisibility(partitionSize, filter0.withBarriers(barrier), filter1);

        // ensure the inner filter sees only the two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        filter0.reset();
        filter1.reset();

        // do the same test, but make the outer filter respect the barrier on the inner filter
        final Table res1 = testDeferredFilterVisibility(
                partitionSize, filter0.withBarriers(barrier), filter1.respectsBarriers(barrier));

        // ensure the inner filter sees all four partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 4 * partitionSize);
        // ensure the partitioning filter sees only the rows that match filter0
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4 * (partitionSize / 2));

        TstUtils.assertTableEquals(res0, res1);
        Assert.eq(res0.size(), "res0.size()", 2 * (partitionSize / 2));
    }

    @Test
    public void testDeferredPartitioningFiltersFullBarrier() {
        // this test ensures that two partitioning filters with a respects barrier also works when lifted
        final Object barrier = new Object();
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final RowSetCapturingFilter filter2 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("B")));
        final Table res0 = testDeferredFilterVisibility(
                partitionSize, filter0, filter1.withBarriers(barrier), filter2.respectsBarriers(barrier));

        // ensure the inner filter sees only the one partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", partitionSize);
        // ensure we see the barrier partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);
        // ensure we see the respects-barrier partition filter as further filtering the partitioned rows
        Assert.eq(filter2.numRowsProcessed(), "filter2.numRowsProcessed()", 2);

        Assert.eq(res0.size(), "res0.size()", partitionSize / 2);
    }

    @Test
    public void testDeferredPartitioningFilterSplitsBarrier() {
        // this test ensures that two partitioning filters with a respects barrier also works when lifted
        final Object barrier = new Object();
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final RowSetCapturingFilter filter2 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("B")));
        // we need to add a "dummy" filter in the front to defer the other three
        final Table res0 = testDeferredFilterVisibility(
                partitionSize, RawString.of("II < 128"), filter1.withBarriers(barrier), filter0.withSerial(),
                filter2.respectsBarriers(barrier));

        // ensure the inner filter sees two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the barrier partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);
        // however, the respects barrier could not be lifted, so it should match result of filter0
        Assert.eq(filter2.numRowsProcessed(), "filter2.numRowsProcessed()", 2 * (partitionSize / 2));

        Assert.eq(res0.size(), "res0.size()", partitionSize / 2);
    }

    private Table testDeferredFilterVisibility(final long partitionSize, final Filter... filters) {
        final PartitionAwareSourceTableTestUtils.TestTDS tds =
                new PartitionAwareSourceTableTestUtils.TestTDS();
        final TableKey tableKey = new PartitionAwareSourceTableTestUtils.TableKeyImpl();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                (PartitionAwareSourceTableTestUtils.TableLocationProviderImpl) tds
                        .getTableLocationProvider(tableKey);

        // create 4 partitions;
        for (char partition = 'A'; partition <= 'D'; partition++) {
            tableLocationProvider.appendLocation(
                    new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl(String.valueOf(partition)));
        }
        tableLocationProvider.locations.values().forEach(location -> location.setSize(partitionSize));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II")),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                ExecutionContext.getContext().getUpdateGraph());
        final DataIndex dataIndex = DataIndexer.getDataIndex(source, "partition");
        Assert.neqNull(dataIndex, "dataIndex");

        // note that first filter is always a non-partitioning filter
        final Table intermediateResult = source.where(filters[0]);
        Assert.eqTrue(intermediateResult instanceof DeferredViewTable,
                "intermediateResult instanceof DeferredViewTable");

        return intermediateResult.where(Filter.and(Arrays.asList(filters).subList(1, filters.length))).coalesce();
    }
}
