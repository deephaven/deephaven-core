//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
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
import io.deephaven.engine.table.impl.select.SortedClockFilter;
import io.deephaven.engine.table.impl.select.UnsortedClockFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.testutil.StepClock;
import io.deephaven.engine.testutil.filters.ReindexingRowSetCapturingFilter;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.TestClock;
import io.deephaven.qst.type.Type;
import io.deephaven.util.SafeCloseable;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.stringCol;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        final Table res0 = testFilterVisibility(partitionSize, false, filter0, filter1);

        // ensure the inner filter sees only the two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        filter0.reset();
        filter1.reset();

        // do the same test, but make the inner filter a serial filter; partitioning filter checks all rows
        final Table res1 = testFilterVisibility(partitionSize, false, filter0.withSerial(), filter1);

        // ensure the inner filter sees all four partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 4 * partitionSize);
        // ensure the partitioning filter sees only the rows from the partition column data index
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        assertTableEquals(res0, res1);
        Assert.eq(res0.size(), "res0.size()", 2 * (partitionSize / 2));
    }

    @Test
    public void testPartitioningFilterDoesNotRespectSerialWithStatefulDefault() {
        testPartitioningFilterDoesNotRespectSerialWithStatefulDefault(false, false);
        testPartitioningFilterDoesNotRespectSerialWithStatefulDefault(true, false);
        testPartitioningFilterDoesNotRespectSerialWithStatefulDefault(false, true);
        testPartitioningFilterDoesNotRespectSerialWithStatefulDefault(true, true);
    }

    public void testPartitioningFilterDoesNotRespectSerialWithStatefulDefault(boolean statelessByDefault, boolean iterateFilters) {
        final boolean oldStatefulDefault = QueryTable.STATELESS_FILTERS_BY_DEFAULT;
        try (final SafeCloseable ignored = () -> {
            QueryTable.STATELESS_FILTERS_BY_DEFAULT = oldStatefulDefault;
        }) {
            QueryTable.STATELESS_FILTERS_BY_DEFAULT = statelessByDefault;

            final long partitionSize = 128;
            final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
            final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(RawString.of("partition.equalsIgnoreCase(`A`) || partition.equalsIgnoreCase(`B`)"));

            final Table res0 = testFilterVisibility(partitionSize, iterateFilters, filter0, filter1);


            // ensure the inner filter sees only the two partitions' data
            Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
            // ensure we see the partition filter as filtering only the partitioned rows
            Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

            filter0.reset();
            filter1.reset();

            // do the same test, but make the inner filter a serial filter; partitioning filter checks all rows
            final WhereFilter explicitSerial = filter1.withSerial();
            final Table res1 = testFilterVisibility(partitionSize, iterateFilters, filter0, explicitSerial);
            assertTrue(filter0.permitParallelization());
            assertFalse(explicitSerial.permitParallelization());

            // ensure the inner filter sees all four partitions' data
            Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 4 * partitionSize);
            // ensure the partitioning filter sees all the values
            Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 2 * partitionSize);

            assertTableEquals(res0, res1);
            Assert.eq(res0.size(), "res0.size()", 2 * (partitionSize / 2));
        }
    }

    @Test
    public void testPartitioningFilterWithRespectsBarrier() {
        final Object barrier = new Object();
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final Table res0 = testFilterVisibility(partitionSize, false, filter0.withDeclaredBarriers(barrier), filter1);

        // ensure the inner filter sees only the two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        filter0.reset();
        filter1.reset();

        // do the same test, but make the outer filter respect the barrier on the inner filter
        final Table res1 = testFilterVisibility(
                partitionSize, false, filter0.withDeclaredBarriers(barrier), filter1.withRespectedBarriers(barrier));

        // ensure the inner filter sees all four partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 4 * partitionSize);
        // ensure the partitioning filter sees only the rows from the partition column data index
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        assertTableEquals(res0, res1);
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
                partitionSize, false, filter0, filter1.withDeclaredBarriers(barrier), filter2.withRespectedBarriers(barrier));

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
                partitionSize, false, filter1.withDeclaredBarriers(barrier), filter0.withSerial(),
                filter2.withRespectedBarriers(barrier));

        // ensure the inner filter sees two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the barrier partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);
        // however, the respects barrier could not be lifted, it uses the data index from a filtered table that
        // was created by filter0 (so the index reflects that filtering)
        Assert.eq(filter2.numRowsProcessed(), "filter2.numRowsProcessed()", 2);

        Assert.eq(res0.size(), "res0.size()", partitionSize / 2);
    }

    private Table testFilterVisibility(final long partitionSize, boolean iterateFilters, final Filter... filters) {
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

        if (iterateFilters) {
            Table result = source;
            for (Filter filter : filters) {
                result = result.where(filter);
            }
            return result.coalesce();
        } else {
            return source.where(Filter.and(filters)).coalesce();
        }
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
        // ensure the partitioning filter sees only the rows from the partition column data index
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        assertTableEquals(res0, res1);
        Assert.eq(res0.size(), "res0.size()", 2 * (partitionSize / 2));
    }

    @Test
    public void testDeferredPartitioningFilterWithRespectsBarrier() {
        final Object barrier = new Object();
        final long partitionSize = 128;
        final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final Table res0 = testDeferredFilterVisibility(partitionSize, filter0.withDeclaredBarriers(barrier), filter1);

        // ensure the inner filter sees only the two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        filter0.reset();
        filter1.reset();

        // do the same test, but make the outer filter respect the barrier on the inner filter
        final Table res1 = testDeferredFilterVisibility(
                partitionSize, filter0.withDeclaredBarriers(barrier), filter1.withRespectedBarriers(barrier));

        // ensure the inner filter sees all four partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", 4 * partitionSize);
        // ensure the partitioning filter sees only the rows from the partition column data index
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        assertTableEquals(res0, res1);
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
                partitionSize, filter0, filter1.withDeclaredBarriers(barrier), filter2.withRespectedBarriers(barrier));

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
                partitionSize, RawString.of("II < 128"), filter1.withDeclaredBarriers(barrier), filter0.withSerial(),
                filter2.withRespectedBarriers(barrier));

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

    private Table testStaticFilterSplit(final long partitionSize, final Filter... filters) {
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
                        ColumnDefinition.ofLong("II"),
                        ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                null);

        Table result = source;
        for (Filter filter : filters) {
            result = result.where(filter);
        }

        return result;
    }

    @Test
    public void testClockFilterReordering() {
        final TestClock clock = new TestClock();
        clock.setMillis(Instant.now().toEpochMilli());

        final long partitionSize = 128;

        final RowSetCapturingFilter filter0 =
                new RowSetCapturingFilter(new UnsortedClockFilter("Timestamp", clock, true));
        final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("A"), Literal.of("B")));
        final Table res0 = testStaticFilterSplit(partitionSize, filter0, filter1);

        final Table coalesced = res0.coalesce();

        TableTools.show(coalesced);

        // ensure the inner filter sees only the two partitions' data
        Assert.eq(filter0.numRowsProcessed(), "filter0.numRowsProcessed()", partitionSize * 2);
        // ensure we see the barrier partition filter as filtering only the partitioned rows
        Assert.eq(filter1.numRowsProcessed(), "filter1.numRowsProcessed()", 4);

        Assert.eq(coalesced.size(), "res0.size()", partitionSize * 2);
    }


    @Test
    public void testSortedClockFilterReorderingWithAttribute() {
        final long partitionSize = 128;

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

        final RowSetCapturingFilter partFilter = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("B"), Literal.of("D")));
        final StepClock stepClock = new StepClock(0, 1, 2);
        final ReindexingRowSetCapturingFilter clockFilter =
                new ReindexingRowSetCapturingFilter(new SortedClockFilter("Timestamp", stepClock, true));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II"),
                        ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                null);

        // note that the first filter is always a non-partitioning filter
        final Table clockFiltered = source.where(clockFilter);
        Assert.eqTrue(clockFiltered instanceof DeferredViewTable,
                "withAttribute instanceof DeferredViewTable");
        final Table withAttribute =
                clockFiltered.withAttributes(Collections.singletonMap(Table.ADD_ONLY_TABLE_ATTRIBUTE, true));
        Assert.eqTrue(withAttribute instanceof DeferredViewTable,
                "withAttribute instanceof DeferredViewTable");

        final Table partitionFiltered = withAttribute.where(partFilter);
        Assertions.assertThat(partitionFiltered).isInstanceOf(QueryTable.class);

        // ensure the inner filter sees two partitions' data
        Assert.eq(clockFilter.numRowsProcessed(), "clockFilter.numRowsProcessed()", 2 * partitionSize);
        // ensure we see the barrier partition filter as filtering only the partitioned rows
        Assert.eq(partFilter.numRowsProcessed(), "partFilter.numRowsProcessed()", 4);

        TableTools.show(partitionFiltered);
    }

    @Test
    public void testDeferredWhereWithEmptyWhere() {
        final long partitionSize = 128;

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

        final RowSetCapturingFilter iiFilter =
                new RowSetCapturingFilter(FilterComparison.eq(ColumnName.of("II"), Literal.of(10L)));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II"),
                        ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                null);

        final Table deferredFilter = source.where(iiFilter);

        Assert.eqTrue(deferredFilter instanceof DeferredViewTable,
                "deferredFilter instanceof DeferredViewTable");

        final Table emptyWhere = deferredFilter.where();

        Assert.eqTrue(emptyWhere instanceof QueryTable, "partitionFiltered instanceof QueryTable");

        // ensure the inner filter sees two partitions' data
        Assert.eq(iiFilter.numRowsProcessed(), "iiFilter.numRowsProcessed()", 4 * partitionSize);
    }

    @Test
    public void testDeferredDropPartition() {
        final long partitionSize = 128;

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

        final RowSetCapturingFilter iiFilter =
                new RowSetCapturingFilter(FilterComparison.eq(ColumnName.of("II"), Literal.of(10L)));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II"),
                        ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                null);

        final Table deferredDrop = source.dropColumns("partition");

        final Table filtered = deferredDrop.where(iiFilter);
        final Table coalesced = filtered.coalesce();

        // ensure the inner filter sees two partitions' data
        Assert.eq(iiFilter.numRowsProcessed(), "iiFilter.numRowsProcessed()", 4 * partitionSize);

        assertEquals(4, coalesced.size());
    }

    @Test
    public void testPostViewFilterInCoalesce() {
        final long partitionSize = 128;

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

        final RowSetCapturingFilter kkFilter =
                new RowSetCapturingFilter(FilterComparison.eq(ColumnName.of("KK"), Literal.of(20L)));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II"),
                        ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                null);

        final Table deferredView = source.updateView("KK=II * 2");

        final Table filtered = deferredView.where(kkFilter);
        final Table coalesced = filtered.coalesce();

        // ensure the inner filter sees two partitions' data
        Assert.eq(kkFilter.numRowsProcessed(), "iiFilter.numRowsProcessed()", 4 * partitionSize);

        assertEquals(4, coalesced.size());

        TableTools.show(coalesced);
    }

    @Test
    public void testForceCoalesceWithPartitionFilter() {
        final long partitionSize = 128;

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

        final RowSetCapturingFilter kkFilter =
                new RowSetCapturingFilter(FilterComparison.eq(ColumnName.of("KK"), Literal.of(20L)));
        final RowSetCapturingFilter iiFilter =
                new RowSetCapturingFilter(FilterComparison.eq(ColumnName.of("II"), Literal.of(10L)));

        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II"),
                        ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                null);

        final Table deferredView = source.updateView("KK=II * 2");

        final Table filtered = deferredView.where(kkFilter);
        Assertions.assertThat(filtered).isInstanceOf(DeferredViewTable.class);
        final Table partFilter = filtered.where("partition in `A`");
        Assertions.assertThat(partFilter).isInstanceOf(QueryTable.class);

        // ensure the inner filter sees two partitions' data
        Assert.eq(kkFilter.numRowsProcessed(), "kkFilter.numRowsProcessed()", partitionSize);

        assertEquals(1, partFilter.size());

        TableTools.show(partFilter);

        final Table preViewFiltered = deferredView.where(iiFilter);
        Assertions.assertThat(preViewFiltered).isInstanceOf(DeferredViewTable.class);
        final Table preViewWithPart = preViewFiltered.where("partition in `B`");
        Assertions.assertThat(preViewWithPart).isInstanceOf(QueryTable.class);
        // ensure the inner filter sees two partitions' data
        Assert.eq(iiFilter.numRowsProcessed(), "iiFilter.numRowsProcessed()", partitionSize);
        assertEquals(1, preViewWithPart.size());

        kkFilter.reset();
        final Table partAndPost = deferredView.where(
                Filter.and(Arrays.asList(kkFilter, FilterComparison.eq(ColumnName.of("partition"), Literal.of("C")))));
        Assertions.assertThat(partAndPost).isInstanceOf(QueryTable.class);
        Assert.eq(kkFilter.numRowsProcessed(), "kkFilter.numRowsProcessed()", partitionSize);
        assertEquals(1, partAndPost.size());
    }

    @Test
    public void testSelectDistinctSimple() {
        final long partitionSize = 128;

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
                null);

        final Table selectDistinct1 = source.selectDistinct("partition");
        final Table selectDistinct1a = source.selectDistinct("x=partition + `_x`");

        final Table deferredView = source.updateView("K = II * 2");
        final Table selectDistinct2 = deferredView.selectDistinct("partition");
        final Table selectDistinct2a = deferredView.selectDistinct("x=partition + `_x`");

        Table expectedNoChanges = TableTools.newTable(stringCol("partition", "A", "B", "C", "D"));
        Table expectedWithX = TableTools.newTable(stringCol("x", "A_x", "B_x", "C_x", "D_x"));
        assertTableEquals(expectedNoChanges, selectDistinct1);
        assertTableEquals(expectedWithX, selectDistinct1a);
        assertTableEquals(expectedNoChanges, selectDistinct2);
        assertTableEquals(expectedWithX, selectDistinct2a);

        final Table selectDistinct3 = source.selectDistinct("II");
        final Table selectDistinct3a = deferredView.selectDistinct("K");
        assertTableEquals(TableTools.emptyTable(partitionSize).updateView("II=ii"), selectDistinct3);
        assertTableEquals(TableTools.emptyTable(partitionSize).updateView("K=ii * 2"), selectDistinct3a);
    }

    @Test
    public void testSelectDistinctWithChangedPartition() {
        final long partitionSize = 128;

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
                null);

        final Table deferredView = source.updateView("partition = partition + `_x`");
        final Table selectDistinct1 = deferredView.selectDistinct("partition");
        final Table selectDistinct2 = deferredView.selectDistinct("II");

        assertTableEquals(TableTools.newTable(stringCol("partition", "A_x", "B_x", "C_x", "D_x")), selectDistinct1);
        assertTableEquals(TableTools.emptyTable(partitionSize).updateView("II=ii"), selectDistinct2);
    }
}
