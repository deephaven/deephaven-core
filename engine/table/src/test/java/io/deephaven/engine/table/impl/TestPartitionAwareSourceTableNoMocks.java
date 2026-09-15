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
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.apache.commons.lang3.mutable.MutableObject;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
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
import java.util.Map;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
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

    public void testPartitioningFilterDoesNotRespectSerialWithStatefulDefault(boolean statelessByDefault,
            boolean iterateFilters) {
        final boolean oldStatefulDefault = QueryTable.STATELESS_FILTERS_BY_DEFAULT;
        try (final SafeCloseable ignored = () -> {
            QueryTable.STATELESS_FILTERS_BY_DEFAULT = oldStatefulDefault;
        }) {
            QueryTable.STATELESS_FILTERS_BY_DEFAULT = statelessByDefault;

            final long partitionSize = 128;
            final RowSetCapturingFilter filter0 = new RowSetCapturingFilter(RawString.of("II < 64"));
            final RowSetCapturingFilter filter1 = new RowSetCapturingFilter(
                    RawString.of("partition.equalsIgnoreCase(`A`) || partition.equalsIgnoreCase(`B`)"));

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
                partitionSize, false, filter0, filter1.withDeclaredBarriers(barrier),
                filter2.withRespectedBarriers(barrier));

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
        // the respects barrier could not be lifted but operates on constant column regions (vs. rows). Since,
        // the filter is performed as a chunk filter, no rows are actually processed by the filter.
        Assert.eq(filter2.numRowsProcessed(), "filter2.numRowsProcessed()", 0);

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

    @Test
    public void testVirtualRowVariablesBeforeFilter() {
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

        final Table withRowVariables = source.updateView("X=ii + 1");
        Assertions.assertThat(withRowVariables).isInstanceOf(QueryTable.class);
        final Table filtered = withRowVariables.where("II % 2 == 0");

        Table expected = TableTools.emptyTable(partitionSize * 4).updateView("X=ii + 1").where("ii % 2 == 0");
        assertTableEquals(expected, filtered.view("X"));

        final Table withRowVariablesView = source.view("II", "X=ii + 1");
        Assertions.assertThat(withRowVariablesView).isInstanceOf(QueryTable.class);
        final Table viewFiltered = withRowVariablesView.where("II % 2 == 0");
        assertTableEquals(expected, viewFiltered.view("X"));

        // column arrays also coalesce
        final Table withArray = source.view("II", "X=II_.size()");
        Assertions.assertThat(withArray).isInstanceOf(QueryTable.class);
        final Table withArrayFiltered = withArray.where("II % 2 == 0");
        assertTableEquals(TableTools.emptyTable(partitionSize * 2).view("X=" + (partitionSize * 4) + "L"),
                withArrayFiltered.view("X"));
    }

    private static final TableDefinition PARTITIONED_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("partition").withPartitioning(),
            ColumnDefinition.ofLong("II"));

    /**
     * A partition-aware source table that already carries {@code partitioningColumnFilters}, as
     * {@code getFilteredTable} produces for a {@code where} over partitioning columns. Built directly, because
     * {@code where} coalesces its result and so does not hand back the filtered source table itself.
     */
    private PartitionAwareSourceTable filteredPartitionedSource(
            final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider,
            final String description,
            final WhereFilter... partitioningColumnFilters) {
        return new PartitionAwareSourceTable(
                PARTITIONED_DEFINITION,
                description,
                RegionedTableComponentFactoryImpl.INSTANCE,
                locationProvider,
                ExecutionContext.getContext().getUpdateGraph(),
                Map.of("partition", PARTITIONED_DEFINITION.getColumn("partition")),
                partitioningColumnFilters);
    }

    private static PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider(
            final PartitionAwareSourceTableTestUtils.TestTDS tds,
            final String... partitions) {
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                (PartitionAwareSourceTableTestUtils.TableLocationProviderImpl) tds
                        .getTableLocationProvider(new PartitionAwareSourceTableTestUtils.TableKeyImpl());
        for (final String partition : partitions) {
            locationProvider.appendLocation(new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl(partition));
        }
        return locationProvider;
    }

    private static DynamicWhereFilter partitionFilter(final Table setTable) {
        return new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("partition"));
    }

    /** Rows per location for the partition-tracking tests. */
    private static final int PARTITION_SIZE = 4;

    /**
     * A refreshing source table over {@code locationProvider}, with no partitioning column filters of its own.
     */
    private Table partitionedSource(
            final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider,
            final String description) {
        return new PartitionAwareSourceTable(
                PARTITIONED_DEFINITION,
                description,
                RegionedTableComponentFactoryImpl.INSTANCE,
                locationProvider,
                ExecutionContext.getContext().getUpdateGraph());
    }

    /**
     * The {@code partition} column a result covering {@code partitions} must have, {@link #PARTITION_SIZE} rows each.
     */
    private static Table expectedPartitions(final String... partitions) {
        return TableTools.newTable(stringCol("partition", Arrays.stream(partitions)
                .flatMap(partition -> IntStream.range(0, PARTITION_SIZE).mapToObj(ignored -> partition))
                .toArray(String[]::new)));
    }

    /** Discover one more location, with the same size as the rest. */
    private static void appendSizedLocation(
            final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider,
            final String partition) {
        final PartitionAwareSourceTableTestUtils.TableLocationKeyImpl locationKey =
                new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl(partition);
        locationProvider.appendLocation(locationKey);
        locationProvider.locations.get(locationKey).setSize(PARTITION_SIZE);
    }

    /**
     * Every table that inherits a partitioning column filter must get its own copy of it. A {@link WhereFilter}
     * accumulates per-operation state as it is applied, so tables sharing one filter instance would fail as soon as the
     * second of them was coalesced. A {@link DynamicWhereFilter} over a static set table is used here because it
     * rejects reuse explicitly; a refreshing one could not be a partitioning column filter at all.
     */
    @Test
    public void testPartitioningFiltersAreCopiedForDerivedTables() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                locationProvider(tds, "A", "B");
        final Table setTable = TableTools.newTable(stringCol("partition", "A"));

        final PartitionAwareSourceTable filteredSource =
                filteredPartitionedSource(locationProvider, "derivedTables", partitionFilter(setTable));

        // A plain copy, a redefinition that keeps the partitioning column, and one that drops it. Each owns its
        // filters, so each can be coalesced without disturbing the others.
        final Table copied = filteredSource.copy();
        final Table withoutData = filteredSource.dropColumns("II");
        final Table withoutPartition = filteredSource.dropColumns("partition");

        assertEquals(1, filteredSource.coalesce().size());
        assertEquals(1, copied.coalesce().size());
        assertEquals(1, withoutData.coalesce().size());
        assertEquals(1, withoutPartition.coalesce().size());
    }

    /**
     * Location discovery filters the newly found location keys every time it runs, so a partitioning column filter is
     * applied once per pass and must be copied for each. Without that, the second pass fails on a filter that has
     * already been applied.
     */
    @Test
    public void testPartitioningFilterIsCopiedForEachLocationDiscovery() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                locationProvider(tds, "A", "B");
        final Table setTable = TableTools.newTable(stringCol("partition", "A", "C"));

        final PartitionAwareSourceTable filteredSource =
                filteredPartitionedSource(locationProvider, "locationDiscovery", partitionFilter(setTable));
        final Table coalesced = filteredSource.coalesce();
        assertEquals(1, coalesced.size());

        // A second discovery pass runs the same filter again, against the newly found location.
        updateGraph.getDelegate().startCycleForUnitTests(false);
        locationProvider.appendLocation(new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl("C"));
        updateGraph.refreshSources();
        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.getDelegate().completeCycleForUnitTests();

        assertFalse(coalesced.isFailed());
        assertEquals(2, coalesced.size());
    }

    /**
     * A refreshing filter over a partitioning column must survive location discovery, and must take the newly
     * discovered locations into account.
     * <p>
     * Because such a filter is applied after coalescing rather than to the location keys, discovery does not run it at
     * all: the source table includes every location, and the filter sees the rows the new location contributes as an
     * ordinary upstream addition.
     * <p>
     * The timeout bounds any regression that hangs discovery. It runs the body on another thread, which is why the
     * execution context is opened explicitly rather than relying on {@link #setUp}.
     */
    @Test(timeout = 60_000)
    public void testRefreshingPartitioningFilterAcrossLocationDiscovery() {
        try (final SafeCloseable ignoredContext = updateGraph.getContext().open()) {
            final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
            final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                    locationProvider(tds, "A", "B");
            locationProvider.locations.values().forEach(location -> location.setSize(PARTITION_SIZE));
            final QueryTable setTable = TstUtils.testRefreshingTable(i(0, 1).toTracking(),
                    stringCol("partition", "A", "C"));

            final Table source = partitionedSource(locationProvider, "refreshingDiscovery");
            final Table filtered = source.where(partitionFilter(setTable));
            assertTableEquals(expectedPartitions("A"), filtered.view("partition"));

            // C is discovered, and it is in the set.
            updateGraph.getDelegate().startCycleForUnitTests(false);
            appendSizedLocation(locationProvider, "C");
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.getDelegate().completeCycleForUnitTests();

            assertFalse(filtered.isFailed());
            assertTableEquals(expectedPartitions("A", "C"), filtered.view("partition"));
        }
    }

    /**
     * The same discovery, but driven on the simulated update graph thread, which is where {@code refreshSources} runs
     * in production. That thread can neither wait for a set filter's listener nor read it consistently, which is why a
     * refreshing filter must not take part in discovery at all.
     * <p>
     * {@link CapturingUpdateGraph#refreshSources()} runs on the calling thread, so the test-thread variant above does
     * not exercise this path.
     */
    @Test(timeout = 60_000)
    public void testRefreshingPartitioningFilterAcrossLocationDiscoveryOnUpdateThread() {
        try (final SafeCloseable ignoredContext = updateGraph.getContext().open()) {
            final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
            final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                    locationProvider(tds, "A", "B");
            locationProvider.locations.values().forEach(location -> location.setSize(PARTITION_SIZE));
            final QueryTable setTable = TstUtils.testRefreshingTable(i(0, 1).toTracking(),
                    stringCol("partition", "A", "C"));

            final Table source = partitionedSource(locationProvider, "refreshingDiscoveryOnUpdateThread");
            final Table filtered = source.where(partitionFilter(setTable));
            assertTableEquals(expectedPartitions("A"), filtered.view("partition"));

            // Capture the cause if the result fails, so that the failure is diagnosable.
            final MutableObject<Throwable> tableFailure = new MutableObject<>();
            filtered.addUpdateListener(new InstrumentedTableUpdateListenerAdapter("capture", filtered, false) {
                @Override
                public void onUpdate(final TableUpdate upstream) {}

                @Override
                public void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
                    tableFailure.setValue(originalException);
                }
            });

            updateGraph.getDelegate().startCycleForUnitTests(false);
            appendSizedLocation(locationProvider, "C");
            updateGraph.getDelegate().refreshUpdateSourceForUnitTests(() -> {
                assertTrue(updateGraph.currentThreadProcessesUpdates());
                try (final SafeCloseable ignored = updateGraph.getContext().open()) {
                    updateGraph.refreshSources();
                }
            });
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.getDelegate().completeCycleForUnitTests();

            if (tableFailure.getValue() != null) {
                throw new AssertionError("location discovery failed on the update thread: " + tableFailure.getValue(),
                        tableFailure.getValue());
            }
            assertFalse(filtered.isFailed());
            assertTableEquals(expectedPartitions("A", "C"), filtered.view("partition"));
        }
    }

    /**
     * A refreshing filter over a partitioning column keeps tracking its set table: a partition whose value leaves the
     * set is removed, and one whose value joins it is added.
     * <p>
     * This is why such a filter is applied after coalescing rather than to the location keys. Location discovery
     * evaluates the partitioning column filters once per batch of newly discovered keys and keeps no listener on them
     * ({@link SourceTable#maybeAddLocations} discards the {@code where} it runs), so nothing there could react to the
     * set changing.
     */
    @Test
    public void testRefreshingPartitioningFilterTracksSetChanges() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                locationProvider(tds, "A", "B", "C");
        locationProvider.locations.values().forEach(location -> location.setSize(PARTITION_SIZE));

        final QueryTable setTable = TstUtils.testRefreshingTable(i(0, 1).toTracking(),
                stringCol("partition", "A", "B"));

        final Table source = partitionedSource(locationProvider, "tracksSetChanges");
        final Table filtered = source.where(partitionFilter(setTable));
        assertTableEquals(expectedPartitions("A", "B"), filtered.view("partition"));

        // B leaves the set and C joins it.
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(setTable, i(1));
            TstUtils.addToTable(setTable, i(2), stringCol("partition", "C"));
            setTable.notifyListeners(i(2), i(1), i());
        });

        assertFalse(filtered.isFailed());
        assertTableEquals(expectedPartitions("A", "C"), filtered.view("partition"));
    }

    /**
     * Demoting a refreshing filter must not drag the filters after it out of the fast path.
     * <p>
     * Filters in a conjunction may be reordered freely except across a serial filter or a barrier, and the fast path
     * already hoists static partitioning filters over everything else; a demoted filter is one more thing to hoist
     * over. So only a filter that respects a barrier the refreshing filter declares has to follow it (a serial filter
     * follows everything before it regardless), and the existing barrier bookkeeping already arranges that: a demoted
     * filter contributes no barriers to the prioritized set.
     */
    @Test
    public void testRefreshingPartitioningFilterDoesNotDemoteLaterFilters() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                locationProvider(tds, "A", "B", "C", "D");
        locationProvider.locations.values().forEach(location -> location.setSize(PARTITION_SIZE));
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0, 1).toTracking(),
                stringCol("partition", "A", "B"));

        // The static partitioning filter is still prioritized. It sees the four location keys rather than the rows,
        // and the locations it excludes never join the table: B, the only partition in the result, is the first
        // included location and so occupies the first region.
        final RowSetCapturingFilter independent = new RowSetCapturingFilter(FilterIn.of(
                ColumnName.of("partition"), Literal.of("B"), Literal.of("C")));
        final Table result = partitionedSource(locationProvider, "laterFilterPrioritized")
                .where(Filter.and(partitionFilter(setTable), independent));
        Assert.eq(independent.numRowsProcessed(), "independent.numRowsProcessed()", 4);
        assertTableEquals(expectedPartitions("B"), result.view("partition"));
        Assert.eq(result.getRowSet().firstRowKey(), "result.getRowSet().firstRowKey()",
                RegionedColumnSource.getFirstRowKey(0), "first region");

        // Respecting a barrier the refreshing filter declares puts it back behind that filter, after coalescing. Every
        // location is included now, so B is the second region rather than the first.
        final Object barrier = new Object();
        final Table barriered = partitionedSource(locationProvider, "laterFilterBehindBarrier")
                .where(Filter.and(
                        partitionFilter(setTable).withDeclaredBarriers(barrier),
                        FilterIn.of(ColumnName.of("partition"), Literal.of("B"), Literal.of("C"))
                                .withRespectedBarriers(barrier)));
        assertTableEquals(expectedPartitions("B"), barriered.view("partition"));
        Assert.eq(barriered.getRowSet().firstRowKey(), "barriered.getRowSet().firstRowKey()",
                RegionedColumnSource.getFirstRowKey(1), "second region");
    }

    /**
     * A {@link DynamicWhereFilter} is refreshing exactly when its set table is. Over a static set it is an ordinary
     * partitioning column filter and keeps the location-key fast path, by both routes: {@code where} directly on the
     * source table, and {@code where} on a {@link DeferredViewTable} over it. Either way, only the selected locations
     * are ever included, so the first selected partition lands in the first region.
     */
    @Test
    public void testStaticSetFilterKeepsTheFastPath() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                locationProvider(tds, "A", "B", "C", "D");
        locationProvider.locations.values().forEach(location -> location.setSize(PARTITION_SIZE));
        final Table setTable = TableTools.newTable(stringCol("partition", "C", "D"));

        // Directly on the source table.
        final Table direct = partitionedSource(locationProvider, "staticDirect").where(partitionFilter(setTable));
        Assertions.assertThat(direct).isInstanceOf(QueryTable.class);
        assertTableEquals(expectedPartitions("C", "D"), direct.view("partition"));
        Assert.eq(direct.getRowSet().firstRowKey(), "direct.getRowSet().firstRowKey()",
                RegionedColumnSource.getFirstRowKey(0), "first region");

        // Through a deferred view: the non-partitioning filter defers, and the set filter then coalesces it.
        final Table deferred = partitionedSource(locationProvider, "staticDeferred").where("II >= 0");
        Assertions.assertThat(deferred).isInstanceOf(DeferredViewTable.class);
        final Table viaDeferred = deferred.where(partitionFilter(setTable));
        Assertions.assertThat(viaDeferred).isInstanceOf(QueryTable.class);
        assertTableEquals(expectedPartitions("C", "D"), viaDeferred.view("partition"));
        Assert.eq(viaDeferred.getRowSet().firstRowKey(), "viaDeferred.getRowSet().firstRowKey()",
                RegionedColumnSource.getFirstRowKey(0), "first region");
    }

    /**
     * Over a refreshing set the same filter is a row filter, and is deferred like one by both routes: {@code where}
     * hands back a {@link DeferredViewTable} whether applied directly to the source table or to a view over it, and the
     * filter runs once something coalesces that. Every location is included, so the first selected partition lands in
     * the third region, behind the two the filter excludes.
     */
    @Test
    public void testRefreshingSetFilterCoalescesEveryLocation() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                locationProvider(tds, "A", "B", "C", "D");
        locationProvider.locations.values().forEach(location -> location.setSize(PARTITION_SIZE));
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0, 1).toTracking(),
                stringCol("partition", "C", "D"));

        // Directly on the source table.
        final Table direct =
                partitionedSource(locationProvider, "refreshingDirect").where(partitionFilter(setTable));
        Assertions.assertThat(direct).isInstanceOf(DeferredViewTable.class);
        final Table directCoalesced = direct.coalesce();
        assertTableEquals(expectedPartitions("C", "D"), directCoalesced.view("partition"));
        Assert.eq(directCoalesced.getRowSet().firstRowKey(), "directCoalesced.getRowSet().firstRowKey()",
                RegionedColumnSource.getFirstRowKey(2), "third region");

        // Through a deferred view.
        final Table viaDeferred = partitionedSource(locationProvider, "refreshingDeferred")
                .where("II >= 0")
                .where(partitionFilter(setTable));
        Assertions.assertThat(viaDeferred).isInstanceOf(DeferredViewTable.class);
        final Table viaDeferredCoalesced = viaDeferred.coalesce();
        assertTableEquals(expectedPartitions("C", "D"), viaDeferredCoalesced.view("partition"));
        Assert.eq(viaDeferredCoalesced.getRowSet().firstRowKey(), "viaDeferredCoalesced.getRowSet().firstRowKey()",
                RegionedColumnSource.getFirstRowKey(2), "third region");
    }

    /**
     * A refreshing set filter applied first must not cost a later static partitioning filter its location pruning. The
     * refreshing filter is a row filter now, so it is deferred like any other, and the static filter that follows is
     * still prioritized: only its locations are included, so the first selected partition lands in the first region.
     */
    @Test
    public void testStaticPartitioningFilterStillPrunesAfterRefreshingSetFilter() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                locationProvider(tds, "A", "B", "C", "D");
        locationProvider.locations.values().forEach(location -> location.setSize(PARTITION_SIZE));
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0, 1).toTracking(),
                stringCol("partition", "A", "B"));

        final Table result = partitionedSource(locationProvider, "refreshingThenStatic")
                .where(partitionFilter(setTable))
                .where(FilterIn.of(ColumnName.of("partition"), Literal.of("B"), Literal.of("C")));
        assertTableEquals(expectedPartitions("B"), result.view("partition"));
        Assert.eq(result.getRowSet().firstRowKey(), "result.getRowSet().firstRowKey()",
                RegionedColumnSource.getFirstRowKey(0), "first region");
    }
}
