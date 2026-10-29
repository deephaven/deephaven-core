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
import io.deephaven.engine.context.PoisonedUpdateGraph;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.StandaloneLivenessManager;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.SortedClockFilter;
import io.deephaven.engine.table.impl.select.TimeSeriesFilter;
import io.deephaven.engine.table.impl.select.UnsortedClockFilter;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegatingBase;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import org.apache.commons.lang3.mutable.MutableObject;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.testutil.StepClock;
import io.deephaven.engine.testutil.filters.ReindexingRowSetCapturingFilter;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.TestClock;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import io.deephaven.engine.table.PartitionedTableFactory;
import io.deephaven.engine.table.PartitionedTable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.*;

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

    /**
     * A filter on non-partitioning columns is deferred, and the deferred table asks each filter whether it is
     * refreshing before the filter has seen its source table. Here the window can still move past the rows, so the
     * filter is refreshing once it has seen the source.
     */
    @Test
    public void testDeferredTimeSeriesFilter() {
        // the test locations' timestamps are a few nanoseconds after the epoch, so all of them are in the window
        final TestClock clock = new TestClock().setNanos(DateTimeUtils.MINUTE);
        checkDeferredTimeSeriesFilter(clock, false, true, 4 * 128);
        checkDeferredTimeSeriesFilter(clock, true, true, 0);
    }

    /**
     * As {@link #testDeferredTimeSeriesFilter()}, but every row is already outside the window and the source is static,
     * so once the filter has seen the source it knows that nothing will ever change.
     */
    @Test
    public void testDeferredTimeSeriesFilterStatic() {
        final TestClock clock = new TestClock().setMillis(Instant.now().toEpochMilli());
        checkDeferredTimeSeriesFilter(clock, false, false, 0);
        checkDeferredTimeSeriesFilter(clock, true, false, 4 * 128);
    }

    private void checkDeferredTimeSeriesFilter(
            final TestClock clock,
            final boolean invert,
            final boolean expectRefreshing,
            final long expectedSize) {
        // Coalescing applies copies of the deferred filters, so only the coalesced table, not the filters we hold,
        // learns whether anything can change.
        final long partitionSize = 128;

        final WhereFilter timeSeriesFilter = TimeSeriesFilter.newBuilder()
                .columnName("Timestamp").period("PT5m").clock(clock).invert(invert).build();
        assertTrue(timeSeriesFilter.isRefreshing());
        final Table bare = testStaticFilterSplit(partitionSize, timeSeriesFilter);
        assertTrue(bare instanceof DeferredViewTable);
        final Table bareCoalesced = bare.coalesce();
        assertEquals(expectedSize, bareCoalesced.size());
        assertEquals(expectRefreshing, bareCoalesced.isRefreshing());

        final WhereFilter composedTimeSeriesFilter = TimeSeriesFilter.newBuilder()
                .columnName("Timestamp").period("PT5m").clock(clock).invert(invert).build();
        final WhereFilter composed = DisjunctiveFilter.of(
                composedTimeSeriesFilter, WhereFilter.of(RawString.of("II < 0"))).withDeclaredBarriers(new Object());
        assertTrue(composed.isRefreshing());
        final Table viaComposed = testStaticFilterSplit(partitionSize, composed);
        assertTrue(viaComposed instanceof DeferredViewTable);
        final Table composedCoalesced = viaComposed.coalesce();
        assertEquals(expectedSize, composedCoalesced.size());
        assertEquals(expectRefreshing, composedCoalesced.isRefreshing());
    }

    /**
     * One thread coalesces a deferred table during an update cycle without the update graph lock. Its condition uses a
     * query scope parameter, so it cannot filter previous values, and the deferred where's snapshot waits for the
     * shared lock. Another thread, already waiting for the exclusive lock when that happens, holds it once the cycle
     * completes and coalesces the same deferred table. The two coalesces' where operations never overlap, both
     * coalesces complete with the same result, and only that result filters rows added afterwards.
     */
    @Test
    public void testDeferredCoalesceWhileExclusiveLockHolderCoalesces() throws Exception {
        checkDeferredCoalesceWhileExclusiveLockHolderCoalesces(true);
    }

    /**
     * As {@link #testDeferredCoalesceWhileExclusiveLockHolderCoalesces()}, but the source is not coalesced first, so
     * the lock-free thread coalesces the source within the attempt that it abandons.
     */
    @Test
    public void testDeferredCoalesceOfUncoalescedSourceWhileExclusiveLockHolderCoalesces() throws Exception {
        checkDeferredCoalesceWhileExclusiveLockHolderCoalesces(false);
    }

    private void checkDeferredCoalesceWhileExclusiveLockHolderCoalesces(final boolean coalesceSourceFirst)
            throws Exception {
        final long partitionSize = 128;
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                partitionedLocationProvider(partitionSize);
        final Table source = refreshingPartitionedSource(tableLocationProvider);
        if (coalesceSourceFirst) {
            // the lock-free thread's attempt can then be abandoned only at the deferred where's snapshot
            source.coalesce();
        }
        ExecutionContext.getContext().getQueryScope().putParam("threshold", 0L);
        // copies of the filters, one for each coalesce, share what they capture
        final RowSetCapturingFilter filter = new RowSetCapturingFilter(RawString.of("II + threshold >= 0"));
        final OverlapRecordingFilter overlapRecordingFilter = new OverlapRecordingFilter(filter);
        final Table coalesced = coalesceWhileExclusiveLockHolderCoalesces(source.where(overlapRecordingFilter));
        assertEquals(4 * partitionSize, coalesced.size());
        assertEquals(1, overlapRecordingFilter.maxOperationsInProgress.get());

        filter.reset();
        tableLocationProvider.locations.values().forEach(location -> location.setSize(2 * partitionSize));
        updateGraph.getDelegate().startCycleForUnitTests(false);
        updateGraph.refreshSources();
        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.getDelegate().completeCycleForUnitTests();
        assertEquals(8 * partitionSize, coalesced.size());
        assertEquals(4 * partitionSize, filter.numRowsProcessed());
    }

    /**
     * As {@link #testDeferredCoalesceWhileExclusiveLockHolderCoalesces()}, but the deferred where is memoizable, so the
     * exclusive lock holder's where reaches the same memoized operation that the other thread is computing.
     */
    @Test
    public void testDeferredMemoizedCoalesceWhileExclusiveLockHolderCoalesces() throws Exception {
        checkDeferredMemoizedCoalesceWhileExclusiveLockHolderCoalesces(true);
    }

    /**
     * As {@link #testDeferredMemoizedCoalesceWhileExclusiveLockHolderCoalesces()}, but the source is not coalesced
     * first, so the lock-free thread coalesces the source within the attempt that it abandons.
     */
    @Test
    public void testDeferredMemoizedCoalesceOfUncoalescedSourceWhileExclusiveLockHolderCoalesces() throws Exception {
        checkDeferredMemoizedCoalesceWhileExclusiveLockHolderCoalesces(false);
    }

    private void checkDeferredMemoizedCoalesceWhileExclusiveLockHolderCoalesces(final boolean coalesceSourceFirst)
            throws Exception {
        final long partitionSize = 128;
        final Table source = refreshingPartitionedSource(partitionedLocationProvider(partitionSize));
        if (coalesceSourceFirst) {
            // the lock-free thread's attempt can then be abandoned only at the deferred where's snapshot
            source.coalesce();
        }
        final WhereFilter filter = new PreviousRejectingFilter(new MatchFilter(MatchOptions.INVERTED, "II", -1L));
        final boolean oldMemoize = QueryTable.setMemoizeResults(true);
        try {
            final Table coalesced = coalesceWhileExclusiveLockHolderCoalesces(source.where(filter));
            assertEquals(4 * partitionSize, coalesced.size());
        } finally {
            QueryTable.setMemoizeResults(oldMemoize);
        }
    }

    /**
     * As {@link #testDeferredCoalesceWhileExclusiveLockHolderCoalesces()}, but the deferred where filters in parallel,
     * and on each worker its filter performs a nested snapshot that needs the update graph lock during an update cycle.
     */
    @Test
    public void testDeferredParallelCoalesceWhileExclusiveLockHolderCoalesces() throws Exception {
        checkDeferredParallelCoalesceWhileExclusiveLockHolderCoalesces(true);
    }

    /**
     * As {@link #testDeferredParallelCoalesceWhileExclusiveLockHolderCoalesces()}, but the source is not coalesced
     * first, so the lock-free thread coalesces the source within the attempt that it abandons.
     */
    @Test
    public void testDeferredParallelCoalesceOfUncoalescedSourceWhileExclusiveLockHolderCoalesces() throws Exception {
        checkDeferredParallelCoalesceWhileExclusiveLockHolderCoalesces(false);
    }

    private void checkDeferredParallelCoalesceWhileExclusiveLockHolderCoalesces(final boolean coalesceSourceFirst)
            throws Exception {
        final long partitionSize = 128;
        final Table source = refreshingPartitionedSource(partitionedLocationProvider(partitionSize));
        if (coalesceSourceFirst) {
            // the lock-free thread's attempt can then be abandoned only at the workers' nested snapshots
            source.coalesce();
        }
        final boolean oldForceParallelWhere = QueryTable.FORCE_PARALLEL_WHERE;
        final boolean oldDisableParallelWhere = QueryTable.DISABLE_PARALLEL_WHERE;
        final int oldParallelWhereSegments = QueryTable.PARALLEL_WHERE_SEGMENTS;
        final long oldParallelWhereRowsPerSegment = QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT;
        QueryTable.FORCE_PARALLEL_WHERE = true;
        QueryTable.DISABLE_PARALLEL_WHERE = false;
        QueryTable.PARALLEL_WHERE_SEGMENTS = 2;
        QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT = partitionSize;
        try {
            final NestedSnapshotFilter filter =
                    new NestedSnapshotFilter(new MatchFilter(MatchOptions.INVERTED, "II", -1L));
            final Table coalesced = coalesceWhileExclusiveLockHolderCoalesces(source.where(filter));
            assertEquals(4 * partitionSize, coalesced.size());
            assertTrue(filter.workerSnapshots.get() > 0);
        } finally {
            QueryTable.FORCE_PARALLEL_WHERE = oldForceParallelWhere;
            QueryTable.DISABLE_PARALLEL_WHERE = oldDisableParallelWhere;
            QueryTable.PARALLEL_WHERE_SEGMENTS = oldParallelWhereSegments;
            QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT = oldParallelWhereRowsPerSegment;
        }
    }

    /**
     * Before filtering, performs a nested snapshot that cannot run concurrently with an update cycle, so that it needs
     * the update graph lock while one is updating. Counts, over this filter and its copies, the nested snapshots that
     * ran on a thread other than the one that created the filter.
     */
    private static class NestedSnapshotFilter extends WhereFilterDelegatingBase {
        private final Thread creatingThread;
        private final AtomicInteger workerSnapshots;

        private NestedSnapshotFilter(final WhereFilter filter) {
            this(filter, Thread.currentThread(), new AtomicInteger());
        }

        private NestedSnapshotFilter(
                final WhereFilter filter,
                final Thread creatingThread,
                final AtomicInteger workerSnapshots) {
            super(filter);
            this.creatingThread = creatingThread;
            this.workerSnapshots = workerSnapshots;
        }

        @NotNull
        @Override
        public WritableRowSet filter(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            final UpdateGraph updateGraph = table.getUpdateGraph();
            ConstructSnapshot.callDataSnapshotFunction("NestedSnapshotFilter", new ConstructSnapshot.SnapshotControl() {
                @Override
                public Boolean usePreviousValues(final long beforeClockValue) {
                    return false;
                }

                @Override
                public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
                    return true;
                }

                @Override
                public UpdateGraph getUpdateGraph() {
                    return updateGraph;
                }
            }, (final boolean snapshotUsePrev, final long beforeClockValue) -> {
                if (LogicalClock.getState(beforeClockValue) == LogicalClock.State.Updating
                        && !updateGraph.sharedLock().isHeldByCurrentThread()
                        && !updateGraph.exclusiveLock().isHeldByCurrentThread()) {
                    throw new ConstructSnapshot.NoSnapshotAllowedException();
                }
                return true;
            });
            if (Thread.currentThread() != creatingThread) {
                workerSnapshots.incrementAndGet();
            }
            return super.filter(selection, fullSet, table, usePrev);
        }

        @Override
        public WhereFilter maybeUnwrapFilter() {
            return this;
        }

        @Override
        public WhereFilter copy() {
            return new NestedSnapshotFilter(filter.copy(), creatingThread, workerSnapshots);
        }
    }

    /**
     * A memoizable filter that cannot filter previous values, so that its where falls back to the update graph lock
     * during an update cycle.
     */
    private static class PreviousRejectingFilter extends WhereFilterDelegatingBase {
        private PreviousRejectingFilter(final WhereFilter filter) {
            super(filter);
        }

        @NotNull
        @Override
        public WritableRowSet filter(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            if (usePrev) {
                throw new PreviousFilteringNotSupported();
            }
            return super.filter(selection, fullSet, table, usePrev);
        }

        @Override
        public WhereFilter maybeUnwrapFilter() {
            return this;
        }

        @Override
        public WhereFilter copy() {
            return new PreviousRejectingFilter(filter.copy());
        }
    }

    /**
     * Records the greatest number of where operations, over this filter and its copies, that are in progress at once.
     */
    private static class OverlapRecordingFilter extends WhereFilterDelegatingBase {
        private final AtomicInteger operationsInProgress;
        private final AtomicInteger maxOperationsInProgress;

        private OverlapRecordingFilter(final WhereFilter filter) {
            this(filter, new AtomicInteger(), new AtomicInteger());
        }

        private OverlapRecordingFilter(
                final WhereFilter filter,
                final AtomicInteger operationsInProgress,
                final AtomicInteger maxOperationsInProgress) {
            super(filter);
            this.operationsInProgress = operationsInProgress;
            this.maxOperationsInProgress = maxOperationsInProgress;
        }

        @Override
        public SafeCloseable beginOperation(@NotNull final Table sourceTable) {
            maxOperationsInProgress.accumulateAndGet(operationsInProgress.incrementAndGet(), Math::max);
            final SafeCloseable delegateOperation;
            try {
                delegateOperation = super.beginOperation(sourceTable);
            } catch (final RuntimeException | Error failure) {
                operationsInProgress.decrementAndGet();
                throw failure;
            }
            return () -> {
                try (final SafeCloseable ignored = delegateOperation) {
                    operationsInProgress.decrementAndGet();
                }
            };
        }

        @Override
        public WhereFilter maybeUnwrapFilter() {
            return this;
        }

        @Override
        public WhereFilter copy() {
            return new OverlapRecordingFilter(filter.copy(), operationsInProgress, maxOperationsInProgress);
        }
    }

    /**
     * One thread coalesces {@code deferred} during an update cycle without the update graph lock, and its deferred
     * where's snapshot waits for the shared lock. Another thread, already waiting for the exclusive lock when that
     * happens, holds it once the cycle completes and coalesces {@code deferred} too. Both coalesces must complete with
     * the same result.
     *
     * @return The coalesced result
     */
    private Table coalesceWhileExclusiveLockHolderCoalesces(final Table deferred) throws Exception {
        assertTrue(deferred instanceof DeferredViewTable);
        final Table[] results = runWhileExclusiveLockHolderRuns(deferred::coalesce, deferred::coalesce);
        assertSame(results[0], results[1]);
        return results[1];
    }

    /**
     * During an update cycle, one thread starts waiting for the exclusive lock, and then another thread without the
     * update graph lock runs {@code lockFreeOperation} until it waits for the update graph lock. Once the cycle
     * completes, the first thread runs {@code exclusiveOperation} while holding the exclusive lock. Both operations
     * must complete.
     *
     * @return The results of {@code exclusiveOperation} and {@code lockFreeOperation}, in that order
     */
    private Table[] runWhileExclusiveLockHolderRuns(
            final Supplier<Table> exclusiveOperation,
            final Supplier<Table> lockFreeOperation) throws Exception {
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final UpdateGraph contextUpdateGraph = executionContext.getUpdateGraph();
        final CompletableFuture<Table> exclusiveResult = new CompletableFuture<>();
        final Thread exclusiveThread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                exclusiveResult.complete(contextUpdateGraph.exclusiveLock().computeLocked(exclusiveOperation::get));
            } catch (final Throwable failure) {
                exclusiveResult.completeExceptionally(failure);
            }
        }, "exclusive-operation");
        final CompletableFuture<Table> lockFreeResult = new CompletableFuture<>();
        final Thread lockFreeThread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                lockFreeResult.complete(lockFreeOperation.get());
            } catch (final Throwable failure) {
                lockFreeResult.completeExceptionally(failure);
            }
        }, "lock-free-operation");
        exclusiveThread.setDaemon(true);
        lockFreeThread.setDaemon(true);

        updateGraph.getDelegate().startCycleForUnitTests(false);
        try {
            exclusiveThread.start();
            awaitWaitingForUpdateGraphLock(exclusiveThread, exclusiveResult);
            lockFreeThread.start();
            awaitWaitingForUpdateGraphLock(lockFreeThread, lockFreeResult);
        } finally {
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.getDelegate().completeCycleForUnitTests();
        }

        try {
            return new Table[] {
                    exclusiveResult.get(30, TimeUnit.SECONDS),
                    lockFreeResult.get(30, TimeUnit.SECONDS)};
        } catch (final TimeoutException timeout) {
            final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            final long[] deadlockedIds = threadMXBean.findDeadlockedThreads();
            final StringBuilder deadlocked = new StringBuilder();
            if (deadlockedIds != null) {
                for (final ThreadInfo threadInfo : threadMXBean.getThreadInfo(deadlockedIds, true, true)) {
                    deadlocked.append('\n').append(threadInfo.getThreadName()).append(" waiting on ")
                            .append(threadInfo.getLockName()).append(" owned by ")
                            .append(threadInfo.getLockOwnerName());
                    for (final StackTraceElement frame : threadInfo.getStackTrace()) {
                        deadlocked.append("\n\tat ").append(frame);
                    }
                }
            }
            throw new AssertionError("Operations did not complete; deadlocked threads:" + deadlocked, timeout);
        }
    }

    /**
     * One thread computes a memoized where during an update cycle without the update graph lock; the where cannot
     * filter previous values, so it waits for the shared lock while computing. Another thread, already waiting for the
     * exclusive lock when that happens, holds it once the cycle completes and transforms a partitioned table, whose
     * transformation runs the same where on an initialization worker. Since the transforming thread holds the lock and
     * waits for the worker, the worker must not wait for the other thread's memoized computation.
     */
    @Test
    public void testTransformWorkerDoesNotWaitForMemoizedOperationWhileExclusiveLockHolderTransforms()
            throws Exception {
        final long partitionSize = 128;
        final Table source = refreshingPartitionedSource(partitionedLocationProvider(partitionSize)).coalesce();
        final WhereFilter filter = new PreviousRejectingFilter(new MatchFilter(MatchOptions.INVERTED, "II", -1L));
        final PartitionedTable partitioned = PartitionedTableFactory.ofTables(source);
        final AtomicReference<Thread> exclusiveThread = new AtomicReference<>();
        final AtomicBoolean transformedOnWorker = new AtomicBoolean();
        final boolean oldMemoize = QueryTable.setMemoizeResults(true);
        final boolean oldForceParallelSelect = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
        try {
            final Table[] results = runWhileExclusiveLockHolderRuns(() -> {
                exclusiveThread.set(Thread.currentThread());
                return partitioned.transform(constituent -> {
                    if (Thread.currentThread() != exclusiveThread.get()) {
                        transformedOnWorker.set(true);
                    }
                    return constituent.where(filter);
                }).merge();
            }, () -> source.where(filter));
            assertTrue("the transformation must run on an initialization worker", transformedOnWorker.get());
            assertEquals(4 * partitionSize, results[0].size());
            assertEquals(4 * partitionSize, results[1].size());
        } finally {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = oldForceParallelSelect;
            QueryTable.setMemoizeResults(oldMemoize);
        }
    }

    /**
     * A refreshing deferred table coalesces on a thread that may wait for the update graph, so its coalesce runs in a
     * liveness scope of its own. Once garbage is collected and the liveness referents it made unreachable are cleaned
     * up, the coalesced result is still live and still updates, because the caller's liveness scope retains it.
     */
    @Test
    public void testCoalescedResultSurvivesGarbageCollection() throws InterruptedException {
        final long partitionSize = 128;
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                partitionedLocationProvider(partitionSize);
        final Table deferred = refreshingPartitionedSource(tableLocationProvider)
                .where(new MatchFilter(MatchOptions.INVERTED, "II", -1L));
        assertTrue(deferred instanceof DeferredViewTable);
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Table coalesced = deferred.coalesce();
            assertEquals(4 * partitionSize, coalesced.size());
            for (int round = 0; round < 3; ++round) {
                awaitLivenessCleanup();
            }
            assertTrue(coalesced.tryRetainReference());
            coalesced.dropReference();

            tableLocationProvider.locations.values().forEach(location -> location.setSize(2 * partitionSize));
            updateGraph.getDelegate().startCycleForUnitTests(false);
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.getDelegate().completeCycleForUnitTests();
            assertEquals(8 * partitionSize, coalesced.size());
        }
    }

    /**
     * Collect garbage until the liveness cleanup processor has cleaned up a referent that became unreachable after this
     * method was invoked.
     */
    private static void awaitLivenessCleanup() throws InterruptedException {
        final CountDownLatch cleaned = new CountDownLatch(1);
        CleanupReferenceProcessorInstance.LIVENESS.registerPhantom(new Object(), cleaned::countDown);
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!cleaned.await(10, TimeUnit.MILLISECONDS)) {
            assertTrue("liveness cleanup did not run", System.nanoTime() < deadlineNanos);
            System.gc();
        }
    }

    /**
     * A static source table coalesces while a {@link StandaloneLivenessManager} is the current liveness manager.
     */
    @Test
    public void testStaticCoalesceWithStandaloneLivenessManager() {
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                partitionedLocationProvider(128);
        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II"),
                        ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                tableLocationProvider.getKey().toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                null);
        assertFalse(source.isRefreshing());
        final StandaloneLivenessManager livenessManager = new StandaloneLivenessManager(false);
        LivenessScopeStack.push(livenessManager);
        try {
            final Table coalesced = source.coalesce();
            assertEquals(4 * 128, coalesced.size());
            assertSame(coalesced, source.coalesce());
        } finally {
            LivenessScopeStack.pop(livenessManager);
        }
    }

    /**
     * A refreshing deferred table coalesces while a {@link StandaloneLivenessManager} is the current liveness manager.
     * Its source is already coalesced and its where is memoized, so the coalesce reuses live cached results.
     */
    @Test
    public void testRefreshingCachedCoalesceWithStandaloneLivenessManager() {
        final Table source = refreshingPartitionedSource(partitionedLocationProvider(128));
        final boolean oldMemoize = QueryTable.setMemoizeResults(true);
        try {
            final Table filtered = source.coalesce().where(new MatchFilter(MatchOptions.INVERTED, "II", -1L));
            final Table deferred = source.where(new MatchFilter(MatchOptions.INVERTED, "II", -1L));
            assertTrue(deferred instanceof DeferredViewTable);
            assertTrue(deferred.isRefreshing());
            final StandaloneLivenessManager livenessManager = new StandaloneLivenessManager(false);
            LivenessScopeStack.push(livenessManager);
            try {
                assertSame(filtered, deferred.coalesce());
            } finally {
                LivenessScopeStack.pop(livenessManager);
            }
        } finally {
            QueryTable.setMemoizeResults(oldMemoize);
        }
    }

    /**
     * A static source table built without a registered update graph coalesces.
     */
    @Test
    public void testStaticCoalesceWithoutUpdateGraph() {
        try (final SafeCloseable ignored =
                ExecutionContext.getContext().withUpdateGraph(PoisonedUpdateGraph.INSTANCE).open()) {
            final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                    partitionedLocationProvider(128);
            final Table source = new PartitionAwareSourceTable(
                    TableDefinition.of(
                            ColumnDefinition.ofString("partition").withPartitioning(),
                            ColumnDefinition.ofLong("II"),
                            ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                    tableLocationProvider.getKey().toString(),
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    tableLocationProvider,
                    null);
            assertFalse(source.isRefreshing());
            assertSame(PoisonedUpdateGraph.INSTANCE, source.getUpdateGraph());
            final Table coalesced = source.coalesce();
            assertEquals(4 * 128, coalesced.size());
            assertSame(coalesced, source.coalesce());
        }
    }

    /**
     * Wait until {@code thread} is parked waiting for the update graph lock. If {@code thread} completes first, its
     * result or failure is reported instead.
     */
    private static void awaitWaitingForUpdateGraphLock(
            final Thread thread,
            final CompletableFuture<Table> result) throws Exception {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            if (result.isDone()) {
                throw new AssertionError(thread.getName() + " completed without waiting for the update graph lock: "
                        + result.get());
            }
            final ThreadInfo threadInfo = threadMXBean.getThreadInfo(thread.getId());
            if (threadInfo != null && threadInfo.getLockInfo() != null
                    && threadInfo.getLockInfo().getClassName().startsWith(ReentrantReadWriteLock.class.getName())) {
                return;
            }
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError(thread.getName() + " is not waiting for the update graph lock: " + threadInfo);
            }
            Thread.sleep(10);
        }
    }

    /**
     * A table location provider with four partitions of {@code partitionSize} rows.
     */
    private static PartitionAwareSourceTableTestUtils.TableLocationProviderImpl partitionedLocationProvider(
            final long partitionSize) {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final TableKey tableKey = new PartitionAwareSourceTableTestUtils.TableKeyImpl();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider =
                (PartitionAwareSourceTableTestUtils.TableLocationProviderImpl) tds.getTableLocationProvider(tableKey);
        for (char partition = 'A'; partition <= 'D'; partition++) {
            tableLocationProvider.appendLocation(
                    new PartitionAwareSourceTableTestUtils.TableLocationKeyImpl(String.valueOf(partition)));
        }
        tableLocationProvider.locations.values().forEach(location -> location.setSize(partitionSize));
        return tableLocationProvider;
    }

    /**
     * A refreshing partitioned source table over {@code tableLocationProvider}.
     */
    private static Table refreshingPartitionedSource(
            final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl tableLocationProvider) {
        final Table source = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofString("partition").withPartitioning(),
                        ColumnDefinition.ofLong("II"),
                        ColumnDefinition.of("Timestamp", Type.find(Instant.class))),
                tableLocationProvider.getKey().toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                ExecutionContext.getContext().getUpdateGraph());
        assertTrue(source.isRefreshing());
        return source;
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
        final Table copied = filteredSource.copy(filteredSource.getAttributes());
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

    @Test
    public void testPartitionWhereCopiesFilterAttributes() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final PartitionAwareSourceTableTestUtils.TableLocationProviderImpl locationProvider =
                locationProvider(tds, "A", "B");
        final Table source = partitionedSource(locationProvider, "filterAttributes").withAttributes(Map.of(
                Table.SORTABLE_COLUMNS_ATTRIBUTE, "partition",
                Table.MERGED_TABLE_ATTRIBUTE, true));

        final Table filtered = source.where("partition in `A`");
        Assert.eq(filtered.getAttribute(Table.SORTABLE_COLUMNS_ATTRIBUTE), "sortable columns", "partition");
        Assert.eqFalse(filtered.hasAttribute(Table.MERGED_TABLE_ATTRIBUTE), "has merged attribute");
    }

    @Test
    public void testCopyKeepsReplacedColumnSourceManagerAttribute() {
        final PartitionAwareSourceTableTestUtils.TestTDS tds = new PartitionAwareSourceTableTestUtils.TestTDS();
        final Table source = partitionedSource(locationProvider(tds, "A", "B"), "csmAttributes");
        final String csmAttribute = source.hasAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE)
                ? Table.APPEND_ONLY_TABLE_ATTRIBUTE
                : Table.ADD_ONLY_TABLE_ATTRIBUTE;
        Assert.equals(source.getAttribute(csmAttribute), "source " + csmAttribute, Boolean.TRUE);

        final Table replaced = source.withAttributes(Map.of(csmAttribute, Boolean.FALSE));
        Assert.equals(replaced.getAttribute(csmAttribute), "replaced " + csmAttribute, Boolean.FALSE);
        final Table copied = replaced.withAttributes(Map.of("Other", "o"));
        Assert.equals(copied.getAttribute(csmAttribute), "copied " + csmAttribute, Boolean.FALSE);
        Assert.equals(copied.getAttribute("Other"), "copied Other", "o");
    }
}
