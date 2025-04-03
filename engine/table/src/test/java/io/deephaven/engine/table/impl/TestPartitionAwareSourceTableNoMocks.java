//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.SortColumn;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.ImmutableTableKey;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.AbstractColumnLocation;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableDataService;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.PartitionedTableLocationKey;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionByte;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionChar;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionDouble;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionFloat;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionInt;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionLong;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionObject;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionShort;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionByte;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionChar;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionDouble;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionFloat;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionInt;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionLong;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionObject;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionShort;
import io.deephaven.generic.region.AppendOnlyRegionAccessor;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        final TestTDS tds = new TestTDS();
        final TableKey tableKey = new TableKeyImpl();
        final TableLocationProviderImpl tableLocationProvider =
                (TableLocationProviderImpl) tds.getTableLocationProvider(tableKey);
        tableLocationProvider.appendLocation(new TableLocationKeyImpl("A"));

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
        tableLocationProvider.appendLocation(new TableLocationKeyImpl("B"));
        updateGraph.refreshSources();
        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.getDelegate().completeCycleForUnitTests();

        Assert.eqFalse(table.isFailed(), "table.isFailed()");
        Assert.eq(table.size(), "table.size()", 2);
        TableTools.show(table);
    }

    @Test
    public void testSizeChangeGeneratesModify() {
        final TestTDS tds = new TestTDS();
        final TableKey tableKey = new TableKeyImpl();
        final TableLocationProviderImpl tableLocationProvider =
                (TableLocationProviderImpl) tds.getTableLocationProvider(tableKey);
        tableLocationProvider.appendLocation(new TableLocationKeyImpl("A"));

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

    private static class TestTDS extends AbstractTableDataService {
        public TestTDS() {
            super("TestTDS");
        }

        @Override
        protected @NotNull TableLocationProvider makeTableLocationProvider(@NotNull TableKey tableKey) {
            return new TableLocationProviderImpl(tableKey);
        }
    }

    private static class Subscription {
    }

    private static class TableLocationProviderImpl extends AbstractTableLocationProvider {
        private Subscription subscription = null;

        private Map<TableLocationKey, TableLocationImpl> locations = new LinkedHashMap<>();

        private TableLocationProviderImpl(@NotNull final TableKey tableKey) {
            super(tableKey, true, TableUpdateMode.APPEND_ONLY, TableUpdateMode.APPEND_ONLY);
        }

        public void appendLocation(@NotNull final TableLocationKeyImpl locationKey) {
            locations.put(locationKey, new TableLocationImpl((TableKeyImpl) getKey(), locationKey));
            handleTableLocationKeyAdded(locationKey);
        }

        @Override
        protected @NotNull TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
            return locations.get(locationKey);
        }

        @Override
        public void refresh() {
            locations.keySet().forEach(this::handleTableLocationKeyAdded);
        }

        @Override
        protected void activateUnderlyingDataSource() {
            final Subscription localSubscription = subscription = new Subscription();
            locations.keySet().forEach(this::handleTableLocationKeyAdded);
            activationSuccessful(localSubscription);
        }

        @Override
        protected void deactivateUnderlyingDataSource() {
            subscription = null;
        }

        @Override
        protected <T> boolean matchSubscriptionToken(final T token) {
            return token == subscription;
        }

        @Override
        public String getImplementationName() {
            return "PythonTableDataService.TableLocationProvider";
        }
    }

    private static class TableKeyImpl implements ImmutableTableKey {
        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append(getImplementationName());
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public String getImplementationName() {
            return "PythonTableDataService.TableKeyImpl";
        }
    }

    public static class TableLocationKeyImpl extends PartitionedTableLocationKey {
        private TableLocationKeyImpl(final String partitionName) {
            super(Map.of("partition", partitionName));
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append(getImplementationName());
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public String getImplementationName() {
            return "TestPartitionAwareSourceTableNoMocks.TableLocationKeyImpl";
        }
    }

    private static class TableLocationImpl extends AbstractTableLocation {
        private volatile Subscription subscription;
        private long size = 1;

        private TableLocationImpl(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKey locationKey) {
            super(tableKey, locationKey, true);
        }

        public void setSize(final long size) {
            this.size = Math.max(this.size, size);
            handleUpdate(RowSetFactory.flat(size), System.currentTimeMillis());
        }

        @Override
        protected @NotNull ColumnLocation makeColumnLocation(@NotNull final String name) {
            return new ColumnLocationImpl(this, name);
        }

        @Override
        public void refresh() {
            handleUpdate(RowSetFactory.flat(size), System.currentTimeMillis());
        }

        @Override
        public @NotNull List<SortColumn> getSortedColumns() {
            return List.of();
        }

        @Override
        public @NotNull List<String[]> getDataIndexColumns() {
            return List.of();
        }

        @Override
        public boolean hasDataIndex(@NotNull final String... columns) {
            return false;
        }

        @Override
        public @Nullable BasicDataIndex loadDataIndex(@NotNull final String... columns) {
            return null;
        }

        @Override
        protected void activateUnderlyingDataSource() {
            final Subscription localSubscription = subscription = new Subscription();
            handleUpdate(RowSetFactory.flat(size), System.currentTimeMillis());
            activationSuccessful(localSubscription);
        }

        @Override
        protected void deactivateUnderlyingDataSource() {}

        @Override
        protected <T> boolean matchSubscriptionToken(final T token) {
            return token == subscription;
        }

        @Override
        public String getImplementationName() {
            return "TestPartitionAwareSourceTableNoMocks.TableLocationImpl";
        }
    }

    @FunctionalInterface
    private interface RowAppender {
        void appendRow(@NotNull WritableChunk<Values> chunk, long rowKey);
    }

    private static class ColumnLocationImpl extends AbstractColumnLocation {
        private static final int PAGE_SIZE = 1 << 16;
        private static final long REGION_MASK = RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK;

        protected ColumnLocationImpl(
                @NotNull final TableLocationImpl tableLocation,
                @NotNull final String name) {
            super(tableLocation, name);
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public ColumnRegionChar<Values> makeColumnRegionChar(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionChar<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter((chunk, rowKey) -> {
                        chunk.asWritableCharChunk().add((char) (rowKey % 128));
                    }));
        }

        @Override
        public ColumnRegionByte<Values> makeColumnRegionByte(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionByte<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter((chunk, rowKey) -> {
                        chunk.asWritableByteChunk().add((byte) rowKey);
                    }));
        }

        @Override
        public ColumnRegionShort<Values> makeColumnRegionShort(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionShort<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter((chunk, rowKey) -> {
                        chunk.asWritableShortChunk().add((short) rowKey);
                    }));
        }

        @Override
        public ColumnRegionInt<Values> makeColumnRegionInt(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionInt<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter((chunk, rowKey) -> {
                        chunk.asWritableShortChunk().add((short) rowKey);
                    }));

        }

        @Override
        public ColumnRegionLong<Values> makeColumnRegionLong(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionLong<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter((chunk, rowKey) -> {
                        chunk.asWritableLongChunk().add(rowKey);
                    }));
        }

        @Override
        public ColumnRegionFloat<Values> makeColumnRegionFloat(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionFloat<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter((chunk, rowKey) -> {
                        chunk.asWritableFloatChunk().add((float) rowKey);
                    }));
        }

        @Override
        public ColumnRegionDouble<Values> makeColumnRegionDouble(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionDouble<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter((chunk, rowKey) -> {
                        chunk.asWritableDoubleChunk().add((double) rowKey);
                    }));
        }

        @Override
        public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(
                @NotNull final ColumnDefinition<TYPE> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionObject<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter((chunk, rowKey) -> {
                        chunk.asWritableObjectChunk().add(rowKey);
                    }));
        }

        private class TableServiceGetRangeAdapter implements AppendOnlyRegionAccessor<Values> {
            private final RowAppender rowAppender;

            public TableServiceGetRangeAdapter(@NotNull RowAppender rowAppender) {
                this.rowAppender = rowAppender;
            }

            @Override
            public void readChunkPage(
                    final long firstRowPosition,
                    final int minimumSize,
                    @NotNull final WritableChunk<Values> destination) {
                destination.setSize(0);
                for (int ii = 0; ii < minimumSize; ++ii) {
                    rowAppender.appendRow(destination, firstRowPosition + ii);
                }
            }

            @Override
            public long size() {
                return getTableLocation().getSize();
            }
        }
    }
}
