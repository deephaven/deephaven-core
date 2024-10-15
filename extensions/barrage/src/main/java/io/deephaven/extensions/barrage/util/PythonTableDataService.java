//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.api.SortColumn;
import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.util.TableTools;
import io.deephaven.generic.region.*;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jpy.PyObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.*;

import static io.deephaven.extensions.barrage.util.ArrowToTableConverter.parseArrowIpcMessage;

@ScriptApi
public class PythonTableDataService extends AbstractTableDataService {

    private static final int PAGE_SIZE = Configuration.getInstance()
            .getIntegerForClassWithDefault(PythonTableDataService.class, "PAGE_SIZE", 1 << 16);
    private static final long REGION_MASK = RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK;

    private final BackendAccessor backend;

    @ScriptApi
    public static PythonTableDataService create(@NotNull final PyObject pyTableDataService) {
        return new PythonTableDataService(pyTableDataService);
    }

    /**
     * Construct a Deephaven {@link io.deephaven.engine.table.impl.locations.TableDataService TableDataService} wrapping
     * the provided {@link BackendAccessor}.
     */
    private PythonTableDataService(@NotNull final PyObject pyTableDataService) {
        super("PythonTableDataService");
        this.backend = new BackendAccessor(pyTableDataService);
    }

    /**
     * Get a Deephaven {@link Table} for the supplied name.
     *
     * @param tableKey The table key
     * @param live Whether the table should update as new data becomes available
     * @return The {@link Table}
     */
    @ScriptApi
    public Table makeTable(@NotNull final TableKeyImpl tableKey, final boolean live) {
        final TableLocationProviderImpl tableLocationProvider =
                (TableLocationProviderImpl) getTableLocationProvider(tableKey);
        return new PartitionAwareSourceTable(
                tableLocationProvider.tableDefinition,
                tableKey.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                tableLocationProvider,
                live ? ExecutionContext.getContext().getUpdateGraph() : null);
    }

    private static class SchemaPair {
        BarrageUtil.ConvertedArrowSchema tableSchema;
        BarrageUtil.ConvertedArrowSchema partitionSchema;
    }

    /**
     * This Backend impl marries the Python TableDataService with the Deephaven TableDataService. By performing the
     * object translation here, we can keep the Python TableDataService implementation simple and focused on the Python
     * side of the implementation.
     */
    private static class BackendAccessor {
        private final PyObject pyTableDataService;

        private BackendAccessor(
                @NotNull final PyObject pyTableDataService) {
            this.pyTableDataService = pyTableDataService;
        }

        /**
         * Get the schema for the table and partition columns.
         *
         * @param tableKey the table key
         * @return the schemas
         */
        public SchemaPair getTableSchema(
                @NotNull final TableKeyImpl tableKey) {
            final ByteBuffer[] schemas =
                    pyTableDataService.call("_table_schema", tableKey.key).getObjectArrayValue(ByteBuffer.class);
            final SchemaPair result = new SchemaPair();
            result.tableSchema = convertSchema(schemas[0]);
            result.partitionSchema = convertSchema(schemas[1]);
            return result;
        }

        private BarrageUtil.ConvertedArrowSchema convertSchema(final ByteBuffer original) {
            // The Schema instance (especially originated from Python) can't be assumed to be valid after the return
            // of this method. Until https://github.com/jpy-consortium/jpy/issues/126 is resolved, we need to make a
            // copy of
            // the header to use after the return of this method.

            try {
                final BarrageProtoUtil.MessageInfo mi = parseArrowIpcMessage(original);
                if (mi.header.headerType() != MessageHeader.Schema) {
                    throw new IllegalArgumentException("The input is not a valid Arrow Schema IPC message");
                }
                final Schema schema = new Schema();
                Message.getRootAsMessage(mi.header.getByteBuffer()).header(schema);

                return BarrageUtil.convertArrowSchema(schema);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse Arrow IPC message", e);
            }
        }

        /**
         * Get the existing partitions for the table.
         *
         * @param tableKey the table key
         * @param listener the listener to call with each partition's table location key
         */
        public void getExistingPartitions(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final Consumer<TableLocationKeyImpl> listener) {
            final BiConsumer<TableLocationKeyImpl, ByteBuffer[]> convertingListener =
                    (tableLocationKey, byteBuffers) -> {
                        // TODO: parse real partition column values into map
                        listener.accept(new TableLocationKeyImpl(tableLocationKey.locationKey, Map.of()));
                    };

            pyTableDataService.call("_existing_partitions", tableKey.key, convertingListener);
        }

        /**
         * Subscribe to new partitions for the table.
         *
         * @param tableKey the table key
         * @param listener the listener to call with each partition's table location key
         * @return a {@link SafeCloseable} that can be used to cancel the subscription
         */
        public SafeCloseable subscribeToNewPartitions(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final Consumer<TableLocationKeyImpl> listener) {
            final BiConsumer<TableLocationKeyImpl, ByteBuffer[]> convertingListener =
                    (tableLocationKey, byteBuffers) -> {
                        // TODO: parse real partition column values into map
                        listener.accept(new TableLocationKeyImpl(tableLocationKey.locationKey, Map.of()));
                    };

            final PyObject cancellationCallback = pyTableDataService.call(
                    "_subscribe_to_new_partitions", tableKey.key, convertingListener);
            return () -> {
                cancellationCallback.call("__call__");
            };
        }

        /**
         * Get the size of a partition.
         *
         * @param tableKey the table key
         * @param tableLocationKey the table location key
         * @param listener the listener to call with the partition size
         */
        public void getPartitionSize(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKeyImpl tableLocationKey,
                @NotNull final LongConsumer listener) {
            pyTableDataService.call("_partition_size", tableKey.key, tableLocationKey.locationKey, listener);
        }

        /**
         * Subscribe to changes in the size of a partition.
         *
         * @param tableKey the table key
         * @param tableLocationKey the table location key
         * @param listener the listener to call with the partition size
         * @return a {@link SafeCloseable} that can be used to cancel the subscription
         */
        public SafeCloseable subscribeToPartitionSizeChanges(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKeyImpl tableLocationKey,
                @NotNull final LongConsumer listener) {

            final PyObject cancellationCallback = pyTableDataService.call(
                    "_subscribe_to_partition_size_changes", tableKey.key, tableLocationKey.locationKey, listener);

            return () -> {
                cancellationCallback.call("__call__");
            };
        }

        /**
         * Get a range of data for a column.
         *
         * @param tableKey the table key
         * @param tableLocationKey the table location key
         * @param columnDefinition the column definition
         * @param firstRowPosition the first row position
         * @param minimumSize the minimum size
         * @return the number of rows read
         */
        public BarrageMessage getColumnValues(
                TableKeyImpl tableKey,
                TableLocationKeyImpl tableLocationKey,
                ColumnDefinition<?> columnDefinition,
                long firstRowPosition,
                int minimumSize) {
            // TODO: should we tell python maximum size that can be accepted?
            // TODO: do we want to use column definition? what is best for the "lazy" python user?
            // A - we use string column name
            // B - Column Definition (column name + type)
            // C - Arrow Field type
            return ConstructSnapshot.constructBackplaneSnapshot(
                    this, (BaseTable<?>) TableTools.emptyTable(0));
        }
    }

    @Override
    protected @NotNull TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
        if (!(tableKey instanceof TableKeyImpl)) {
            throw new UnsupportedOperationException(String.format("%s: Unsupported TableKey %s", this, tableKey));
        }
        return new TableLocationProviderImpl((TableKeyImpl) tableKey);
    }

    /**
     * {@link TableKey} implementation for TableService.
     */
    public static class TableKeyImpl implements ImmutableTableKey {

        private final PyObject key;

        public TableKeyImpl(@NotNull final PyObject key) {
            this.key = key;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final TableKeyImpl otherTableKey = (TableKeyImpl) other;
            return this.key.equals(otherTableKey.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append("PythonTableDataService.TableKey[name=")
                    .append(key.toString())
                    .append(']');
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public String getImplementationName() {
            return "PythonTableDataService.TableKey";
        }
    }

    private static final AtomicReferenceFieldUpdater<TableLocationProviderImpl, Subscription> TABLE_LOC_PROVIDER_SUBSCRIPTION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(TableLocationProviderImpl.class, Subscription.class, "subscription");

    /**
     * {@link TableLocationProvider} implementation for TableService.
     */
    private class TableLocationProviderImpl extends AbstractTableLocationProvider {

        private final TableDefinition tableDefinition;

        volatile Subscription subscription = null;

        private TableLocationProviderImpl(@NotNull final TableKeyImpl tableKey) {
            super(tableKey, true);
            // TODO NOCOMMIT: Add partition column to table definition
            tableDefinition = backend.getTableSchema(tableKey).tableSchema.tableDef;
        }

        @Override
        protected @NotNull TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
            if (!(locationKey instanceof TableLocationKeyImpl)) {
                throw new UnsupportedOperationException(String.format(
                        "%s: Unsupported TableLocationKey %s", this, locationKey));
            }
            return new TableLocationImpl((TableKeyImpl) getKey(), (TableLocationKeyImpl) locationKey);
        }

        @Override
        public void refresh() {
            TableKeyImpl key = (TableKeyImpl) getKey();
            backend.getExistingPartitions(key, this::handleTableLocationKey);
        }

        @Override
        protected void activateUnderlyingDataSource() {
            TableKeyImpl key = (TableKeyImpl) getKey();
            final Subscription localSubscription = subscription = new Subscription();
            localSubscription.cancellationCallback = backend.subscribeToNewPartitions(key, tableLocationKey -> {
                if (localSubscription != subscription) {
                    // we've been cancelled and/or replaced
                    return;
                }

                handleTableLocationKey(tableLocationKey);
            });
        }

        @Override
        protected void deactivateUnderlyingDataSource() {
            final Subscription localSubscription = subscription;
            if (localSubscription != null
                    && TABLE_LOC_PROVIDER_SUBSCRIPTION_UPDATER.compareAndSet(this, localSubscription, null)) {
                localSubscription.cancellationCallback.close();
            }
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

    /**
     * {@link TableLocationKey} implementation for TableService.
     */
    public static class TableLocationKeyImpl extends PartitionedTableLocationKey {

        private final PyObject locationKey;

        public TableLocationKeyImpl(@NotNull final PyObject locationKey) {
            this(locationKey, Map.of());
        }

        private TableLocationKeyImpl(
                @NotNull final PyObject locationKey,
                @NotNull final Map<String, Comparable<?>> partitionValues) {
            super(partitionValues);
            this.locationKey = locationKey;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final TableLocationKeyImpl otherTableLocationKey = (TableLocationKeyImpl) other;
            return this.locationKey.equals(otherTableLocationKey.locationKey);
        }

        @Override
        public int hashCode() {
            return locationKey.hashCode();
        }

        @Override
        public int compareTo(@NotNull final TableLocationKey other) {
            if (getClass() != other.getClass()) {
                throw new ClassCastException(String.format("Cannot compare %s to %s", getClass(), other.getClass()));
            }
            final TableLocationKeyImpl otherTableLocationKey = (TableLocationKeyImpl) other;
            // TODO: What exactly is supposed to happen if partition values are equal but these are different locations?
            return PartitionsComparator.INSTANCE.compare(partitions, otherTableLocationKey.partitions);
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append("PythonTableDataService.TableLocationKeyImpl[partitions=")
                    .append(PartitionsFormatter.INSTANCE, partitions)
                    .append(']');
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public String getImplementationName() {
            return "PythonTableDataService.TableLocationKeyImpl";
        }
    }

    private static final AtomicReferenceFieldUpdater<TableLocationImpl, Subscription> TABLE_LOC_SUBSCRIPTION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(TableLocationImpl.class, Subscription.class, "subscription");

    /**
     * {@link TableLocation} implementation for TableService.
     */
    public class TableLocationImpl extends AbstractTableLocation {

        volatile Subscription subscription = null;

        private long size;

        private TableLocationImpl(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKeyImpl locationKey) {
            super(tableKey, locationKey, true);
        }

        private void checkSizeChange(final long newSize) {
            // TODO: should we throw if python tells us size decreased? or just ignore smaller sizes?
            synchronized (getStateLock()) {
                if (size >= newSize) {
                    return;
                }

                size = newSize;
                handleUpdate(RowSetFactory.flat(size), System.currentTimeMillis());
            }
        }

        @Override
        protected @NotNull ColumnLocation makeColumnLocation(@NotNull final String name) {
            return new ColumnLocationImpl(this, name);
        }

        @Override
        public void refresh() {
            final TableKeyImpl key = (TableKeyImpl) getTableKey();
            final TableLocationKeyImpl location = (TableLocationKeyImpl) getKey();
            backend.getPartitionSize(key, location, this::checkSizeChange);
        }

        @Override
        public @NotNull List<SortColumn> getSortedColumns() {
            // TODO: we may be able to fetch this from the metadata or table definition post conversion
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
            final TableKeyImpl key = (TableKeyImpl) getTableKey();
            final TableLocationKeyImpl location = (TableLocationKeyImpl) getKey();

            final Subscription localSubscription = subscription = new Subscription();
            localSubscription.cancellationCallback = backend.subscribeToPartitionSizeChanges(key, location, newSize -> {
                if (localSubscription != subscription) {
                    // we've been cancelled and/or replaced
                    return;
                }

                checkSizeChange(newSize);
            });
        }

        @Override
        protected void deactivateUnderlyingDataSource() {
            final Subscription localSubscription = subscription;
            if (localSubscription != null
                    && TABLE_LOC_SUBSCRIPTION_UPDATER.compareAndSet(this, localSubscription, null)) {
                localSubscription.cancellationCallback.close();
            }
        }

        @Override
        protected <T> boolean matchSubscriptionToken(final T token) {
            return token == subscription;
        }

        @Override
        public String getImplementationName() {
            return "PythonTableDataService.TableLocationImpl";
        }
    }

    /**
     * {@link ColumnLocation} implementation for TableService.
     */
    public class ColumnLocationImpl extends AbstractColumnLocation {

        protected ColumnLocationImpl(
                @NotNull final PythonTableDataService.TableLocationImpl tableLocation,
                @NotNull final String name) {
            super(tableLocation, name);
        }

        @Override
        public boolean exists() {
            // Schema is consistent across all partitions with the same segment ID. This implementation should be
            // changed when/if we add support for schema evolution.
            return true;
        }

        @Override
        public ColumnRegionChar<Values> makeColumnRegionChar(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionChar<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionByte<Values> makeColumnRegionByte(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionByte<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionShort<Values> makeColumnRegionShort(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionShort<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionInt<Values> makeColumnRegionInt(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionInt<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter(columnDefinition));

        }

        @Override
        public ColumnRegionLong<Values> makeColumnRegionLong(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionLong<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter(columnDefinition));

        }

        @Override
        public ColumnRegionFloat<Values> makeColumnRegionFloat(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionFloat<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionDouble<Values> makeColumnRegionDouble(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionDouble<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(
                @NotNull final ColumnDefinition<TYPE> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionObject<>(REGION_MASK, PAGE_SIZE,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        private class TableServiceGetRangeAdapter implements AppendOnlyRegionAccessor<Values> {
            private final @NotNull ColumnDefinition<?> columnDefinition;

            public TableServiceGetRangeAdapter(@NotNull ColumnDefinition<?> columnDefinition) {
                this.columnDefinition = columnDefinition;
            }

            @Override
            public void readChunkPage(long firstRowPosition, int minimumSize,
                    @NotNull WritableChunk<Values> destination) {
                final TableLocationImpl location = (TableLocationImpl) getTableLocation();
                final TableKeyImpl key = (TableKeyImpl) location.getTableKey();

                final BarrageMessage msg = backend.getColumnValues(
                        key, (TableLocationKeyImpl) location.getKey(), columnDefinition, firstRowPosition, minimumSize);

                if (msg.length < minimumSize) {
                    throw new TableDataException(String.format("Not enough data returned. Read %d rows but minimum "
                            + "expected was %d. Short result from get_column_values(%s, %s, %s, %d, %d).",
                            msg.length, minimumSize, key.key, ((TableLocationKeyImpl) location.getKey()).locationKey,
                            columnDefinition.getName(), firstRowPosition, minimumSize));
                }

                int offset = 0;
                for (final Chunk<Values> rbChunk : msg.addColumnData[0].data) {
                    int length = Math.min(destination.capacity() - offset, rbChunk.size());
                    destination.copyFromChunk(rbChunk, 0, offset, length);
                    offset += length;
                    if (offset >= destination.capacity()) {
                        break;
                    }
                }
            }

            @Override
            public long size() {
                return getTableLocation().getSize();
            }
        }
    }

    private static class Subscription {
        SafeCloseable cancellationCallback;
    }
}
