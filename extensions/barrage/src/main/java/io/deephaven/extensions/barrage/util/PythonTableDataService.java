package io.deephaven.extensions.barrage.util;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.*;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.generic.region.*;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jpy.PyObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

@ScriptApi
public class PythonTableDataService extends AbstractTableDataService {

    public static final ColumnName PARTITION_COLUMN_NAME = ColumnName.of("Partition");
    public static final ColumnDefinition<String> PARTITION_COLUMN_DEFINITION = ColumnDefinition.ofString(
                    PARTITION_COLUMN_NAME.name())
            .withPartitioning();

    private static final int PAGE_SIZE = Configuration.getInstance()
            .getIntegerForClassWithDefault(PythonTableDataService.class, "PAGE_SIZE", 1 << 16);
    private static final long REGION_MASK = RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK;

    private final PartitionedTableDataServiceBackend backend;
    private final PyObject pyTableDataService;

    /**
     * Construct a Deephaven {@link io.deephaven.engine.table.impl.locations.TableDataService TableDataService} wrapping
     * the provided {@link PartitionedTableDataServiceBackend}.
     */
    private PythonTableDataService(@NotNull final PyObject pyTableDataService) {
        super("PythonTableDataService");
        this.pyTableDataService = pyTableDataService;
        this.backend = new PartitionedTableDataServiceBackendImpl();
    }

    @ScriptApi
    public static PythonTableDataService create(@NotNull final PyObject pyTableDataService) {
        return new PythonTableDataService(pyTableDataService);
    }

    public static class PartitionIdentification {
        TableLocationKeyImpl locationKey;
        BarrageMessage partitionColumnValues;
    }

    public interface PartitionedTableDataServiceBackend {

        BarrageUtil.ConvertedArrowSchema getTableSchema(TableKeyImpl tableKey); // convert Arrow schema to ConvertedArrowSchema
        void getExistingPartitions(TableKeyImpl tableKey, Consumer<PartitionIdentification> listener);

        SafeCloseable subscribeToNewPartitions(TableKeyImpl tableKey, Consumer<PartitionIdentification> listener);

        void getPartitionSize(TableKeyImpl tableKey, TableLocationKeyImpl tableLocationKey, LongConsumer listener);

        SafeCloseable subscribeToPartitionSizeChanges(TableKeyImpl tableKey, TableLocationKeyImpl tableLocationKey, LongConsumer listener);

        BarrageMessage getColumnValues(TableKeyImpl tableKey, TableLocationKeyImpl tableLocationKey, ColumnDefinition<?> columnDefinition, long firstRowPosition, int minimumSize);

    }

    private class PartitionedTableDataServiceBackendImpl implements PartitionedTableDataServiceBackend {

        @Override
        public BarrageUtil.ConvertedArrowSchema getTableSchema(TableKeyImpl tableKey) {
            pyTableDataService.call("table_schema", tableKey.key);
            return null;
        }

        @Override
        public void getExistingPartitions(TableKeyImpl tableKey, Consumer<PartitionIdentification> listener) {
        }

        @Override
        public SafeCloseable subscribeToNewPartitions(TableKeyImpl tableKey, Consumer<PartitionIdentification> listener) {
            return null;
        }

        @Override
        public void getPartitionSize(TableKeyImpl tableKey, TableLocationKeyImpl tableLocationKey, LongConsumer listener) {
            listener.accept(0L);
        }

        @Override
        public SafeCloseable subscribeToPartitionSizeChanges(TableKeyImpl tableKey, TableLocationKeyImpl tableLocationKey, LongConsumer listener) {
            return null;
        }

        @Override
        public BarrageMessage getColumnValues(TableKeyImpl tableKey, TableLocationKeyImpl tableLocationKey, ColumnDefinition<?> columnDefinition, long firstRowPosition, int minimumSize) {
            return null;
        }
    }

//    @Override
//    public String getImplementationName() {
//        return "PythonTableDataService";
//    }

    /**
     * Get a Deephaven {@link Table} for the supplied name.
     *
     * @param tableKey The table key
     * @param live      Whether the table should update as new data becomes available
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
            return logOutput.append("TableService.TableKey[name=")
                    .append(key.toString())
                    .append(']');
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public String getImplementationName() {
            return "PartitionedTableDataService.TableKey";
        }
    }

    private static final AtomicReferenceFieldUpdater<TableLocationProviderImpl, Subscription> TABLE_LOC_PROVIDER_SUBSCRIPTION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(TableLocationProviderImpl.class, Subscription.class, "subscription");

    /**
     * {@link TableLocationProvider} implementation for TableService.
     */
    private class TableLocationProviderImpl extends AbstractTableLocationProvider {

        private final TableDefinition tableDefinition;

        volatile Subscription<PyObject> subscription = null;

        private TableLocationProviderImpl(@NotNull final TableKeyImpl tableKey) {
            super(tableKey, true);
            final TableDefinition rawDefinition = backend.getTableSchema(tableKey).tableDef;
            final List<ColumnDefinition<?>> columns = new ArrayList<>(rawDefinition.numColumns() + 1);
            columns.addAll(rawDefinition.getColumns());
            columns.add(PARTITION_COLUMN_DEFINITION);
            tableDefinition = TableDefinition.of(columns);
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
            // call handleTableLocationKey for all partitions
//            TableKeyImpl key = (TableKeyImpl) getKey();
//            backend.listPartitions(key.tableName, partition -> {
//                handleTableLocationKey(new TableLocationKeyImpl(partition));
//            });
        }

        @Override
        protected void activateUnderlyingDataSource() {
            TableKeyImpl key = (TableKeyImpl) getKey();
            final Subscription<PyObject> localSubscription = new Subscription<>() {
                @Override
                public void onNext(PyObject partition) {
                    if (subscription != this) {
                        // we've been cancelled and/or replaced
                        return;
                    }

                    if (partition.equals("SubscribeAcknowledgment")) {
                        refresh();
                        activationSuccessful(this);
                        return;
                    }

                    handleTableLocationKey(new TableLocationKeyImpl(partition));
                }

                @Override
                protected void onFailure(@Nullable Throwable t) {
                    activationFailed(this, new TableDataException(getImplementationName() + ": new partitions "
                            + "subscription to table " + getKey() + " failed", t));
                    TABLE_LOC_PROVIDER_SUBSCRIPTION_UPDATER.compareAndSet(TableLocationProviderImpl.this, this, null);
                }
            };
            subscription = localSubscription;
//            localSubscription.setGrpcSubscription(backend.subscribeToNewPartitions(key.tableName, localSubscription));
        }

        @Override
        protected void deactivateUnderlyingDataSource() {
            final Subscription<PyObject> localSubscription = subscription;
//            if (localSubscription != null
//                    && TABLE_LOC_PROVIDER_SUBSCRIPTION_UPDATER.compareAndSet(this, localSubscription, null)) {
//                localSubscription.cancel();
//            }
        }

        @Override
        protected <T> boolean matchSubscriptionToken(final T token) {
            return token == subscription;
        }

        @Override
        public String getImplementationName() {
            return "TableService.TableLocationProvider";
        }
    }

    /**
     * {@link TableLocationKey} implementation for TableService.
     */
    public static class TableLocationKeyImpl extends PartitionedTableLocationKey {

        private int cachedHashCode;

        /**
         * @param partition The partition ID
         */
        private TableLocationKeyImpl(@NotNull final PyObject partition) {
//            super(Map.of(PARTITION_COLUMN_NAME.name(), partition));
            super(Map.of()); // TODO: The Deephaven table has the partition column values from the callback in getExistingPartitions()
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
            return partitions.equals(otherTableLocationKey.partitions);
        }

        @Override
        public int hashCode() {
            if (cachedHashCode == 0) {
                final int computedHashCode = partitions.hashCode();
                // Don't use 0; that's used by StandaloneTableLocationKey, and also our sentinel for the need to compute
                if (computedHashCode == 0) {
                    final int fallbackHashCode = TableLocationKeyImpl.class.hashCode();
                    cachedHashCode = fallbackHashCode == 0 ? 1 : fallbackHashCode;
                } else {
                    cachedHashCode = computedHashCode;
                }
            }
            return cachedHashCode;
        }

        @Override
        public int compareTo(@NotNull final TableLocationKey other) {
            if (getClass() != other.getClass()) {
                throw new ClassCastException(String.format("Cannot compare %s to %s", getClass(), other.getClass()));
            }
            final TableLocationKeyImpl otherTableLocationKey = (TableLocationKeyImpl) other;
            return PartitionsComparator.INSTANCE.compare(partitions, otherTableLocationKey.partitions);
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append("PythonTableDataService.TableLocationKey[partitions=")
                    .append(PartitionsFormatter.INSTANCE, partitions)
                    .append(']');
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public String getImplementationName() {
            return "PythonTableDataService.TableLocationKey";
        }
    }

    private static final AtomicReferenceFieldUpdater<TableLocationImpl, Subscription> TABLE_LOC_SUBSCRIPTION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(TableLocationImpl.class, Subscription.class, "subscription");

    /**
     * {@link TableLocation} implementation for TableService.
     */
    public class TableLocationImpl extends AbstractTableLocation {

        private String executorUri;
        volatile Subscription<Long> subscription = null;


        private long size;

        private TableLocationImpl(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKeyImpl locationKey) {
            super(tableKey, locationKey, true);
        }

        private synchronized String getExecutorUri() {
//            if (executorUri == null) {
//                final Flight.FlightEndpoint endpoint =
//                        backend.getPartitionExecutor(((TableKeyImpl)getTableKey()).tableName, getKey().getPartitionValue(PARTITION_COLUMN_NAME.name()));
//                if (endpoint == null || endpoint.getLocationCount() == 0) {
//                    executorUri = null;
//                } else {
//                    executorUri = endpoint.getLocation(0).getUri();
//                }
//            }
            return executorUri;
        }

        private void checkSizeChange(final long newSize) {
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
//            final TableKeyImpl key = (TableKeyImpl) getTableKey();
//            final TableLocationKeyImpl location = (TableLocationKeyImpl) getKey();
//            final String partitionId = location.getPartitionValue(PARTITION_COLUMN_NAME.name());
//
//            checkSizeChange(backend.getPartitionSize(key.tableName, partitionId, getExecutorUri()));
        }

        public void asyncRefresh(
                final Runnable onSuccess,
                final Runnable onFailure) {
            final TableKeyImpl key = (TableKeyImpl) getTableKey();
            final TableLocationKeyImpl location = (TableLocationKeyImpl) getKey();
            final String partitionId = location.getPartitionValue(PARTITION_COLUMN_NAME.name());

//            backend.getPartitionSize(key.tableName, partitionId, getExecutorUri(), currSize -> {
//                checkSizeChange(currSize);
//                onSuccess.run();
//            }, onFailure);
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
            TableKeyImpl key = (TableKeyImpl) getTableKey();
            TableLocationKeyImpl location = (TableLocationKeyImpl) getKey();
            final String partitionId = location.getPartitionValue(PARTITION_COLUMN_NAME.name());
            final Subscription<Long> localSubscription = new Subscription<>() {
                @Override
                public void onNext(Long newSize) {
                    if (subscription != this) {
                        // we've been cancelled and/or replaced
                        return;
                    }

                    // the server does not send its current size once subscribed, leading to potential race conditions
                    // regarding the size of the partition. If the table service is correct and these are append only,
                    // then we can ignore size decreases and duplicate updates.
                    checkSizeChange(newSize);
                }

                @Override
                protected void onFailure(@Nullable Throwable t) {
                    activationFailed(this, new TableDataException(String.format(
                            "%s: new rows subscription to (table %s, partition %s) failed",
                            getImplementationName(), getKey(), partitionId), t));
                    TABLE_LOC_SUBSCRIPTION_UPDATER.compareAndSet(TableLocationImpl.this, this, null);
                }
            };

            subscription = localSubscription;
//            localSubscription.setGrpcSubscription(backend.subscribeToNewRows(
//                    key.tableName,
//                    partitionId,
//                    localSubscription, getExecutorUri()));

            // Note at this time that the NewRows subscription does not send an acknowledgement which leads to a race
            // condition where the size may be incorrect if an update is missed. This is a known issue and will be fixed
            // in the future. For now, assume that we receive this `ack` immediately.
            asyncRefresh(() -> {
                activationSuccessful(localSubscription);
            }, () -> {
                activationFailed(localSubscription, new TableDataException(String.format(
                        "%s: new rows async refresh of (table %s, partition %s) failed",
                        getImplementationName(), getKey(), partitionId)));
//                if (TABLE_LOC_SUBSCRIPTION_UPDATER.compareAndSet(this, localSubscription, null)) {
//                    localSubscription.cancel();
//                }
            });
        }

        @Override
        protected void deactivateUnderlyingDataSource() {
            final Subscription<Long> localSubscription = subscription;
//            if (localSubscription != null
//                    && TABLE_LOC_SUBSCRIPTION_UPDATER.compareAndSet(this, localSubscription, null)) {
//                localSubscription.cancel();
            }
        }

        @Override
        protected <T> boolean matchSubscriptionToken(final T token) {
            return token == subscription;
        }

        @Override
        public String getImplementationName() {
            return "TableService.TableLocation";
        }
    }

    public static abstract class Subscription<R> implements StreamObserver<R> {
//        private boolean alreadyCancelled = false;
//        private StreamObserver<Flight.FlightData> grpcSubscription;
//
//        public synchronized void setGrpcSubscription(StreamObserver<Flight.FlightData> grpcSubscription) {
//            if (alreadyCancelled) {
//                grpcSubscription.onCompleted();
//            } else {
//                this.grpcSubscription = grpcSubscription;
//            }
//        }
//
//        public synchronized void cancel() {
//            alreadyCancelled = true;
//            if (grpcSubscription != null) {
//                grpcSubscription.onCompleted();
//                grpcSubscription = null;
//            }
//        }

        @Override
        public void onError(Throwable t) {
            doRetry(t);
        }

        @Override
        public void onCompleted() {
            doRetry(null);
        }

        protected synchronized void doRetry(Throwable t) {
            // TODO: retry a few times before giving up
            onFailure(t);
        }

        protected abstract void onFailure(@Nullable Throwable t);
    }

    /**
     * {@link ColumnLocation} implementation for TableService.
     */
    public class ColumnLocationImpl extends AbstractColumnLocation {

        protected ColumnLocationImpl(@NotNull final PythonTableDataService.TableLocationImpl tableLocation, @NotNull final String name) {
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
            return new AppendOnlyFixedSizePageRegionChar<>(REGION_MASK, PAGE_SIZE, new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionByte<Values> makeColumnRegionByte(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionByte<>(REGION_MASK, PAGE_SIZE, new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionShort<Values> makeColumnRegionShort(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionShort<>(REGION_MASK, PAGE_SIZE, new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionInt<Values> makeColumnRegionInt(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionInt<>(REGION_MASK, PAGE_SIZE, new TableServiceGetRangeAdapter(columnDefinition));

        }

        @Override
        public ColumnRegionLong<Values> makeColumnRegionLong(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionLong<>(REGION_MASK, PAGE_SIZE, new TableServiceGetRangeAdapter(columnDefinition));

        }

        @Override
        public ColumnRegionFloat<Values> makeColumnRegionFloat(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionFloat<>(REGION_MASK, PAGE_SIZE, new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionDouble<Values> makeColumnRegionDouble(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionDouble<>(REGION_MASK, PAGE_SIZE, new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(
                @NotNull final ColumnDefinition<TYPE> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionObject<>(REGION_MASK, PAGE_SIZE, new TableServiceGetRangeAdapter(columnDefinition));
        }

        private class TableServiceGetRangeAdapter implements AppendOnlyRegionAccessor<Values> {
            private final @NotNull ColumnDefinition<?> columnDefinition;

            public TableServiceGetRangeAdapter(@NotNull ColumnDefinition<?> columnDefinition) {
                this.columnDefinition = columnDefinition;
            }

            @Override
            public void readChunkPage(long firstRowPosition, int minimumSize, @NotNull WritableChunk<Values> destination) {
                TableLocationImpl location = (TableLocationImpl) getTableLocation();

                String tableName = ((TableKeyImpl) location.getTableKey()).toString();
                String partitionId = location.getKey().getPartitionValue(PARTITION_COLUMN_NAME.name());

                // TODO: we could send a hint to the server to return more data than requested to avoid having to make
                // multiple calls to getDataRange. The maximum data that can be read is
                // `destination.capacity() - destination.size()`.
                final int numRowsRead = 0;
//                final int numRowsRead = backend.getDataRange(
//                        tableName,
//                        partitionId,
//                        location.getExecutorUri(),
//                        columnDefinition,
//                        firstRowPosition,
//                        minimumSize,
//                        destination);

                if (numRowsRead < minimumSize) {
                    throw new TableDataException("Not enough data returned. Read " + numRowsRead
                            + " rows but minimum expected was " + minimumSize + " from getDataRange(" + tableName + ", "
                            + partitionId + ", " + location.getExecutorUri() + ", " + columnDefinition.getName() + ", "
                            + firstRowPosition + ", " + minimumSize + ", destination)");
                }
            }

            @Override
            public long size() {
                return getTableLocation().getSize();
            }
        }
    }

}
