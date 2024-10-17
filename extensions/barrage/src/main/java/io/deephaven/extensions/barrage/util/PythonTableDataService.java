//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.SortColumn;
import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
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
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.*;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.DefaultChunkReadingFactory;
import io.deephaven.generic.region.*;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jpy.PyObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static io.deephaven.extensions.barrage.chunk.ChunkReader.typeInfo;
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
     * Get a Deephaven {@link Table} for the supplied {@link TableKey}.
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
        public BarrageUtil.ConvertedArrowSchema[] getTableSchema(
                @NotNull final TableKeyImpl tableKey) {
            final BarrageUtil.ConvertedArrowSchema[] schemas = new BarrageUtil.ConvertedArrowSchema[2];
            final Consumer<ByteBuffer[]> onRawSchemas = byteBuffers -> {
                if (byteBuffers.length != 2) {
                    throw new IllegalArgumentException("Expected two Arrow IPC messages: found " + byteBuffers.length);
                }

                for (int ii = 0; ii < 2; ++ii) {
                    schemas[ii] = BarrageUtil.convertArrowSchema(ArrowToTableConverter.parseArrowSchema(
                            ArrowToTableConverter.parseArrowIpcMessage(byteBuffers[ii])));
                }
            };

            pyTableDataService.call("_table_schema", tableKey.key, onRawSchemas);

            return schemas;
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
                    (tableLocationKey, byteBuffers) -> processNewPartition(listener, tableLocationKey, byteBuffers);

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
                    (tableLocationKey, byteBuffers) -> processNewPartition(listener, tableLocationKey, byteBuffers);

            final PyObject cancellationCallback = pyTableDataService.call(
                    "_subscribe_to_new_partitions", tableKey.key, convertingListener);
            return () -> cancellationCallback.call("__call__");
        }

        private void processNewPartition(
                @NotNull final Consumer<TableLocationKeyImpl> listener,
                @NotNull final TableLocationKeyImpl tableLocationKey,
                @NotNull final ByteBuffer[] byteBuffers) {
            if (byteBuffers.length == 0) {
                listener.accept(tableLocationKey);
                return;
            }

            if (byteBuffers.length != 2) {
                throw new IllegalArgumentException("Expected Single Record Batch: found "
                        + byteBuffers.length);
            }

            final Map<String, Comparable<?>> partitionValues = new LinkedHashMap<>();
            final Schema schema = ArrowToTableConverter.parseArrowSchema(
                    ArrowToTableConverter.parseArrowIpcMessage(byteBuffers[0]));
            final BarrageUtil.ConvertedArrowSchema arrowSchema = BarrageUtil.convertArrowSchema(schema);

            final ArrayList<ChunkReader> readers = new ArrayList<>();
            final ChunkType[] columnChunkTypes = arrowSchema.computeWireChunkTypes();
            final Class<?>[] columnTypes = arrowSchema.computeWireTypes();
            final Class<?>[] componentTypes = arrowSchema.computeWireComponentTypes();
            for (int i = 0; i < columnTypes.length; i++) {
                final int factor = (arrowSchema.conversionFactors == null) ? 1 : arrowSchema.conversionFactors[i];
                ChunkReader reader = DefaultChunkReadingFactory.INSTANCE.getReader(
                        BarrageUtil.DEFAULT_SNAPSHOT_DESER_OPTIONS, factor,
                        typeInfo(columnChunkTypes[i], columnTypes[i], componentTypes[i], schema.fields(i)));
                readers.add(reader);
            }

            final BarrageProtoUtil.MessageInfo recordBatchMessageInfo = parseArrowIpcMessage(byteBuffers[1]);
            if (recordBatchMessageInfo.header.headerType() != MessageHeader.RecordBatch) {
                throw new IllegalArgumentException("byteBuffers[1] is not a valid Arrow RecordBatch IPC message");
            }
            final RecordBatch batch = (RecordBatch) recordBatchMessageInfo.header.header(new RecordBatch());

            final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                    new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                            i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

            final PrimitiveIterator.OfLong bufferInfoIter = ArrowToTableConverter.extractBufferInfo(batch);

            // populate the partition values
            for (int ci = 0; ci < schema.fieldsLength(); ++ci) {
                try (final WritableChunk<Values> columnValues = readers.get(ci).readChunk(
                        fieldNodeIter, bufferInfoIter, recordBatchMessageInfo.inputStream, null, 0, 0)) {

                    if (columnValues.size() != 1) {
                        throw new IllegalArgumentException("Expected Single Row: found " + columnValues.size());
                    }

                    // partition values are always boxed to make partition value comparisons easier
                    try (final ChunkBoxer.BoxerKernel boxer =
                            ChunkBoxer.getBoxer(columnValues.getChunkType(), columnValues.size())) {
                        // noinspection unchecked
                        final ObjectChunk<Comparable<?>, ? extends Values> boxedValues =
                                (ObjectChunk<Comparable<?>, ? extends Values>) boxer.box(columnValues);
                        partitionValues.put(schema.fields(ci).name(), boxedValues.get(0));
                    }
                } catch (final IOException unexpected) {
                    throw new UncheckedDeephavenException(unexpected);
                }
            }

            listener.accept(new TableLocationKeyImpl(tableLocationKey.locationKey, partitionValues));
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

            return () -> cancellationCallback.call("__call__");
        }

        /**
         * Get a range of data for a column.
         *
         * @param tableKey the table key
         * @param tableLocationKey the table location key
         * @param columnDefinition the column definition
         * @param firstRowPosition the first row position
         * @param minimumSize the minimum size
         * @return the column values
         */
        public List<WritableChunk<Values>> getColumnValues(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKeyImpl tableLocationKey,
                @NotNull final ColumnDefinition<?> columnDefinition,
                final long firstRowPosition,
                final int minimumSize,
                final int maximumSize) {

            final List<WritableChunk<Values>> resultChunks = new ArrayList<>();
            final Consumer<ByteBuffer[]> onMessages = messages -> {
                if (messages.length < 2) {
                    throw new IllegalArgumentException("Expected at least two Arrow IPC messages: found "
                            + messages.length);
                }

                final Schema schema = ArrowToTableConverter.parseArrowSchema(
                        ArrowToTableConverter.parseArrowIpcMessage(messages[0]));
                final BarrageUtil.ConvertedArrowSchema arrowSchema = BarrageUtil.convertArrowSchema(schema);

                if (schema.fieldsLength() > 1) {
                    throw new UnsupportedOperationException("More columns returned than requested.");
                }

                final int factor = (arrowSchema.conversionFactors == null) ? 1 : arrowSchema.conversionFactors[0];
                final ChunkReader reader = DefaultChunkReadingFactory.INSTANCE.getReader(
                        BarrageUtil.DEFAULT_SNAPSHOT_DESER_OPTIONS, factor,
                        typeInfo(arrowSchema.computeWireChunkTypes()[0],
                                arrowSchema.computeWireTypes()[0],
                                arrowSchema.computeWireComponentTypes()[0],
                                schema.fields(0)));

                try {
                    for (int ii = 1; ii < messages.length; ++ii) {
                        final BarrageProtoUtil.MessageInfo recordBatchMessageInfo = parseArrowIpcMessage(messages[ii]);
                        if (recordBatchMessageInfo.header.headerType() != MessageHeader.RecordBatch) {
                            throw new IllegalArgumentException(
                                    "byteBuffers[" + ii + "] is not a valid Arrow RecordBatch IPC message");
                        }
                        final RecordBatch batch = (RecordBatch) recordBatchMessageInfo.header.header(new RecordBatch());

                        final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                                new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                                        i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

                        final PrimitiveIterator.OfLong bufferInfoIter = ArrowToTableConverter.extractBufferInfo(batch);

                        resultChunks.add(reader.readChunk(
                                fieldNodeIter, bufferInfoIter, recordBatchMessageInfo.inputStream, null, 0, 0));
                    }
                } catch (final IOException unexpected) {
                    SafeCloseable.closeAll(resultChunks.iterator());
                    throw new UncheckedDeephavenException(unexpected);
                }
            };

            pyTableDataService.call("_column_values",
                    tableKey.key, tableLocationKey.locationKey, columnDefinition.getName(), firstRowPosition,
                    minimumSize, maximumSize, onMessages);

            return resultChunks;
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
            // TODO NOCOMMIT @ryan: PyObject's hash is based on pointer location of object which would change if
            // two different Python objects have the same value.
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
            final BarrageUtil.ConvertedArrowSchema[] schemas = backend.getTableSchema(tableKey);

            final TableDefinition tableDef = schemas[0].tableDef;
            final TableDefinition partitionDef = schemas[1].tableDef;
            final Map<String, ColumnDefinition<?>> columns = new LinkedHashMap<>(tableDef.numColumns());

            for (final ColumnDefinition<?> column : tableDef.getColumns()) {
                columns.put(column.getName(), column);
            }

            for (final ColumnDefinition<?> column : partitionDef.getColumns()) {
                final ColumnDefinition<?> existingDef = columns.get(column.getName());
                // validate that both definitions are the same
                if (existingDef != null && !existingDef.equals(column)) {
                    throw new IllegalArgumentException(String.format(
                            "Column %s has conflicting definitions in table and partition schemas: %s vs %s",
                            column.getName(), existingDef, column));
                }

                columns.put(column.getName(), column.withPartitioning());
            }

            tableDefinition = TableDefinition.of(columns.values());
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
            // TODO NOCOMMIT @ryan: should we let the python table service impl activate so that they may invoke the
            // callback immediately?
            localSubscription.cancellationCallback = backend.subscribeToNewPartitions(key, tableLocationKey -> {
                if (localSubscription != subscription) {
                    // we've been cancelled and/or replaced
                    return;
                }

                handleTableLocationKey(tableLocationKey);
            });
            activationSuccessful(localSubscription);
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

        /**
         * Construct a TableLocationKeyImpl. Used by the Python adapter.
         *
         * @param locationKey the location key
         */
        @ScriptApi
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
            // TODO NOCOMMIT @ryan: PyObject's hash is based on pointer location of object which would change if
            // two different Python objects have the same value.
            return locationKey.hashCode();
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

        private synchronized void checkSizeChange(final long newSize) {
            // TODO NOCOMMIT @ryan: should we throw if python tells us size decreased? or just ignore smaller sizes?
            if (size >= newSize) {
                return;
            }

            size = newSize;
            handleUpdate(RowSetFactory.flat(size), System.currentTimeMillis());
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
            activationSuccessful(localSubscription);
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
            public void readChunkPage(
                    final long firstRowPosition,
                    final int minimumSize,
                    @NotNull final WritableChunk<Values> destination) {
                final TableLocationImpl location = (TableLocationImpl) getTableLocation();
                final TableKeyImpl key = (TableKeyImpl) location.getTableKey();

                final List<WritableChunk<Values>> values = backend.getColumnValues(
                        key, (TableLocationKeyImpl) location.getKey(), columnDefinition,
                        firstRowPosition, minimumSize, destination.capacity());

                final int numRows = values.stream().mapToInt(WritableChunk::size).sum();

                if (numRows < minimumSize) {
                    throw new TableDataException(String.format("Not enough data returned. Read %d rows but minimum "
                            + "expected was %d. Result from get_column_values(%s, %s, %s, %d, %d).",
                            numRows, minimumSize, key.key, ((TableLocationKeyImpl) location.getKey()).locationKey,
                            columnDefinition.getName(), firstRowPosition, minimumSize));
                }
                if (numRows > destination.capacity()) {
                    throw new TableDataException(String.format("Too much data returned. Read %d rows but maximum "
                            + "expected was %d. Result from get_column_values(%s, %s, %s, %d, %d).",
                            numRows, destination.capacity(), key.key,
                            ((TableLocationKeyImpl) location.getKey()).locationKey, columnDefinition.getName(),
                            firstRowPosition, minimumSize));
                }

                int offset = 0;
                for (final Chunk<Values> rbChunk : values) {
                    int length = Math.min(destination.capacity() - offset, rbChunk.size());
                    destination.copyFromChunk(rbChunk, 0, offset, length);
                    offset += length;
                }
                destination.setSize(offset);
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
