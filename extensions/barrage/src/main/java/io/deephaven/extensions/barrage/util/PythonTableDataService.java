//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.UncheckedDeephavenException;
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
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.TableUpdateMode;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.IntStream;

import static io.deephaven.extensions.barrage.util.ArrowToTableConverter.parseArrowIpcMessage;

@ScriptApi
public class PythonTableDataService extends AbstractTableDataService {

    private static final int DEFAULT_PAGE_SIZE = Configuration.getInstance()
            .getIntegerForClassWithDefault(PythonTableDataService.class, "defaultPageSize", 1 << 16);
    private static final long REGION_MASK = RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK;

    private final BackendAccessor backend;
    private final ChunkReader.Factory chunkReaderFactory;
    private final StreamReaderOptions streamReaderOptions;
    private final int pageSize;

    @ScriptApi
    public static PythonTableDataService create(
            @NotNull final PyObject pyTableDataService,
            @Nullable final ChunkReader.Factory chunkReaderFactory,
            @Nullable final StreamReaderOptions streamReaderOptions,
            final int pageSize) {
        return new PythonTableDataService(
                pyTableDataService,
                chunkReaderFactory == null ? DefaultChunkReadingFactory.INSTANCE : chunkReaderFactory,
                streamReaderOptions == null ? BarrageUtil.DEFAULT_SNAPSHOT_DESER_OPTIONS : streamReaderOptions,
                pageSize <= 0 ? DEFAULT_PAGE_SIZE : pageSize);
    }

    /**
     * Construct a Deephaven {@link io.deephaven.engine.table.impl.locations.TableDataService TableDataService} wrapping
     * the provided Python TableDataServiceBackend.
     *
     * @param pyTableDataService The Python TableDataService
     * @param pageSize The page size to use for all regions
     */
    private PythonTableDataService(
            @NotNull final PyObject pyTableDataService,
            @NotNull final ChunkReader.Factory chunkReaderFactory,
            @NotNull final StreamReaderOptions streamReaderOptions,
            final int pageSize) {
        super("PythonTableDataService");
        this.backend = new BackendAccessor(pyTableDataService);
        this.chunkReaderFactory = chunkReaderFactory;
        this.streamReaderOptions = streamReaderOptions;
        this.pageSize = pageSize <= 0 ? DEFAULT_PAGE_SIZE : pageSize;
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
    private class BackendAccessor {
        private final PyObject pyTableDataService;

        private BackendAccessor(
                @NotNull final PyObject pyTableDataService) {
            this.pyTableDataService = pyTableDataService;
        }

        /**
         * Get two schemas, the first for partitioning columns whose values will be derived from TableLocationKey and
         * applied to all rows in the associated TableLocation, and the second specifying the table data to be read
         * chunk-wise (in columnar fashion) from the TableLocations.
         *
         * @param tableKey the table key
         * @return the schemas
         */
        public BarrageUtil.ConvertedArrowSchema[] getTableSchema(
                @NotNull final TableKeyImpl tableKey) {
            final AsyncState<BarrageUtil.ConvertedArrowSchema[]> asyncState = new AsyncState<>();

            final Consumer<ByteBuffer[]> onRawSchemas = byteBuffers -> {
                final BarrageUtil.ConvertedArrowSchema[] schemas = new BarrageUtil.ConvertedArrowSchema[2];

                if (byteBuffers.length != schemas.length) {
                    asyncState.setError(new IllegalArgumentException(String.format(
                            "Provided too many IPC messages. Expected %d, received %d.",
                            schemas.length, byteBuffers.length)));
                    return;
                }

                for (int ii = 0; ii < schemas.length; ++ii) {
                    try {
                        schemas[ii] = BarrageUtil.convertArrowSchema(
                                ArrowToTableConverter.parseArrowSchema(
                                        ArrowToTableConverter.parseArrowIpcMessage(
                                                byteBuffers[ii])));
                    } catch (final Exception e) {
                        final String schemaType = ii % 2 == 0 ? "data table" : "partitioning column";
                        asyncState.setError(new IllegalArgumentException(String.format(
                                "Failed to parse %s schema message", schemaType), e));
                        return;
                    }
                }

                asyncState.setResult(schemas);
            };

            final Consumer<String> onFailure = errorString -> asyncState.setError(
                    new UncheckedDeephavenException(errorString));

            pyTableDataService.call("_table_schema", tableKey.key, onRawSchemas, onFailure);

            return asyncState.awaitResult(err -> new TableDataException(String.format(
                    "%s: table_schema failed", tableKey), err));
        }

        /**
         * Get the existing table locations for the provided {@code tableKey}.
         *
         * @param definition the table definition to validate partitioning columns against
         * @param tableKey the table key
         * @param listener the listener to call with each existing table location key
         */
        public void getTableLocations(
                @NotNull final TableDefinition definition,
                @NotNull final TableKeyImpl tableKey,
                @NotNull final Consumer<TableLocationKeyImpl> listener) {
            final AsyncState<Boolean> asyncState = new AsyncState<>();

            final BiConsumer<TableLocationKeyImpl, ByteBuffer[]> convertingListener =
                    (tableLocationKey, byteBuffers) -> {
                        try {
                            processTableLocationKey(definition, tableKey, listener, tableLocationKey, byteBuffers);
                        } catch (final RuntimeException e) {
                            asyncState.setError(e);
                        }
                    };

            final Runnable onSuccess = () -> asyncState.setResult(true);
            final Consumer<String> onFailure = errorString -> asyncState.setError(
                    new UncheckedDeephavenException(errorString));

            pyTableDataService.call("_table_locations", tableKey.key, convertingListener, onSuccess, onFailure);
            asyncState.awaitResult(err -> new TableDataException(String.format(
                    "%s: table_locations failed", tableKey), err));
        }

        /**
         * Subscribe to table location updates for the provided {@code tableKey}.
         * <p>
         * The {@code tableLocationListener} should be invoked with all existing table locations. Any asynchronous calls
         * to {@code tableLocationListener}, {@code successCallback}, or {@code failureCallback} will block until this
         * method has completed.
         *
         * @param definition the table definition to validate partitioning columns against
         * @param tableKey the table key
         * @param tableLocationListener the tableLocationListener to call with each table location key
         * @param successCallback the success callback; called when the subscription is successfully established and the
         *        tableLocationListener has been called with all existing table locations
         * @param failureCallback the failure callback; called to deliver an exception triggered while activating or
         *        maintaining the underlying data source
         * @return a {@link SafeCloseable} that can be used to cancel the subscription
         */
        public SafeCloseable subscribeToTableLocations(
                @NotNull final TableDefinition definition,
                @NotNull final TableKeyImpl tableKey,
                @NotNull final Consumer<TableLocationKeyImpl> tableLocationListener,
                @NotNull final Runnable successCallback,
                @NotNull final Consumer<RuntimeException> failureCallback) {
            final AtomicBoolean locationProcessingFailed = new AtomicBoolean();
            final AtomicReference<SafeCloseable> cancellationCallbackRef = new AtomicReference<>();

            final BiConsumer<TableLocationKeyImpl, ByteBuffer[]> convertingListener =
                    (tableLocationKey, byteBuffers) -> {
                        if (locationProcessingFailed.get()) {
                            return;
                        }
                        try {
                            processTableLocationKey(
                                    definition, tableKey, tableLocationListener, tableLocationKey, byteBuffers);
                        } catch (final RuntimeException e) {
                            failureCallback.accept(e);
                            // we must also cancel the python subscription
                            locationProcessingFailed.set(true);
                            final SafeCloseable localCancellationCallback = cancellationCallbackRef.get();
                            if (localCancellationCallback != null) {
                                localCancellationCallback.close();
                            }
                        }
                    };

            final Consumer<String> onFailure = errorString -> failureCallback.accept(
                    new UncheckedDeephavenException(errorString));

            final PyObject cancellationCallback = pyTableDataService.call(
                    "_subscribe_to_table_locations", tableKey.key,
                    convertingListener, successCallback, onFailure);
            final SafeCloseable cancellationCallbackOnce = new SafeCloseable() {

                private final AtomicBoolean invoked = new AtomicBoolean();

                @Override
                public void close() {
                    if (invoked.compareAndSet(false, true)) {
                        cancellationCallback.call("__call__");
                        cancellationCallbackRef.set(null);
                    }
                }
            };
            cancellationCallbackRef.set(cancellationCallbackOnce);
            if (locationProcessingFailed.get()) {
                cancellationCallbackOnce.close();
            }
            return cancellationCallbackOnce;
        }

        private void processTableLocationKey(
                @NotNull final TableDefinition definition,
                @NotNull final TableKeyImpl tableKey,
                @NotNull final Consumer<TableLocationKeyImpl> listener,
                @NotNull final TableLocationKeyImpl tableLocationKey,
                @NotNull final ByteBuffer[] byteBuffers) {
            if (byteBuffers.length == 0) {
                if (!definition.getPartitioningColumns().isEmpty()) {
                    throw new IllegalArgumentException(String.format("%s:%s: table_location_key callback expected "
                            + "partitioned column values but none were provided", tableKey, tableLocationKey));
                }
                listener.accept(tableLocationKey);
                return;
            }

            if (byteBuffers.length != 2) {
                throw new IllegalArgumentException(String.format("%s:%s: table_location_key callback expected 2 IPC "
                        + "messages describing the wire format of the partitioning columns followed by partitioning "
                        + "values, but received %d messages", tableKey, tableLocationKey, byteBuffers.length));
            }

            // partitioning values must be in the same order as the partitioning keys, so we'll prepare an ordered map
            // with null values for each key so that we may fill them in out of order
            final Map<String, Comparable<?>> partitioningValues = new LinkedHashMap<>(
                    definition.getPartitioningColumns().size());
            definition.getPartitioningColumns().forEach(column -> partitioningValues.put(column.getName(), null));

            // note that we recompute chunk readers for each location to support some schema evolution
            final Schema partitioningValuesSchema = ArrowToTableConverter.parseArrowSchema(
                    ArrowToTableConverter.parseArrowIpcMessage(byteBuffers[0]));
            final BarrageUtil.ConvertedArrowSchema schemaPlus =
                    BarrageUtil.convertArrowSchema(partitioningValuesSchema);

            try {
                definition.checkCompatibility(schemaPlus.tableDef);
            } catch (TableDefinition.IncompatibleTableDefinitionException err) {
                throw new IllegalArgumentException(String.format("%s:%s: table_location_key callback received "
                        + "partitioning schema that is incompatible with table definition", tableKey, tableLocationKey),
                        err);
            }

            final ChunkReader[] readers = schemaPlus.computeChunkReaders(
                    chunkReaderFactory,
                    partitioningValuesSchema,
                    streamReaderOptions);

            final BarrageProtoUtil.MessageInfo recordBatchMessageInfo = parseArrowIpcMessage(byteBuffers[1]);
            if (recordBatchMessageInfo.header.headerType() != MessageHeader.RecordBatch) {
                throw new IllegalArgumentException(String.format("%s:%s: table_location_key callback received 2nd IPC "
                        + "message that is not a valid Arrow RecordBatch", tableKey, tableLocationKey));
            }
            final RecordBatch batch = (RecordBatch) recordBatchMessageInfo.header.header(new RecordBatch());

            final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                    new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                            i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

            final PrimitiveIterator.OfLong bufferInfoIter = ArrowToTableConverter.extractBufferInfo(batch);

            // extract partitioning values and box them to be used as Comparable in the map
            for (int ci = 0; ci < partitioningValuesSchema.fieldsLength(); ++ci) {
                final String columnName = partitioningValuesSchema.fields(ci).name();
                try (final WritableChunk<Values> columnValues = readers[ci].readChunk(
                        fieldNodeIter, bufferInfoIter, recordBatchMessageInfo.inputStream, null, 0, 0)) {

                    if (columnValues.size() != 1) {
                        throw new IllegalArgumentException(String.format("%s:%s: table_location_key callback received "
                                + "%d rows for partitioning column %s; expected 1", tableKey, tableLocationKey,
                                columnValues.size(), columnName));
                    }

                    partitioningValues.put(columnName, ChunkBoxer.boxedGet(columnValues, 0));
                } catch (final IOException ioe) {
                    throw new UncheckedDeephavenException(String.format(
                            "%s:%s: table_location_key callback failed to read partitioning column %s", tableKey,
                            tableLocationKey, columnName), ioe);
                }
            }

            listener.accept(new TableLocationKeyImpl(tableLocationKey.locationKey, partitioningValues));
        }

        /**
         * Get the size of the given {@code tableLocationKey}.
         *
         * @param tableKey the table key
         * @param tableLocationKey the table location key
         * @param listener the listener to call with the table location size
         */
        public void getTableLocationSize(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKeyImpl tableLocationKey,
                @NotNull final LongConsumer listener) {
            final AsyncState<Long> asyncState = new AsyncState<>();

            final LongConsumer onSize = asyncState::setResult;
            final Consumer<String> onFailure = errorString -> asyncState.setError(
                    new UncheckedDeephavenException(errorString));

            pyTableDataService.call("_table_location_size", tableKey.key, tableLocationKey.locationKey,
                    onSize, onFailure);

            listener.accept(asyncState.awaitResult(err -> new TableDataException(String.format(
                    "%s:%s: table_location_size failed", tableKey, tableLocationKey), err)));
        }

        /**
         * Subscribe to the existing size and future size changes of a table location.
         *
         * @param tableKey the table key
         * @param tableLocationKey the table location key
         * @param sizeListener the sizeListener to call with the partition size
         * @param successCallback the success callback; called when the subscription is successfully established and the
         *        sizeListener has been called with the initial size
         * @param failureCallback the failure callback; called to deliver an exception triggered while activating or
         *        maintaining the underlying data source
         * @return a {@link SafeCloseable} that can be used to cancel the subscription
         */
        public SafeCloseable subscribeToTableLocationSize(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKeyImpl tableLocationKey,
                @NotNull final LongConsumer sizeListener,
                @NotNull final Runnable successCallback,
                @NotNull final Consumer<String> failureCallback) {

            final PyObject cancellationCallback = pyTableDataService.call(
                    "_subscribe_to_table_location_size", tableKey.key, tableLocationKey.locationKey,
                    sizeListener, successCallback, failureCallback);

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
         * @param maximumSize the maximum size
         * @return the column values
         */
        public List<WritableChunk<Values>> getColumnValues(
                @NotNull final TableKeyImpl tableKey,
                @NotNull final TableLocationKeyImpl tableLocationKey,
                @NotNull final ColumnDefinition<?> columnDefinition,
                final long firstRowPosition,
                final int minimumSize,
                final int maximumSize) {

            final AsyncState<List<WritableChunk<Values>>> asyncState = new AsyncState<>();

            final String columnName = columnDefinition.getName();
            final Consumer<ByteBuffer[]> onMessages = messages -> {
                if (messages.length < 2) {
                    asyncState.setError(new IllegalArgumentException(String.format(
                            "expected at least 2 IPC messages describing the wire format of the column followed by "
                                    + "column values, but received %d messages",
                            messages.length)));
                    return;
                }
                final Schema schema = ArrowToTableConverter.parseArrowSchema(
                        ArrowToTableConverter.parseArrowIpcMessage(messages[0]));
                final BarrageUtil.ConvertedArrowSchema schemaPlus = BarrageUtil.convertArrowSchema(schema);

                if (schema.fieldsLength() > 1) {
                    asyncState.setError(new IllegalArgumentException(String.format(
                            "Received more than one field. Received %d fields for columns %s.",
                            schema.fieldsLength(),
                            IntStream.range(0, schema.fieldsLength())
                                    .mapToObj(ci -> schema.fields(ci).name())
                                    .reduce((a, b) -> a + ", " + b).orElse(""))));
                    return;
                }
                if (!columnDefinition.isCompatible(schemaPlus.tableDef.getColumns().get(0))) {
                    asyncState.setError(new IllegalArgumentException(String.format(
                            "Received incompatible column definition. Expected %s, but received %s.",
                            columnDefinition, schemaPlus.tableDef.getColumns().get(0))));
                    return;
                }

                final ArrayList<WritableChunk<Values>> resultChunks = new ArrayList<>(messages.length - 1);
                final ChunkReader reader = schemaPlus.computeChunkReaders(
                        chunkReaderFactory, schema, streamReaderOptions)[0];
                int mi = 1;
                try {
                    for (; mi < messages.length; ++mi) {
                        final BarrageProtoUtil.MessageInfo recordBatchMessageInfo = parseArrowIpcMessage(messages[mi]);
                        if (recordBatchMessageInfo.header.headerType() != MessageHeader.RecordBatch) {
                            throw new IllegalArgumentException(String.format(
                                    "IPC message %d is not a valid Arrow RecordBatch IPC message", mi));
                        }
                        final RecordBatch batch = (RecordBatch) recordBatchMessageInfo.header.header(new RecordBatch());

                        final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                                new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                                        i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

                        final PrimitiveIterator.OfLong bufferInfoIter = ArrowToTableConverter.extractBufferInfo(batch);

                        resultChunks.add(reader.readChunk(
                                fieldNodeIter, bufferInfoIter, recordBatchMessageInfo.inputStream, null, 0, 0));
                    }

                    asyncState.setResult(resultChunks);
                } catch (final IOException ioe) {
                    SafeCloseable.closeAll(resultChunks.iterator());
                    asyncState.setError(new UncheckedDeephavenException(String.format(
                            "failed to read IPC message %d", mi), ioe));
                } catch (final RuntimeException e) {
                    SafeCloseable.closeAll(resultChunks.iterator());
                    asyncState.setError(e);
                }
            };

            final Consumer<String> onFailure = errorString -> asyncState.setError(
                    new UncheckedDeephavenException(errorString));

            pyTableDataService.call("_column_values",
                    tableKey.key, tableLocationKey.locationKey, columnName, firstRowPosition,
                    minimumSize, maximumSize, onMessages, onFailure);

            return asyncState.awaitResult(err -> new TableDataException(String.format(
                    "%s:%s: column_values(%s, %d, %d, %d) failed",
                    tableKey, tableLocationKey, columnName, firstRowPosition, minimumSize, maximumSize), err));
        }
    }

    @Override
    protected @NotNull TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
        if (!(tableKey instanceof TableKeyImpl)) {
            throw new IllegalArgumentException(String.format("%s: unsupported TableKey %s", this, tableKey));
        }
        return new TableLocationProviderImpl((TableKeyImpl) tableKey);
    }

    /**
     * {@link TableKey} implementation for TableService.
     */
    public static class TableKeyImpl implements ImmutableTableKey {

        private final PyObject key;
        private int cachedHashCode;

        public TableKeyImpl(@NotNull final PyObject key) {
            this.key = key;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TableKeyImpl)) {
                return false;
            }
            final TableKeyImpl otherTableKey = (TableKeyImpl) other;
            return this.key.equals(otherTableKey.key);
        }

        @Override
        public int hashCode() {
            if (cachedHashCode == 0) {
                final int computedHashCode = Long.hashCode(key.call("__hash__").getLongValue());
                // Don't use 0; that's used by StandaloneTableKey, and also our sentinel for the need to compute
                if (computedHashCode == 0) {
                    final int fallbackHashCode = TableKeyImpl.class.hashCode();
                    cachedHashCode = fallbackHashCode == 0 ? 1 : fallbackHashCode;
                } else {
                    cachedHashCode = computedHashCode;
                }
            }
            return cachedHashCode;
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append(getImplementationName())
                    .append("[key=").append(key.toString()).append(']');
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

    /**
     * {@link TableLocationProvider} implementation for TableService.
     */
    private class TableLocationProviderImpl extends AbstractTableLocationProvider {

        private final TableDefinition tableDefinition;

        private Subscription subscription = null;

        private TableLocationProviderImpl(@NotNull final TableKeyImpl tableKey) {
            super(tableKey, true, TableUpdateMode.APPEND_ONLY, TableUpdateMode.APPEND_ONLY);
            final BarrageUtil.ConvertedArrowSchema[] schemas = backend.getTableSchema(tableKey);

            final TableDefinition partitioningDef = schemas[0].tableDef;
            final TableDefinition tableDataDef = schemas[1].tableDef;
            final Map<String, ColumnDefinition<?>> columns = new LinkedHashMap<>(
                    partitioningDef.numColumns() + tableDataDef.numColumns());

            // all partitioning columns default to the front
            for (final ColumnDefinition<?> column : partitioningDef.getColumns()) {
                columns.put(column.getName(), column.withPartitioning());
            }

            for (final ColumnDefinition<?> column : tableDataDef.getColumns()) {
                final ColumnDefinition<?> existingDef = columns.get(column.getName());

                if (existingDef == null) {
                    columns.put(column.getName(), column);
                } else if (!existingDef.isCompatible(column)) {
                    // validate that both definitions are the same
                    throw new IllegalArgumentException(String.format("%s: column %s has conflicting definitions in "
                            + "partitioning and table data schemas: %s vs %s", tableKey, column.getName(),
                            existingDef, column));
                }
            }

            tableDefinition = TableDefinition.of(columns.values());
        }

        @Override
        protected @NotNull TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
            if (!(locationKey instanceof TableLocationKeyImpl)) {
                throw new IllegalArgumentException(String.format("%s: Unsupported TableLocationKey %s", this,
                        locationKey));
            }
            return new TableLocationImpl((TableKeyImpl) getKey(), (TableLocationKeyImpl) locationKey);
        }

        @Override
        public void refresh() {
            TableKeyImpl key = (TableKeyImpl) getKey();
            backend.getTableLocations(tableDefinition, key, this::handleTableLocationKeyAdded);
        }

        @Override
        protected void activateUnderlyingDataSource() {
            TableKeyImpl key = (TableKeyImpl) getKey();
            final Subscription localSubscription = subscription = new Subscription();
            localSubscription.cancellationCallback = backend.subscribeToTableLocations(
                    tableDefinition, key, this::handleTableLocationKeyAdded,
                    () -> activationSuccessful(localSubscription),
                    error -> activationFailed(localSubscription, new TableDataException(
                            String.format("%s: subscribe_to_table_locations failed", key), error)));
        }

        @Override
        protected void deactivateUnderlyingDataSource() {
            final Subscription localSubscription = subscription;
            subscription = null;
            if (localSubscription != null) {
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
        private int cachedHashCode;

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
            if (!(other instanceof TableLocationKeyImpl)) {
                return false;
            }
            final TableLocationKeyImpl otherTyped = (TableLocationKeyImpl) other;
            return partitions.equals((otherTyped).partitions) && locationKey.equals(otherTyped.locationKey);
        }

        @Override
        public int hashCode() {
            if (cachedHashCode == 0) {
                final int computedHashCode =
                        31 * partitions.hashCode() + Long.hashCode(locationKey.call("__hash__").getLongValue());
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
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append(getImplementationName())
                    .append(":[key=").append(locationKey.toString())
                    .append(", partitions=").append(PartitionsFormatter.INSTANCE, partitions)
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

        private synchronized void onSizeChanged(final long newSize) {
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
            backend.getTableLocationSize(key, location, this::onSizeChanged);
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
            final LongConsumer subscriptionFilter = newSize -> {
                if (localSubscription != subscription) {
                    // we've been cancelled and/or replaced
                    return;
                }

                onSizeChanged(newSize);
            };
            localSubscription.cancellationCallback = backend.subscribeToTableLocationSize(
                    key, location, subscriptionFilter, () -> activationSuccessful(localSubscription),
                    errorString -> activationFailed(localSubscription, new TableDataException(errorString)));
        }

        @Override
        protected void deactivateUnderlyingDataSource() {
            final Subscription localSubscription = subscription;
            subscription = null;
            if (localSubscription != null) {
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
            // Schema is consistent across all column locations with the same segment ID. This implementation should be
            // changed when/if we add support for rich schema evolution.
            return true;
        }

        @Override
        public ColumnRegionChar<Values> makeColumnRegionChar(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionChar<>(REGION_MASK, pageSize,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionByte<Values> makeColumnRegionByte(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionByte<>(REGION_MASK, pageSize,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionShort<Values> makeColumnRegionShort(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionShort<>(REGION_MASK, pageSize,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionInt<Values> makeColumnRegionInt(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionInt<>(REGION_MASK, pageSize,
                    new TableServiceGetRangeAdapter(columnDefinition));

        }

        @Override
        public ColumnRegionLong<Values> makeColumnRegionLong(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionLong<>(REGION_MASK, pageSize,
                    new TableServiceGetRangeAdapter(columnDefinition));

        }

        @Override
        public ColumnRegionFloat<Values> makeColumnRegionFloat(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionFloat<>(REGION_MASK, pageSize,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public ColumnRegionDouble<Values> makeColumnRegionDouble(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionDouble<>(REGION_MASK, pageSize,
                    new TableServiceGetRangeAdapter(columnDefinition));
        }

        @Override
        public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(
                @NotNull final ColumnDefinition<TYPE> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionObject<>(REGION_MASK, pageSize,
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
                    throw new TableDataException(String.format("%s:%s: column_values(%s, %d, %d, %d) did not return "
                            + "enough data. Read %d rows but expected row range was %d to %d.",
                            key, location, columnDefinition.getName(), firstRowPosition, minimumSize,
                            destination.capacity(), numRows, minimumSize, destination.capacity()));
                }
                if (numRows > destination.capacity()) {
                    throw new TableDataException(String.format("%s:%s: column_values(%s, %d, %d, %d) returned too much "
                            + "data. Read %d rows but maximum allowed is %d.", key, location,
                            columnDefinition.getName(), firstRowPosition, minimumSize, destination.capacity(), numRows,
                            destination.capacity()));
                }

                int offset = 0;
                for (final Chunk<Values> rbChunk : values) {
                    final int length = Math.min(destination.capacity() - offset, rbChunk.size());
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

    /**
     * Helper used to simplify backend asynchronous RPC patterns for otherwise synchronous operations.
     */
    private static class AsyncState<T> {
        private T result;
        private Exception error;

        public synchronized void setResult(final T result) {
            if (this.result != null) {
                throw new IllegalStateException("Callback can only be called once");
            }
            if (result == null) {
                throw new IllegalArgumentException("Callback invoked with null result");
            }
            this.result = result;
            notifyAll();
        }

        public synchronized void setError(@NotNull final Exception error) {
            if (this.error == null) {
                this.error = error;
            }
            notifyAll();
        }

        public synchronized T awaitResult(@NotNull final Function<Exception, RuntimeException> errorMapper) {
            while (result == null && error == null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw errorMapper.apply(e);
                }
            }
            if (error != null) {
                throw errorMapper.apply(error);
            }
            return result;
        }
    }
}
