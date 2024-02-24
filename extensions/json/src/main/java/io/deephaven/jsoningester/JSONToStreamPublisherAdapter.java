/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.Maps;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.Pair;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.io.logger.Logger;
import io.deephaven.jsoningester.msg.*;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An adapter that maps JSON objects to Deephaven columns. Each top-level JSON object produces one Deephaven row.
 * <p>
 * A factory for adapter should be created using the nested Builder class. Each adapter instance is bound to the
 * TableWriter that is passed to the factory method.
 */
public class JSONToStreamPublisherAdapter implements StringIngestionAdapter<StringMessageToBlinkTableAdapter<?>> {
    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final int ERROR_REPORTING = 96;
    private static final int ERROR_PROCESSING = 98;
    private static final int MAX_UNPARSEABLE_LOG_MESSAGES = Configuration.getInstance()
            .getIntegerWithDefault("JSONToStreamPublisherAdapter.maxUnparseableLogMessages", 100);
    private static final int SHUTDOWN_TIMEOUT_SECS = 30;

    /**
     * Column in subtable that identifiers corresponding row in parent table.
     */
    public static final String SUBTABLE_RECORD_ID_COL = "SubtableRecordId";

    /**
     * Suffix of column name in parent table that identifies corresponding rows in subtable.
     */
    private static final String SUBTABLE_RECORD_ID_SUFFIX = "_id";
    static final int N_CONSUMER_THREADS_DEFAULT =
            Configuration.getInstance().getIntegerWithDefault("JSONToStreamPublisherAdapter.consumerThreads", 1);

    @SuppressWarnings("FieldCanBeLocal")
    private final ThreadGroup consumerThreadGroup;
    private final List<AsynchronousDataIngester> allSubtableAdapters;
    private ShutdownManager.Task shutdownTask;
    private final ShutdownManager.OrderingCategory SHUTDOWN_TASK_ORDERING_CATEGORY =
            ShutdownManager.OrderingCategory.FIRST;

    /**
     * The owner message adapter, which has {@link RowSetter setters} for metadata columns.
     */
    @Nullable
    private StringMessageToBlinkTableAdapter<?> owner;

    /**
     * TableWriter to write data to. TODO: replace this with a io.deephaven.stream.StreamPublisherImpl. Be mindful of
     * transactions -- flush() all rows at once. Maybe ensure that subtables are updated at the same time too.
     */
    private final SimpleStreamPublisher streamPublisher;

    private final TObjectIntMap<String> colNameToColIdx;
    private final Map<String, Class<?>> colNameToType;

    private final Logger log;
    private final boolean allowMissingKeys;
    private final boolean allowNullValues;

    /**
     * Chunk populators (which copy data that was parsed into {@link InMemoryRowHolder InMemoryRowHolders} by the
     * {@link #fieldProcessors} into WritableChunks)
     */
    private final Map<String, Consumer<InMemoryRowHolder>> chunkPopulators;

    /**
     * Field processors, which pull data out of parsed JSON messages)
     */
    private final List<ObjIntConsumer<JsonNode>> fieldProcessors = new ArrayList<>();

    private final Map<String, Map<String, JsonPointer>> nestedFieldToJsonPointers = new HashMap<>();

    private final List<String> arrayFieldNames = new ArrayList<>();
    private final List<ObjIntConsumer<JsonNode>> arrayFieldProcessors = new ArrayList<>();
    private final List<JSONToStreamPublisherAdapter> nestedAdapters = new ArrayList<>();
    private final boolean processArrays;

    /**
     * Number of JSON processing threads. If {@code numThreads == 0}, then {@link #consumeString} will process messages
     * synchronously (instead of enqueuing them on the {@link #waitingMessages} queue to be processed by the
     * {@link #consumerThreadGroup consumer threads}).
     */
    private final int numThreads;

    /**
     * Latch used to wait for JSON threads to finish during shutdown (if threads are used)
     */
    private final CountDownLatch consumerThreadsCountDownLatch;

    /**
     * The queue of messages that are awaiting processing. After a message is removed from this queue, parsed, and
     * processed into a {@link InMemoryRowHolder row holder}, it is added to the {@link #processedMessages} queue.
     */
    private final BlockingQueue<JsonMessage> waitingMessages = new LinkedBlockingQueue<>();

    /**
     * The queue of messages that have been processed into {@link InMemoryRowHolder row holders}, but not yet flushed by
     * {@link #cleanup()}.
     */
    private final BlockingQueue<InMemoryRowHolder> processedMessages = new LinkedBlockingQueue<>();
    private final int CONSUMER_WAIT_INTERVAL_MS =
            Configuration.getInstance().getIntegerWithDefault("JSONToStreamPublisherAdapter.consumerWaitInterval", 100);
    private final int CONSUMER_REPORT_INTERVAL_MS =
            Configuration.getInstance().getIntegerWithDefault("JSONToStreamPublisherAdapter.consumerReportInterval",
                    60000);
    private InMemoryRowHolder[] holders;

    /**
     * The number of messages that have been enqueued for processing (e.g. by calling {@link #consumeJson}
     */
    private final AtomicLong messagesQueued = new AtomicLong();

    /**
     * The number of messages that have been processed into a {@link InMemoryRowHolder row holder}. Processed messages
     * must be written and flushed to tables by calling {@link #cleanup()} beforer they are available to the query
     * engine.
     */
    private final LongAdder messagesProcessed = new LongAdder();

    /**
     * Sequence number expected for the next message.
     */
    private final AtomicLong nextMsgNo = new AtomicLong(0);
    private long lastProcessed = 0;
    // We want all the processing threads to share reporting times, so this should not in fact be local.
    // Otherwise all the threads want to report separately.
    @SuppressWarnings("FieldCanBeLocal")
    private long before = System.nanoTime();
    private long lastReportNanos = System.nanoTime();
    private final long reportIntervalNanos = CONSUMER_REPORT_INTERVAL_MS * NANOS_PER_MILLI;
    private long nextReportTime = System.nanoTime() + reportIntervalNanos;
    private final PermissiveArrayList<InMemoryRowHolder> pendingCleanup = new PermissiveArrayList<>();

    private final boolean skipUnparseableMessages =
            Configuration.getInstance().getBooleanWithDefault("JSONToStreamPublisherAdapter.skipUnparseableMessages",
                    true);
    private int unparseableMessagesLogged = 0;

    /**
     * Whether this adapter has been shut down. An exception will be thrown if messages are received (by
     * {@link #consumeString}) after shutdown, since it may be too late for them be processed.
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Instance count. Used for thread names.
     */
    private static final AtomicInteger instanceCounter = new AtomicInteger();
    private final Map<String, JSONToStreamPublisherAdapter> subtableFieldsToAdapters = new LinkedHashMap<>();

    private final ThreadLocal<Queue<SubtableData<JSONToStreamPublisherAdapter>>> subtableProcessingQueueThreadLocal;

    private final boolean isNestedAdapter;
    private final boolean isSubtableAdapter;

    /**
     * Adapter instance ID (used for thread names).
     */
    private final int instanceId;

    /**
     * Function to run after processing a JSON record.
     */
    @Nullable
    private final BiConsumer<MessageMetadata, JsonNode> postProcessConsumer;

    /**
     * Local reference to the chunks (which are really owned by the {@link #streamPublisher}).
     */
    private WritableChunk<Values>[] chunks = null;

    private int nowTimeColIdx = -1;

    JSONToStreamPublisherAdapter(@NotNull final SimpleStreamPublisher streamPublisher,
            @NotNull final Logger log,
            final boolean allowMissingKeys,
            final boolean allowNullValues,
            final boolean processArrays,
            final int nConsumerThreads,
            @NotNull final Map<String, String> columnToJsonField,
            @NotNull final Map<String, JsonPointer> columnToJsonPointer,
            @NotNull final Map<String, ToIntFunction<JsonNode>> columnToIntFunctions,
            @NotNull final Map<String, ToLongFunction<JsonNode>> columnToLongFunctions,
            @NotNull final Map<String, ToDoubleFunction<JsonNode>> columnToDoubleFunctions,
            @NotNull final Map<String, Pair<Class<?>, Function<JsonNode, ?>>> columnToObjectFunctions,
            @NotNull final Map<String, JSONToStreamPublisherAdapterBuilder> nestedFieldBuilders,
            @NotNull final Map<String, String> columnToParallelField,
            @NotNull final Map<String, JSONToStreamPublisherAdapterBuilder> parallelNestedFieldBuilders,
            @NotNull final Map<String, SimpleStreamPublisher> fieldToSubtablePublishers,
            @NotNull final Map<String, JSONToStreamPublisherAdapterBuilder> fieldToSubtableBuilders,
            @NotNull final Map<String, RoutedAdapterInfo> fieldToRoutedTableBuilders,
            @NotNull final Set<String> columnsUnmapped,
            final boolean autoValueMapping,
            final boolean createHolders,
            @Nullable final BiConsumer<MessageMetadata, JsonNode> postProcessConsumer) {
        this(streamPublisher,
                log,
                allowMissingKeys,
                allowNullValues,
                processArrays,
                nConsumerThreads,
                columnToJsonField,
                columnToJsonPointer,
                columnToIntFunctions,
                columnToLongFunctions,
                columnToDoubleFunctions,
                columnToObjectFunctions,
                nestedFieldBuilders,
                columnToParallelField,
                parallelNestedFieldBuilders,
                fieldToSubtablePublishers,
                fieldToSubtableBuilders,
                fieldToRoutedTableBuilders,
                columnsUnmapped,
                autoValueMapping,
                createHolders,
                ThreadLocal.withInitial(ConcurrentLinkedDeque::new),
                postProcessConsumer,
                false);
    }

    /**
     * @param streamPublisher
     * @param log
     * @param allowMissingKeys
     * @param allowNullValues
     * @param processArrays
     * @param nThreads
     * @param columnToJsonField
     * @param columnToJsonPointer
     * @param columnToIntFunctions
     * @param columnToLongFunctions
     * @param columnToDoubleFunctions
     * @param columnToObjectFunctions
     * @param nestedFieldBuilders
     * @param columnToParallelField
     * @param parallelNestedFieldBuilders
     * @param fieldToSubtablePublisher The map of subtable fields to writers is used when building child adapters.
     * @param fieldToSubtableBuilders
     * @param allowedUnmappedColumns
     * @param autoValueMapping
     * @param createHolders Whether to create the InMemmoryRowHolders and associated thread pool.
     * @param subtableProcessingQueueThreadLocal
     * @param postProcessConsumer
     */
    JSONToStreamPublisherAdapter(
            @NotNull final SimpleStreamPublisher streamPublisher,
            @NotNull final Logger log,
            final boolean allowMissingKeys,
            final boolean allowNullValues,
            final boolean processArrays,
            final int nThreads,
            @NotNull final Map<String, String> columnToJsonField,
            @NotNull final Map<String, JsonPointer> columnToJsonPointer,
            @NotNull final Map<String, ToIntFunction<JsonNode>> columnToIntFunctions,
            @NotNull final Map<String, ToLongFunction<JsonNode>> columnToLongFunctions,
            @NotNull final Map<String, ToDoubleFunction<JsonNode>> columnToDoubleFunctions,
            @NotNull final Map<String, Pair<Class<?>, Function<JsonNode, ?>>> columnToObjectFunctions,
            @NotNull final Map<String, JSONToStreamPublisherAdapterBuilder> nestedFieldBuilders,
            @NotNull final Map<String, String> columnToParallelField,
            @NotNull final Map<String, JSONToStreamPublisherAdapterBuilder> parallelNestedFieldBuilders,
            @NotNull final Map<String, SimpleStreamPublisher> fieldToSubtablePublisher,
            @NotNull final Map<String, JSONToStreamPublisherAdapterBuilder> fieldToSubtableBuilders,
            @NotNull final Map<String, RoutedAdapterInfo> routedTableIdsToBuilders,
            @NotNull final Set<String> allowedUnmappedColumns,
            final boolean autoValueMapping,
            final boolean createHolders,
            final ThreadLocal<Queue<SubtableData<JSONToStreamPublisherAdapter>>> subtableProcessingQueueThreadLocal,
            @Nullable final BiConsumer<MessageMetadata, JsonNode> postProcessConsumer,
            final boolean isSubtableAdapter) {
        this.log = log;
        this.streamPublisher = streamPublisher;
        this.allowMissingKeys = allowMissingKeys;
        this.allowNullValues = allowNullValues;
        this.processArrays = processArrays;
        this.numThreads = nThreads;
        this.subtableProcessingQueueThreadLocal = subtableProcessingQueueThreadLocal;
        this.isNestedAdapter = !createHolders;
        this.isSubtableAdapter = isSubtableAdapter;
        this.postProcessConsumer = postProcessConsumer;

        chunkPopulators = new LinkedHashMap<>(streamPublisher.getTableDefinition().numColumns());

        instanceId = instanceCounter.getAndIncrement();


        final TableDefinition tableDefinition = streamPublisher.getTableDefinition();
        colNameToType = Maps.transformValues(tableDefinition.getColumnNameMap(), ColumnDefinition::getDataType);
        colNameToColIdx = new TObjectIntHashMap<>(tableDefinition.numColumns(), 0.5f, -1);
        final String[] expectedColNamesArr = tableDefinition.getColumnNamesArray();
        for (int ii = 0; ii < expectedColNamesArr.length; ii++) {
            colNameToColIdx.put(expectedColNamesArr[ii], ii);
        }

        // Get the list of all the columns that our nested builders provide.
        final Set<String> nestedColumns = Stream.concat(
                nestedFieldBuilders.values().stream(),
                parallelNestedFieldBuilders.values().stream())
                .flatMap(builder -> builder.getDefinedColumns().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // The subtable row ID columns are implicitly defined for every field that's mapped to a subtable.
        final Set<String> subtableRowIdColumns = Stream.concat(
                fieldToSubtableBuilders.keySet().stream(),
                routedTableIdsToBuilders.keySet().stream())
                .map(JSONToStreamPublisherAdapter::getSubtableRowIdColName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // Build a collection of all columns expected to be available in the output table writer.
        final Collection<String> outputColumnNames = new LinkedHashSet<>(columnToJsonField.keySet());
        outputColumnNames.addAll(columnToJsonPointer.keySet());
        outputColumnNames.addAll(columnToIntFunctions.keySet());
        outputColumnNames.addAll(columnToLongFunctions.keySet());
        outputColumnNames.addAll(columnToDoubleFunctions.keySet());
        outputColumnNames.addAll(columnToObjectFunctions.keySet());
        outputColumnNames.addAll(columnToParallelField.keySet());
        outputColumnNames.addAll(allowedUnmappedColumns);
        outputColumnNames.addAll(nestedColumns);
        outputColumnNames.addAll(subtableRowIdColumns);

        final List<String> allColumns = List.copyOf(outputColumnNames);

        final List<String> missingColumns = new ArrayList<>();
        for (final String columnName : expectedColNamesArr) {
            if (nestedColumns.contains(columnName)) {
                continue;
            }
            if (subtableRowIdColumns.contains(columnName)) {
                continue;
            }
            outputColumnNames.remove(columnName);
            if (allowedUnmappedColumns.contains(columnName)) {
                continue;
            }
            if (columnToJsonField.containsKey(columnName)) {
                makeFieldProcessors(columnName, columnToJsonField.get(columnName));
            } else if (columnToJsonPointer.containsKey(columnName)) {
                makeJsonPointerFieldProcessor(columnName, columnToJsonPointer.get(columnName));
            } else if (columnToIntFunctions.containsKey(columnName)) {
                makeIntFunctionFieldProcessor(columnName, columnToIntFunctions.get(columnName));
            } else if (columnToLongFunctions.containsKey(columnName)) {
                makeLongFunctionFieldProcessor(columnName, columnToLongFunctions.get(columnName));
            } else if (columnToDoubleFunctions.containsKey(columnName)) {
                makeDoubleFunctionFieldProcessor(columnName, columnToDoubleFunctions.get(columnName));
            } else if (columnToObjectFunctions.containsKey(columnName)) {
                final Pair<Class<?>, Function<JsonNode, ?>> classFunctionPair = columnToObjectFunctions.get(columnName);
                makeFunctionFieldProcessor(columnName, classFunctionPair.first, classFunctionPair.second);
            } else if (columnToParallelField.containsKey(columnName)) {
                makeArrayFieldProcessors(columnName, columnToParallelField.get(columnName));
            } else if (autoValueMapping) {
                makeFieldProcessors(columnName, columnName);
            } else {
                missingColumns.add(columnName);
            }
        }

        // This is the only part of this method/class that works in the sense of mapping JSON fields to DH outputs.
        // Everything else maps DH outputs to the corresponding JSON source field.
        for (Map.Entry<String, JSONToStreamPublisherAdapterBuilder> subtableEntry : fieldToSubtableBuilders
                .entrySet()) {
            final String fieldName = subtableEntry.getKey();
            outputColumnNames.remove(getSubtableRowIdColName(fieldName));

            final JSONToStreamPublisherAdapterBuilder adapterBuilder = subtableEntry.getValue();
            // outputColumnNames.removeAll(adapterBuilder.getDefinedColumns());

            try {
                final @NotNull SimpleStreamPublisher subtableWriter =
                        Require.neqNull(fieldToSubtablePublisher.get(fieldName), "subtableWriter");
                makeSubtableFieldProcessor(
                        fieldName,
                        false,
                        adapterBuilder,
                        subtableWriter,
                        fieldToSubtablePublisher,
                        allowMissingKeys,
                        allowNullValues,
                        null);
            } catch (RuntimeException ex) {
                throw new JSONIngesterException(
                        "Failed creating field processor for subtable field \"" + fieldName + '"', ex);
            }
        }

        for (Map.Entry<String, RoutedAdapterInfo> routedAdapterEntry : routedTableIdsToBuilders
                .entrySet()) {
            final String routedTableIdentifier = routedAdapterEntry.getKey();
            outputColumnNames.remove(getSubtableRowIdColName(routedTableIdentifier));

            final RoutedAdapterInfo routedAdapterInfo = routedAdapterEntry.getValue();
            final JSONToStreamPublisherAdapterBuilder adapterBuilder = routedAdapterInfo.adapterBuilder;
            final Predicate<JsonNode> routedTablePredicate = routedAdapterInfo.predicate;
            // outputColumnNames.removeAll(adapterBuilder.getDefinedColumns());

            try {
                final SimpleStreamPublisher subtablePublisher =
                        Require.neqNull(fieldToSubtablePublisher.get(routedTableIdentifier), "subtableWriter");
                makeSubtableFieldProcessor(
                        routedTableIdentifier,
                        true,
                        adapterBuilder,
                        subtablePublisher,
                        fieldToSubtablePublisher,
                        allowMissingKeys,
                        allowNullValues, routedTablePredicate);
            } catch (RuntimeException ex) {
                throw new JSONIngesterException(
                        "Failed creating field processor for routed table \"" + routedTableIdentifier + '"', ex);
            }
        }


        for (Map.Entry<String, JSONToStreamPublisherAdapterBuilder> nestedFieldEntry : nestedFieldBuilders.entrySet()) {
            outputColumnNames.removeAll(nestedFieldEntry.getValue().getDefinedColumns());
            final String fieldName = nestedFieldEntry.getKey();
            try {
                makeCompositeFieldProcessor(streamPublisher, fieldToSubtablePublisher, allColumns, fieldName,
                        nestedFieldEntry.getValue());
            } catch (RuntimeException ex) {
                throw new JSONIngesterException(
                        "Failed creating field processor for nested field \"" + fieldName + '"', ex);
            }
        }

        // ensure there is a nested field processor for any field that matches the head() of a JsonPointer
        for (String jsonPointerHeadFieldName : nestedFieldToJsonPointers.keySet()) {
            if (!nestedFieldBuilders.containsKey(jsonPointerHeadFieldName)) {
                try {
                    makeCompositeFieldProcessor(streamPublisher, fieldToSubtablePublisher, allColumns,
                            jsonPointerHeadFieldName,
                            new JSONToStreamPublisherAdapterBuilder().autoValueMapping(false));
                } catch (RuntimeException ex) {
                    throw new JSONIngesterException(
                            "Failed creating field processor for field from JsonPointer \"" + jsonPointerHeadFieldName
                                    + '"',
                            ex);
                }
            }
        }


        for (Map.Entry<String, JSONToStreamPublisherAdapterBuilder> nestedParallelFieldEntry : parallelNestedFieldBuilders
                .entrySet()) {
            outputColumnNames.removeAll(nestedParallelFieldEntry.getValue().getDefinedColumns());
            final String fieldName = nestedParallelFieldEntry.getKey();
            try {
                makeCompositeParallelFieldProcessor(streamPublisher, fieldToSubtablePublisher, allColumns,
                        fieldName, nestedParallelFieldEntry.getValue());
            } catch (RuntimeException ex) {
                throw new JSONIngesterException("Failed creating field processor for nested parallel field \""
                        + fieldName + '"', ex);
            }
        }

        if (!missingColumns.isEmpty()) {
            final StringBuilder sb = new StringBuilder("Found columns without mappings " + missingColumns);
            if (!allowedUnmappedColumns.isEmpty()) {
                sb.append(", allowed unmapped=").append(allowedUnmappedColumns);
            }
            if (!columnToJsonField.isEmpty()) {
                sb.append(", mapped to fields=").append(columnToJsonField.keySet());
            }
            if (!columnToIntFunctions.isEmpty()) {
                sb.append(", mapped to int functions=").append(columnToIntFunctions.keySet());
            }
            if (!columnToLongFunctions.isEmpty()) {
                sb.append(", mapped to long functions=").append(columnToLongFunctions.keySet());
            }
            if (!columnToDoubleFunctions.isEmpty()) {
                sb.append(", mapped to double functions=").append(columnToDoubleFunctions.keySet());
            }
            if (!columnToObjectFunctions.isEmpty()) {
                sb.append(", mapped to functions=").append(columnToObjectFunctions.keySet());
            }
            throw new JSONIngesterException(sb.toString());
        }
        if (!outputColumnNames.isEmpty()) {
            throw new JSONIngesterException(
                    "Found mappings that do not correspond to this table: " + outputColumnNames);
        }

        // A value must be added for ***ALL CHUNKS***, including the unmapped ones.
        // (otherwise, when processing a record, some columns' chunks will have 1 value appended and others will have 0)
        // However, composite adapters must *not* do this -- only the outermost parent adapters.
        // Also note that subtable adapters are effectively parent adapters as well.
        if (!isNestedAdapter) {
            for (String col : allowedUnmappedColumns) {
                addNullFieldSetter(col);
            }
        }

        allSubtableAdapters = getAllSubtableAdapters(this);

        if (numThreads > 1 && (!columnToParallelField.isEmpty() || !parallelNestedFieldBuilders.isEmpty()
                || !subtableFieldsToAdapters.isEmpty())) {
            throw new JSONIngesterException(
                    "JSON multithreaded processing does not yet support multiple output rows per message.");
        }

        if (createHolders) {
            // the top level adapter creates the holders, which have spaces for each saved value including in the nested
            // holders. Now that we've processed all the nested adapters, we can set the holders for those adapters
            // which in turn set it for any nested adapter they have.
            final int numHolders = numThreads > 0 ? numThreads : 1;
            holders = new InMemoryRowHolder[numHolders];
            for (int holderIdx = 0; holderIdx < numHolders; holderIdx++) {
                holders[holderIdx] = createRowHolder(); //
            }

            // Create the message processing threads -- one per holder.
            // For subtable adapters, numThreads is zero, and processing is done synchronously by consumeString().
            // This way, subtables from multiple records are parsed under the parent adapter's thread pool.
            if (numThreads > 0
                    || numThreads == -1 // numThreads==-1 is used in tests to create an adapter that never processes
            ) {
                consumerThreadGroup =
                        new ThreadGroup(
                                JSONToStreamPublisherAdapter.class.getSimpleName() + instanceId + "_ThreadGroup");

                for (int threadCount = 0; threadCount < numThreads; threadCount++) {
                    final Thread t = new ConsumerThread(consumerThreadGroup, instanceId, threadCount);
                    t.setDaemon(true);
                    t.start();
                }
            } else {
                consumerThreadGroup = null;
            }
            setNestedHolders(holders);

            consumerThreadsCountDownLatch = new CountDownLatch(Math.max(0, numThreads));
        } else {
            if (numThreads != 0) {
                throw new IllegalArgumentException(
                        "numThreads must be zero when createHolders is false. numThreads: " + numThreads);
            }
            consumerThreadGroup = null;
            consumerThreadsCountDownLatch = null;
        }
    }

    /**
     * Starts a thread to run {@link #cleanup()} occasionally. Also registers a task to {@link #shutdown()} this adapter
     * cleanly at VM shutdown. The cleanup thread will also {@link #closePublishers() close the table writers} before
     * exiting.
     *
     * @param flushIntervalMillis Interval in milliseconds at which {@code cleanup()} is run.
     */
    synchronized void createCleanupThread(final long flushIntervalMillis) {
        // TODO: It'd be better to kill this and just process the data directly into the chunks, if possible...
        // but that might not be simple/possible without throwing away parallel parsing support

        if (isShutdown.get()) {
            throw new IllegalStateException("Adapter already shut down");
        }
        if (shutdownTask != null) {
            throw new IllegalStateException("Cleanup thread already created");
        }

        // Start a thread to flush the adapter
        final Thread cleanupThread = new Thread(() -> {
            long lastFlush = 0L;
            // cleanup() every flushIntervalMillis until shutdown.
            // (then cleanup once more after consumer threads exit.)
            try {
                while (!isShutdown.get()) {
                    try {
                        // noinspection BusyWait
                        Thread.sleep(flushIntervalMillis);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(
                                Thread.currentThread().getName() + ": Interrupted while waiting to flush", e);
                    }
                    final long now = System.currentTimeMillis();
                    if (now - lastFlush > flushIntervalMillis) {
                        try {
                            lastFlush = now;
                            JSONToStreamPublisherAdapter.this.cleanup();
                        } catch (IOException e) {
                            throw new UncheckedIOException(Thread.currentThread().getName()
                                    + ": Exception while flushing data to table writers", e);
                        }
                    }
                }

                // wait for consumer threads to exit -- all messages should be processed by then
                try {
                    final int timeoutSecs = 30;
                    if (!(JSONToStreamPublisherAdapter.this.consumerThreadsCountDownLatch.await(timeoutSecs,
                            TimeUnit.SECONDS))) {
                        log.warn().append("Consumer threads did not exit within timeout (").append(timeoutSecs)
                                .append(" seconds)").endl();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while waiting for consumer threads to exit", e);
                }

                try {
                    JSONToStreamPublisherAdapter.this.cleanup();
                } catch (IOException e) {
                    throw new UncheckedIOException("Exception occurred while flushing data to table writers", e);
                }
            } finally {
                try {
                    JSONToStreamPublisherAdapter.this.closePublishers();
                } catch (IOException e) {
                    log.error().append(new UncheckedIOException("Exception occurred while closing table writers", e))
                            .endl();
                }
            }
        }, JSONToStreamPublisherAdapter.class.getSimpleName() + '_' + instanceId + "_cleanupThread");
        cleanupThread.setDaemon(true);
        cleanupThread.start();

        // handle shutdown. (since we are taking responsibility for flushing the data, we should also take
        // responsibility for clean shutdown.)
        // this is not done by default in the constructor because it can contribute to memory leaks -- if the writer
        // is an in-memory table writer, then the shutdown hook's reference to this adapter will lead to retention
        // of the table writer, its output table, and all the data written to that table.
        shutdownTask = this::shutdown;
        ProcessEnvironment.getGlobalShutdownManager().registerTask(SHUTDOWN_TASK_ORDERING_CATEGORY, shutdownTask);
    }

    private synchronized void deregisterShutdownTask() {
        // deregister the shutdown task (to prevent a memory leak)
        if (shutdownTask != null) {
            ProcessEnvironment.getGlobalShutdownManager().deregisterTask(SHUTDOWN_TASK_ORDERING_CATEGORY,
                    shutdownTask);
            shutdownTask = null;
        }
    }

    /**
     * Get a column name that will contain an ID used for mapping between the parent table row and the corresponding
     * subtable rows.
     *
     * @param fieldName The name of the field containing the subtable data.
     * @return The {@code fieldName} concatenated with the {@link #SUBTABLE_RECORD_ID_SUFFIX}
     */
    @NotNull
    public static String getSubtableRowIdColName(String fieldName) {
        return fieldName + SUBTABLE_RECORD_ID_SUFFIX;
    }

    private void setHolders(final InMemoryRowHolder[] holders) {
        this.holders = holders;
        setNestedHolders(holders);
    }

    private void setNestedHolders(final InMemoryRowHolder[] holders) {
        nestedAdapters.forEach(na -> na.setHolders(holders));
    }

    private void makeCompositeFieldProcessor(final SimpleStreamPublisher publisher,
            final Map<String, SimpleStreamPublisher> subtablePublishers,
            final List<String> allColumns,
            final String fieldName,
            final JSONToStreamPublisherAdapterBuilder originalNestedBuilder) {

        // Build a new set of columns that are allowed to be unmapped, as far as the nested field processor is
        // concerned.
        final Set<String> newAllowedUnmapped = new HashSet<>(allColumns);

        // JsonPointers to process (i.e. the `JsonPointer.tail()` of pointers whose getMatchingProperty() value
        // matches this fieldName).
        final Map<String, JsonPointer> jsonPointers = nestedFieldToJsonPointers.get(fieldName);

        final JSONToStreamPublisherAdapterBuilder builderWithPointers;
        if (jsonPointers != null && !jsonPointers.isEmpty()) {
            // update the columnToJsonPointer map to handle the tail() of any JsonPointers that were in the
            // parent adapter.
            builderWithPointers = new JSONToStreamPublisherAdapterBuilder(originalNestedBuilder);
            for (Map.Entry<String, JsonPointer> pointers : jsonPointers.entrySet()) {
                builderWithPointers.addColumnFromPointer(pointers.getKey(), pointers.getValue());
            }
        } else {
            builderWithPointers = originalNestedBuilder;
        }

        // anything defined by the nested builder must be mapped.
        newAllowedUnmapped.removeAll(builderWithPointers.getDefinedColumns());

        // // implicit subtable ID columns defined in the parent are not relevant to the nested adapter
        // newAllowedUnmapped.removeAll(subtableFieldsToAdapters.keySet());

        final JSONToStreamPublisherAdapter nestedAdapter =
                builderWithPointers.makeNestedAdapter(log, publisher, subtablePublishers, newAllowedUnmapped,
                        subtableProcessingQueueThreadLocal);
        nestedAdapters.add(nestedAdapter);

        // we make a single field processor that in turn calls the nested adapter's field processors after extracting
        // the correct record from this JSON and making a new one. The nested adapter has as many field setters as
        // needed for each of its fields (meaning the processors and setter array lists are not actually parallel).
        fieldProcessors.add(((jsonRecord, holder) -> {
            try {
                final Object field = JsonNodeUtil.getValue(jsonRecord, fieldName, allowMissingKeys, allowNullValues);
                final JsonNode record;
                if (field == null) {
                    record = NullNode.getInstance();
                } else if (field instanceof JsonNode) {
                    record = (JsonNode) field;
                } else {
                    throw new JSONIngesterException("Field \"" + fieldName + "\" is of unexpected type "
                            + field.getClass() + ", expected JsonNode");
                }
                nestedAdapter.fieldProcessors.forEach(fc -> fc.accept(record, holder));
            } catch (Exception ex) {
                throw new JSONIngesterException("Exception while processing nested field \"" + fieldName + "\"", ex);
            }
        }));

        // Add the chunkPopulators created in the nested adapter to this adapter
        nestedAdapter.chunkPopulators.forEach(this::putChunkPopulator);
    }

    private void makeCompositeParallelFieldProcessor(final SimpleStreamPublisher publisher,
            final Map<String, SimpleStreamPublisher> subtablePublishers,
            final List<String> allColumns,
            final String fieldName,
            final JSONToStreamPublisherAdapterBuilder nestedBuilder) {
        final Set<String> newUnmapped = new HashSet<>(allColumns);
        newUnmapped.removeAll(nestedBuilder.getDefinedColumns());

        // get the JsonPointers to be processed by the nested adapter
        final Map<String, JsonPointer> jsonPointersForNestedAdapter = nestedFieldToJsonPointers.get(fieldName);

        final JSONToStreamPublisherAdapter nestedAdapter =
                nestedBuilder.makeNestedAdapter(log, publisher, subtablePublishers, newUnmapped,
                        subtableProcessingQueueThreadLocal);
        nestedAdapters.add(nestedAdapter);

        arrayFieldNames.add(fieldName);
        arrayFieldProcessors.add(((jsonNode, holder) -> {
            final JsonNode record;
            if (jsonNode == null || jsonNode.isNull()) {
                record = NullNode.getInstance();
            } else {
                record = jsonNode;
            }
            nestedAdapter.fieldProcessors.forEach(fc -> fc.accept(record, holder));
        }));

        // Add the chunkPopulators created in the nested adapter to this adapter
        nestedAdapter.chunkPopulators.forEach(this::putChunkPopulator);
    }


    /**
     * This is like {@link #makeCompositeFieldProcessor} except it processes fields into a different table.
     *
     * @param fieldName The name of the JSON field containing an ArrayNode of subtable data, or {@code null}
     * @param isRoutedTable If {@code true}, the original JSON node is passed to the subtable adapter, instead of the
     *        JsonNode extracted from {@code fieldName}. This is helpful for routing messages to different adapters
     *        (i.e. different tables), typically in conjunction with a {@code subtablePredicate}).
     * @param subtableBuilder The subtable JSON adapter builder
     * @param subtablePublisher The subtable's publisher
     * @param subtableKeyAllowedMissing Whether the subtable's key ({@code fieldName}) is allowed to be missing. Ignored
     *        when {@code isRoutedTable} is {@code true}.
     * @param subtableKeyAllowedNull Whether {@code fieldName} is allowed to have a null value. Ignored when
     *        {@code isRoutedTable} is {@code true}.
     * @param subtablePredicate A predicate to evaluate for the node, to determine whether the node should be processed.
     */
    private void makeSubtableFieldProcessor(
            @NotNull final String fieldName,
            final boolean isRoutedTable,
            @NotNull final JSONToStreamPublisherAdapterBuilder subtableBuilder,
            @NotNull final SimpleStreamPublisher subtablePublisher,
            @NotNull final Map<String, SimpleStreamPublisher> allSubtablePublishers,
            final boolean subtableKeyAllowedMissing,
            final boolean subtableKeyAllowedNull,
            final Predicate<JsonNode> subtablePredicate) {

        // Subtable record counter, mapping each row of the parent table to the corresponding rows of the subtable.
        // TODO: it would be better to use a unique parent message ID if available. (Pull it out of the holder?)
        final AtomicLong subtableRecordIdCounter = new AtomicLong(0);

        // ThreadLocal to store the record counter value. The subtable record ID is captured into the
        // ThreadLocal MutableLong while processing the parent record (and stored in the parent record's row holder),
        // then read later by a field processor in the subtable adapter (which adds it to each subtable row's row
        // holder).
        final ThreadLocal<MutableLong> subtableRecordIdThreadLocal = ThreadLocal.withInitial(MutableLong::new);

        final JSONToStreamPublisherAdapter subtableAdapter = subtableBuilder
                .makeSubtableAdapter(
                        log,
                        subtablePublisher,
                        allSubtablePublishers,
                        Collections.emptySet(),
                        subtableProcessingQueueThreadLocal,
                        subtableRecordIdThreadLocal);
        subtableFieldsToAdapters.put(fieldName, subtableAdapter);

        // monotonic increasing message numbers for subtables are automatically generated
        //
        // This is distinct from the counter above because this is only used internally
        // and is only required to increase during the lifespan of this adapter.
        // The ID above is displayed in tables and ideally would be unique even if persisted/reread.
        final AtomicLong subtableMessageCounter = new AtomicLong(0);

        final SubtableProcessingParameters<JSONToStreamPublisherAdapter> subtableProcessingParameters =
                new SubtableProcessingParameters<>(
                        fieldName,
                        subtableAdapter,
                        subtablePredicate,
                        subtableMessageCounter,
                        subtableKeyAllowedMissing,
                        subtableKeyAllowedNull,
                        !isRoutedTable);

        // Field name in the *parent* Table giving the corresponding ID of the row(s) in the subtable
        final String subtableRowIdColNameInParent = getSubtableRowIdColName(fieldName);

        /*
         * // setter for the *subtable* that will contain that same ID final int subtableRowIdColIdx =
         * subtableAdapter.colNameToColIdx.get(SUBTABLE_RECORD_ID_COL);
         * Assert.equals(subtableAdapter.colNameToType.get(SUBTABLE_RECORD_ID_COL),
         * "subtableAdapter.colNameToType.get(SUBTABLE_RECORD_ID_COL)", long.class); final WritableLongChunk<Values>
         * chunk = subtableAdapter.getChunksToFill()[subtableRowIdColIdx].asWritableLongChunk();
         * 
         * final RowSetter<Long> setter = subtableWriter.getSetter(SUBTABLE_RECORD_ID_COL, long.class); final Class<?>
         * subtableRowIdColType = setter.getType();
         */
        final Class<?> subtableRowIdColType = long.class;

        final ObjIntConsumer<JsonNode> fieldProcessor;
        // TODO: just write to the corresponding chunk directly, instead of into the InMemoryRowHolder?
        final MutableInt position = new MutableInt();
        fieldProcessor = (JsonNode record, int holderNumber) -> {
            // index of the next record being processed into the subtable
            // TODO: holders[holderNumber].getMessageNumber() should be fine
            final long subtableRecordIdxVal = subtableRecordIdCounter.getAndIncrement();

            // store it in a ThreadLocal that is read by the subtable field processor. (It is processed for the subtable
            // synchronously, later in processOneRecordTopLevel).
            subtableRecordIdThreadLocal.get().setValue(subtableRecordIdxVal);

            final JsonNode subtableFieldValue;
            if (isRoutedTable) {
                subtableFieldValue = record;
            } else {
                subtableFieldValue =
                        JsonNodeUtil.checkAllowMissingOrNull(record, fieldName, allowMissingKeys, allowNullValues);
            }

            // store the idx in the rowSetter (later, the fieldSetter will add it to the table)
            // note that this will only work correctly when single-threaded
            final InMemoryRowHolder.SingleRowSetter rowSetter =
                    getSingleRowSetterAndCapturePosition(subtableRowIdColNameInParent, subtableRowIdColType, position,
                            holderNumber);

            if (subtablePredicate == null || subtablePredicate.test(subtableFieldValue)) {
                rowSetter.setLong(subtableRecordIdxVal);
                // Enqueue the subtable node to be processed by the subtable adapter (this happens after all the main
                // fieldProcessors have been processed)
                final Queue<SubtableData<JSONToStreamPublisherAdapter>> subtableProcessingQueue =
                        subtableProcessingQueueThreadLocal.get();
                subtableProcessingQueue.add(new SubtableData<>(subtableProcessingParameters, subtableFieldValue));
            }
        };

        fieldProcessors.add(fieldProcessor);

        // Add a fieldSetter that updates a column in the row in the parent table with the subtable row ID
        final int subtableRowIdColIdx = colNameToColIdx.get(subtableRowIdColNameInParent);

        final Consumer<InMemoryRowHolder> fieldSetterParent =
                (InMemoryRowHolder holder) -> chunks[subtableRowIdColIdx].asWritableLongChunk()
                        .add(holder.getLong(position.intValue()));
        putChunkPopulator(subtableRowIdColNameInParent, fieldSetterParent);
    }

    private void makeFunctionFieldProcessor(
            final String columnName,
            final Class<?> returnType,
            final Function<JsonNode, ?> function) {
        final int colIdx = colNameToColIdx.get(columnName);
        final Class<?> colType = colNameToType.get(columnName);

        if (!colType.isAssignableFrom(returnType)) {
            throw new JSONIngesterException("Column " + columnName + " is of type " + colType
                    + ", can not assign function of type: " + returnType);
        }
        final Consumer<InMemoryRowHolder> fieldSetter;
        final ObjIntConsumer<JsonNode> fieldConsumer;
        final MutableInt position = new MutableInt(0);

        if (colType == boolean.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setBoolean(TypeUtils.unbox((Boolean) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableBooleanChunk()
                    .add(holder.getBoolean(position.intValue()));
        } else if (colType == Boolean.class) {
            // Note that Booleans are stored as bytes. See io.deephaven.util.BooleanUtils.
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setBoolean((Boolean) function.apply(record));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk()
                    .add(holder.getByte(position.intValue()));
        } else if (colType == char.class || colType == Character.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setChar(TypeUtils.unbox((Character) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableCharChunk()
                    .add(holder.getChar(position.intValue()));
        } else if (colType == byte.class || colType == Byte.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setByte(TypeUtils.unbox((Byte) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk()
                    .add(holder.getByte(position.intValue()));
        } else if (colType == short.class || colType == Short.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setShort(TypeUtils.unbox((Short) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableShortChunk()
                    .add(holder.getShort(position.intValue()));
        } else if (colType == int.class || colType == Integer.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setInt(TypeUtils.unbox((Integer) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableIntChunk()
                    .add(holder.getInt(position.intValue()));
        } else if (colType == long.class || colType == Long.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setLong(TypeUtils.unbox((Long) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk()
                    .add(holder.getLong(position.intValue()));
        } else if (colType == float.class || colType == Float.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setFloat(TypeUtils.unbox((Float) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableFloatChunk()
                    .add(holder.getFloat(position.intValue()));
        } else if (colType == double.class || colType == Double.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).setDouble(TypeUtils.unbox((Double) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableDoubleChunk()
                    .add(holder.getDouble(position.intValue()));
        } else {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    returnType, position, holderNumber).set(function.apply(record));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableObjectChunk()
                    .add(holder.getObject(position.intValue()));
        }
        fieldProcessors.add(fieldConsumer);
        putChunkPopulator(columnName, fieldSetter);
    }

    private void makeIntFunctionFieldProcessor(
            final String columnName,
            final ToIntFunction<JsonNode> function) {
        final int colIdx = colNameToColIdx.get(columnName);
        final Class<?> colType = colNameToType.get(columnName);

        if (colType != int.class) {
            throw new JSONIngesterException(
                    "Column " + columnName + " is of type " + colType + ", can not assign ToIntFunction.");
        }

        final MutableInt position = new MutableInt();
        final ObjIntConsumer<JsonNode> fieldConsumer = (JsonNode record,
                int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, int.class, position, holderNumber)
                        .setInt(function.applyAsInt(record));
        final Consumer<InMemoryRowHolder> fieldProcessor =
                (InMemoryRowHolder holder) -> chunks[colIdx].asWritableIntChunk()
                        .add(holder.getInt(position.intValue()));
        fieldProcessors.add(fieldConsumer);
        putChunkPopulator(columnName, fieldProcessor);
    }

    private void makeLongFunctionFieldProcessor(
            final String columnName,
            final ToLongFunction<JsonNode> function) {
        final int colIdx = colNameToColIdx.get(columnName);
        final Class<?> colType = colNameToType.get(columnName);

        if (colType != long.class) {
            throw new JSONIngesterException(
                    "Column " + columnName + " is of type " + colType + ", can not assign ToLongFunction.");
        }

        final MutableInt position = new MutableInt();
        final ObjIntConsumer<JsonNode> fieldConsumer =
                (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, long.class,
                        position, holderNumber).setLong(function.applyAsLong(record));
        final Consumer<InMemoryRowHolder> fieldProcessor =
                (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk()
                        .add(holder.getLong(position.intValue()));
        fieldProcessors.add(fieldConsumer);
        putChunkPopulator(columnName, fieldProcessor);
    }

    private void makeDoubleFunctionFieldProcessor(
            final String columnName,
            final ToDoubleFunction<JsonNode> function) {
        final int colIdx = colNameToColIdx.get(columnName);
        final Class<?> colType = colNameToType.get(columnName);

        if (colType != double.class) {
            throw new JSONIngesterException(
                    "Column " + columnName + " is of type " + colType + ", can not assign ToDoubleFunction.");
        }

        final MutableInt position = new MutableInt();
        final ObjIntConsumer<JsonNode> fieldConsumer =
                (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, double.class,
                        position, holderNumber).setDouble(function.applyAsDouble(record));
        final Consumer<InMemoryRowHolder> fieldProcessor =
                (InMemoryRowHolder holder) -> chunks[colIdx].asWritableDoubleChunk()
                        .add(holder.getDouble(position.intValue()));
        fieldProcessors.add(fieldConsumer);
        putChunkPopulator(columnName, fieldProcessor);
    }

    private void makeFieldProcessors(final String columnName,
            final String fieldName) {
        final Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> p =
                makeFieldProcessorAndSetter(columnName, fieldName);
        fieldProcessors.add(p.first);
        putChunkPopulator(columnName, p.second);
    }

    private void makeJsonPointerFieldProcessor(
            final String columnName,
            final JsonPointer jsonPointer) {

        if (!nestedAdapters.isEmpty()) {
            throw new IllegalStateException("Nested adapter cannot be created before JsonPointers are processed");
        }


        if (jsonPointer.matches()) {
            final Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> p =
                    makeFieldProcessorAndSetterNode(columnName);
            fieldProcessors.add(p.first);
            putChunkPopulator(columnName, p.second);
            return;
        }

        // If the head of the pointer is a field name, then the pointer will be processed by a nested field adapter
        // for that field, we don't need to add a regular fieldProcessor here. (A nested adapter later will handle
        // it.)
        final String jsonPointerTopField = jsonPointer.getMatchingProperty();
        if (jsonPointerTopField != null && !jsonPointerTopField.isEmpty()) {
            nestedFieldToJsonPointers.computeIfAbsent(
                    jsonPointerTopField,
                    s -> new HashMap<>())
                    .put(columnName, jsonPointer.tail());

            return;
        }

        final Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> p =
                makeJsonPointerFieldProcessorAndSetter(columnName, jsonPointer);
        fieldProcessors.add(p.first);
        putChunkPopulator(columnName, p.second);
    }

    private Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> makeFieldProcessorAndSetter(
            final String columnName,
            final String fieldName) {
        final int colIdx = colNameToColIdx.get(columnName);
        final Class<?> colType = colNameToType.get(columnName);

        final Consumer<InMemoryRowHolder> fieldSetter;
        final ObjIntConsumer<JsonNode> fieldConsumer;
        final MutableInt position = new MutableInt();
        // TODO: how does this work with threading? aren't the consumers run by multiple threads at once?
        /*
         * TODO: Why use these MutableInts? The position of each field in the InMemoryRowHolders is fixed anyway
         * (otherwise, parallel parsing never would have worked in the first place). And the InMemoryRowHolder fields
         * are presumably 1:1 with output columns, excluding the post-processing 'nowTime' populateNowTimeChunk().
         * 
         * That also suggest that the InMemoryRowHolder could have properly-sized type-specific arrays, instead of
         * sticking everything into an Object[]
         */
        if (colType == boolean.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setBoolean(
                                    JsonNodeUtil.getBoolean(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableBooleanChunk()
                    .add(holder.getBoolean(position.intValue()));
        } else if (colType == Boolean.class) {
            // Note that Booleans are stored as bytes. See io.deephaven.util.BooleanUtils.
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setBoolean(
                                    JsonNodeUtil.getBoolean(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk()
                    .add(holder.getByte(position.intValue()));
        } else if (colType == char.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setChar(
                                    JsonNodeUtil.getChar(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableCharChunk()
                    .add(holder.getChar(position.intValue()));
        } else if (colType == byte.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setByte(
                                    JsonNodeUtil.getByte(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk()
                    .add(holder.getByte(position.intValue()));
        } else if (colType == short.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setShort(
                                    JsonNodeUtil.getShort(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableShortChunk()
                    .add(holder.getShort(position.intValue()));
        } else if (colType == int.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber)
                            .setInt(JsonNodeUtil.getInt(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableIntChunk()
                    .add(holder.getInt(position.intValue()));
        } else if (colType == long.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setLong(
                                    JsonNodeUtil.getLong(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk()
                    .add(holder.getLong(position.intValue()));
        } else if (colType == float.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setFloat(
                                    JsonNodeUtil.getFloat(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableFloatChunk()
                    .add(holder.getFloat(position.intValue()));
        } else if (colType == double.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setDouble(
                                    JsonNodeUtil.getDouble(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableDoubleChunk()
                    .add(holder.getDouble(position.intValue()));
        } else if (colType == String.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber)
                            .set(JsonNodeUtil.getString(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableObjectChunk()
                    .add(holder.getObject(position.intValue()));
        } else if (colType == Instant.class) {
            // Note that Instants are stored as longs
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).set(
                                    JsonNodeUtil.getInstant(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk()
                    .add(holder.getLong(position.intValue()));
        } else {
            throw new UnsupportedOperationException("Can not convert JSON field to " + colType + " for column "
                    + columnName + " (field " + fieldName + ")");
        }
        return new Pair<>(fieldConsumer, fieldSetter);
    }

    private void addNullFieldSetter(final String columnName) {
        final int colIdx = colNameToColIdx.get(columnName);
        final Class<?> colType = colNameToType.get(columnName);

        final Consumer<InMemoryRowHolder> fieldSetter;
        if (colType == boolean.class) {
            throw new UnsupportedOperationException("Cannot populate primitive boolean column with nulls!");
        } else if (colType == Boolean.class) {
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk()
                    .add(BooleanUtils.booleanAsByte(null));
        } else if (colType == char.class) {
            fieldSetter =
                    (InMemoryRowHolder holder) -> chunks[colIdx].asWritableCharChunk().add(QueryConstants.NULL_CHAR);
        } else if (colType == byte.class) {
            fieldSetter =
                    (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk().add(QueryConstants.NULL_BYTE);
        } else if (colType == short.class) {
            fieldSetter =
                    (InMemoryRowHolder holder) -> chunks[colIdx].asWritableShortChunk().add(QueryConstants.NULL_SHORT);
        } else if (colType == int.class) {
            fieldSetter =
                    (InMemoryRowHolder holder) -> chunks[colIdx].asWritableIntChunk().add(QueryConstants.NULL_INT);
        } else if (colType == long.class) {
            fieldSetter =
                    (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk().add(QueryConstants.NULL_LONG);
        } else if (colType == float.class) {
            fieldSetter =
                    (InMemoryRowHolder holder) -> chunks[colIdx].asWritableFloatChunk().add(QueryConstants.NULL_FLOAT);
        } else if (colType == double.class) {
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableDoubleChunk()
                    .add(QueryConstants.NULL_DOUBLE);
        } else {
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableObjectChunk().add(null);
        }

        putChunkPopulator(columnName, fieldSetter);
    }

    private Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> makeFieldProcessorAndSetterNode(
            final String columnName) {
        final int colIdx = colNameToColIdx.get(columnName);
        final Class<?> colType = colNameToType.get(columnName);

        final Consumer<InMemoryRowHolder> fieldSetter;
        final ObjIntConsumer<JsonNode> fieldConsumer;
        final MutableInt position = new MutableInt();
        if (colType == boolean.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setBoolean(JsonNodeUtil.getBoolean(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableBooleanChunk()
                    .add(holder.getBoolean(position.intValue()));
        } else if (colType == Boolean.class) {
            // Note that Booleans are stored as bytes. See io.deephaven.util.BooleanUtils.
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setBoolean(JsonNodeUtil.getBoolean(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk()
                    .add(holder.getByte(position.intValue()));
        } else if (colType == char.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setChar(JsonNodeUtil.getChar(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableCharChunk()
                    .add(holder.getChar(position.intValue()));
        } else if (colType == byte.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setByte(JsonNodeUtil.getByte(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk()
                    .add(holder.getByte(position.intValue()));
        } else if (colType == short.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setShort(JsonNodeUtil.getShort(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableShortChunk()
                    .add(holder.getShort(position.intValue()));
        } else if (colType == int.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setInt(JsonNodeUtil.getInt(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableIntChunk()
                    .add(holder.getInt(position.intValue()));
        } else if (colType == long.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setLong(JsonNodeUtil.getLong(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk()
                    .add(holder.getLong(position.intValue()));
        } else if (colType == float.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setFloat(JsonNodeUtil.getFloat(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableFloatChunk()
                    .add(holder.getFloat(position.intValue()));
        } else if (colType == double.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).setDouble(JsonNodeUtil.getDouble(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableDoubleChunk()
                    .add(holder.getDouble(position.intValue()));
        } else if (colType == String.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).set(JsonNodeUtil.getString(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableObjectChunk()
                    .add(holder.getObject(position.intValue()));
        } else if (colType == Instant.class) {
            // Note that Instants are stored as longs
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName,
                    colType, position, holderNumber).set(JsonNodeUtil.getInstant(node));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk()
                    .add(holder.getLong(position.intValue()));
        } else {
            throw new UnsupportedOperationException(
                    "Can not convert JSON field to " + colType + " for column " + columnName);
        }
        return new Pair<>(fieldConsumer, fieldSetter);
    }

    private Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> makeJsonPointerFieldProcessorAndSetter(
            final String columnName,
            final JsonPointer jsonPointer) {
        final int colIdx = colNameToColIdx.get(columnName);
        final Class<?> colType = colNameToType.get(columnName);

        final Consumer<InMemoryRowHolder> fieldSetter;
        final ObjIntConsumer<JsonNode> fieldConsumer;
        final MutableInt position = new MutableInt();
        // TODO: how does this MutableInt work with threading? aren't the consumers run by multiple threads at once?
        if (colType == boolean.class || colType == Boolean.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setBoolean(
                                    JsonNodeUtil.getBoolean(record, jsonPointer, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableBooleanChunk()
                    .add(holder.getBoolean(position.intValue()));
        } else if (colType == char.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setChar(
                                    JsonNodeUtil.getChar(record, jsonPointer, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableCharChunk()
                    .add(holder.getChar(position.intValue()));
        } else if (colType == byte.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setByte(
                                    JsonNodeUtil.getByte(record, jsonPointer, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableByteChunk()
                    .add(holder.getByte(position.intValue()));
        } else if (colType == short.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setShort(
                                    JsonNodeUtil.getShort(record, jsonPointer, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableShortChunk()
                    .add(holder.getShort(position.intValue()));
        } else if (colType == int.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber)
                            .setInt(JsonNodeUtil.getInt(record, jsonPointer, allowMissingKeys,
                                    allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableIntChunk()
                    .add(holder.getInt(position.intValue()));
        } else if (colType == long.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setLong(
                                    JsonNodeUtil.getLong(record, jsonPointer, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk()
                    .add(holder.getLong(position.intValue()));
        } else if (colType == float.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setFloat(
                                    JsonNodeUtil.getFloat(record, jsonPointer, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableFloatChunk()
                    .add(holder.getFloat(position.intValue()));
        } else if (colType == double.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).setDouble(
                                    JsonNodeUtil.getDouble(record, jsonPointer, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableDoubleChunk()
                    .add(holder.getDouble(position.intValue()));
        } else if (colType == String.class) {
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber)
                            .set(JsonNodeUtil.getString(record, jsonPointer, allowMissingKeys,
                                    allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableObjectChunk()
                    .add(holder.getObject(position.intValue()));
        } else if (colType == Instant.class) {
            // Note that Instants are stored as longs
            fieldConsumer = (JsonNode record,
                    int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, colType, position,
                            holderNumber).set(
                                    JsonNodeUtil.getInstant(record, jsonPointer, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> chunks[colIdx].asWritableLongChunk()
                    .add(holder.getLong(position.intValue()));
        } else {
            throw new UnsupportedOperationException("Can not convert JSON field to " + colType + " for column "
                    + columnName + " (pointer: " + jsonPointer + ")");
        }
        return new Pair<>(fieldConsumer, fieldSetter);
    }

    private void makeArrayFieldProcessors(
            final String columnName,
            final String fieldName) {
        final Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> p =
                makeFieldProcessorAndSetterNode(columnName);
        arrayFieldNames.add(fieldName);
        arrayFieldProcessors.add(p.first);
        chunkPopulators.put(columnName, p.second);
    }

    /**
     * Get a SingleRowSetter and capture the position within the relevant holder's data array for the object controlled
     * by that setter.
     *
     * @param columnName The name of the column being populated
     * @param setterType The class of the column being populated
     * @param position The AtomicInteger whose value will be set (this must be final to be used in the lambdas where
     *        this can be called)
     * @param holderNumber Which RowHolder this is operating on
     * @return The resultant SingleRowSetter from the RowHolder for this column.
     */
    @NotNull
    private InMemoryRowHolder.SingleRowSetter getSingleRowSetterAndCapturePosition(final String columnName,
            final Class<?> setterType, final MutableInt position, final int holderNumber) {
        final InMemoryRowHolder.SingleRowSetter str = holders[holderNumber].getSetter(columnName, setterType);
        position.setValue(str.getThisPosition());
        return str;
    }

    /**
     * Add the given {@code chunkPopulator} to the {@link #chunkPopulators} map. Throws an exception if a chunk
     * populator for the given {@code columnName} is already present.
     *
     * @param columnName The name of the column to add a chunk populator for.
     * @param chunkPopulator The chunk populator (i.e. a Consumer that populates a chunk with data from a
     *        {@link InMemoryRowHolder}).
     */
    private void putChunkPopulator(@NotNull final String columnName,
            @NotNull final Consumer<InMemoryRowHolder> chunkPopulator) {
        final Consumer<InMemoryRowHolder> existingChunkPopuplator = chunkPopulators.put(columnName, chunkPopulator);
        if (existingChunkPopuplator != null) {
            final int colIdx = colNameToColIdx.get(columnName);
            throw new IllegalStateException(
                    "Already had a chunk populator for column " + columnName + " (colIdx: " + colIdx + ')');
        }
    }

    /**
     * Recursively build a list of subtable adapters, from the given {@code adapter} and all its children. All of these
     * must be {@link AsynchronousDataIngester#cleanup() cleaned up} after rows are written.
     *
     * @return A list of adapters for all subtables
     */
    private static List<AsynchronousDataIngester> getAllSubtableAdapters(JSONToStreamPublisherAdapter adapter) {
        final List<AsynchronousDataIngester> subtableAdatpers =
                new ArrayList<>(adapter.subtableFieldsToAdapters.values());
        for (JSONToStreamPublisherAdapter nestedAdapter : adapter.nestedAdapters) {
            subtableAdatpers.addAll(getAllSubtableAdapters(nestedAdapter));
        }
        return Collections.unmodifiableList(subtableAdatpers);
    }

    /**
     * Consumes a string and wraps it into a {@link TextMessage} with its timestamps and msgId set automatically.
     *
     * @param json The input JSON string
     * @return The {@link BaseMessageMetadata#getMsgNo() message number} assigned to the message. This is automatically
     *         set to the current value of {@link #messagesQueued}.
     */
    public synchronized long consumeString(final String json) {
        long msgId = messagesQueued.get();
        final Instant now = Instant.now();
        final TextJsonMessage msg = new TextJsonMessage(now, now, now, null, msgId, json);
        consumeJson(msg);
        return msgId;
    }

    /**
     * Wraps an InputStream into a {@link StreamJsonMessage} with its timestamps and msgId set automatically.
     *
     * @param json The input JSON string
     * @return The {@link BaseMessageMetadata#getMsgNo() message number} assigned to the message. This is automatically
     *         set to the current value of {@link #messagesQueued}.
     */
    public synchronized long consumeStream(final InputStream json, final Runnable afterParse) {
        long msgId = messagesQueued.get();
        final Instant now = Instant.now();
        final StreamJsonMessage msg = new StreamJsonMessage(now, now, now, null, msgId, json, afterParse);
        consumeJson(msg);
        return msgId;
    }

    @Override
    public void consumeString(@NotNull final TextMessage msg) {
        consumeJson(new JsonMessage() {
            @Override
            public JsonNode getJson() throws JsonNodeUtil.JsonStringParseException {
                return JsonNodeUtil.makeJsonNode(msg.getText());

            }

            @Override
            public String getOriginalText() {
                return msg.getText();
            }

            @Override
            public Instant getSentTime() {
                return msg.getSentTime();
            }

            @Override
            public Instant getReceiveTime() {
                return msg.getReceiveTime();
            }

            @Override
            public Instant getIngestTime() {
                return msg.getIngestTime();
            }

            @Override
            public String getMessageId() {
                return msg.getMessageId();
            }

            @Override
            public long getMsgNo() {
                return msg.getMsgNo();
            }
        });
    }

    public long consumeStream(final InputStream json) {
        return consumeStream(json, null);
    }

    public void consumeJson(@NotNull final JsonMessage msg) {
        if (isShutdown.get()) {
            throw new IllegalStateException("Message received after adapter shutdown!");
        }
        if (numThreads == 0) {
            // process synchronously
            processSingleMessage(0, msg);

            // note: cleanup() must be run manually
            return;
        }
        final long queuedMessages = messagesQueued.getAndIncrement();

        // ensure no messages have been skipped
        // TODO: is this check overkill?
        // the MsgNo is used (1) to ensure messages received from an external source are enqueued in the order in which
        // they are received, and (2) to ensure that processed messages are written in the same order in which they were
        // enqueued when parallel processing is enabled.
        // arguably (1) is the job of the caller, and this adapter should only be responsible for (2), in which case
        // it should set a sequence number itself.
        if (queuedMessages != msg.getMsgNo()) {
            throw new IllegalStateException("Unexpected message number " + msg.getMsgNo()
                    + ", previously queued messages " + queuedMessages);
        }
        waitingMessages.add(msg);
    }

    @Override
    public void cleanup() throws IOException {
        synchronized (pendingCleanup) {
            final long beforePoll = System.nanoTime();
            final long intervalMessages = processedMessages.size();
            log.debug().append("JSONToStreamPublisherAdapter cleanup: Cleanup called with ").append(intervalMessages)
                    .append(" messages to clean up.").endl();
            if (intervalMessages > 0) {
                int cleanedMessages = 0;
                int badMessages = 0;
                processedMessages.drainTo(pendingCleanup);
                pendingCleanup.sort(Comparator.comparingLong(InMemoryRowHolder::getMessageNumber));

                if (!pendingCleanup.isEmpty()) {
                    final long firstMessageNumber = pendingCleanup.get(0).getMessageNumber();
                    log.debug().append("JSONToStreamPublisherAdapter cleanup: Cleaning up starting with ")
                            .append(firstMessageNumber).endl();
                    if (firstMessageNumber < nextMsgNo.longValue()) {
                        throw new IllegalStateException("Unexpected back-in-time message: " + firstMessageNumber
                                + " is less than " + nextMsgNo);
                    }
                }

                // lock the streamPublisher until we finish this round of cleanup. TODO: does this make transactions
                // work?
                synchronized (streamPublisher) {
                    while (!pendingCleanup.isEmpty()
                            && cleanedMessages < pendingCleanup.size()
                            && pendingCleanup.get(cleanedMessages).getMessageNumber() == nextMsgNo.get()) {
                        final InMemoryRowHolder finalHolder = pendingCleanup.get(cleanedMessages);
                        if (finalHolder.getParseException() != null) {
                            // there was a parsing exception for this message; we should log the error and proceed to
                            // the next message
                            if (unparseableMessagesLogged++ < MAX_UNPARSEABLE_LOG_MESSAGES) {
                                final String origMsgTxt = finalHolder.getOriginalText();
                                final String origTxtFormatted =
                                        origMsgTxt == null ? "<original message text unavailable>"
                                                : "\"" + origMsgTxt + "\": ";
                                log.error()
                                        .append("Unable to parse JSON message #")
                                        .append(finalHolder.getMessageNumber())
                                        .append(": ")
                                        .append(origTxtFormatted)
                                        .nl()
                                        .append(finalHolder.getParseException()).endl();
                            }
                            if (!skipUnparseableMessages) {
                                throw new JSONIngesterException("Unable to parse JSON message",
                                        finalHolder.getParseException());
                            }
                            nextMsgNo.incrementAndGet();
                        } else {
                            if (!finalHolder.getIsEmpty()) {
                                //@formatter:off
                            /*
                            TODO: This is an awkward/fragile aspect of StreamToBlinkTableAdapter -- the chunks
                                ***must not be initialized*** until you are certain you are going to add data to them.
                                Otherwise, the UpdateGraph crashes in StreamToBlinkTableAdapter.doRefresh() while calling
                                StreamPublisher.flush(), because StreamPublisher.flush() will try to append the
                                empty chunks to a column source, which is not allowed (and triggers an assertion
                                failure, e.g. in ObjectChunkColumnSource.addChunk)

                                In general, StreamPublisher is tricky to use, at least in this case because...

                                1) The thing that publishes data is directly responsible for filling chunks, and has to
                                track on its own when those chunks are full and flush() them
                                2) The UpdateGraph also flushes data (StreamToBlinkTableAdapter.doRefresh())
                                3) You don't actually have a Table until you create the StreamToBlinkTableAdapter
                                4) You can get yourself in trouble if you don't append to all chunks (can't just skip a
                                   column and let it  be null). Leads to excessive checking, like io.deephaven.kafka.ingest.KafkaStreamPublisher.checkChunkSizes
                                5) (TODO) Who/what is responsible for closing chunks???
                             */
                            //@formatter:on

                                ensureChunksInitialized();
                                Assert.gtZero(streamPublisher.getChunkRemainingSpace(),
                                        "streamPublisher.getChunkRemainingSpace()");

                                // First, set all the fields for which there is a 1-1 message/row ratio.
                                chunkPopulators.values().forEach(fp -> fp.accept(finalHolder));

                                // Then run cleanup() for any subtable adapters before, we commit this row
                                // (The parent row has IDs referring to subtable rows, so subtable rows must be written
                                // first to ensure that anything querying the parent table can refer to the subtable
                                // data.)
                                for (AsynchronousDataIngester subtableAdapter : allSubtableAdapters) {
                                    subtableAdapter.cleanup();
                                }

                                if (!isSubtableAdapter) {
                                    // subtable adapters don't handle message metadata -- only the top adapter
                                    populateNowTimeChunk();
                                }


                                /*
                                 * TODO: how should we replace this in StreamPublisher world? we could do something
                                 * similar to DynamicTableWriter.DynamicTableRow.writeRow, where rows from an incomplete
                                 * transaction are essentially ignored until the transaction is complete, but we're not
                                 * set up to do that because StreamPublisherImpl.flush() turns over the entire existing
                                 * chunks to the table
                                 *
                                 * we could also StreamPublisherImpl.flush()/StreamPublisherImpl.getChunks() before and
                                 * after every transaction, but that could get expensive if there are lots of
                                 * transactions (e.g. from subtables or expanded arrays)
                                 *
                                 *
                                 */
                                // TODO: what do we do here instead??
                                // tableWriter.setFlags(finalHolder.getFlags());
                                // tableWriter.writeRow();
                                // tableWriter.flush();

                                if (!isNestedAdapter) {
                                    // If this is the outermost parent adapter, then decrement the remaining space
                                    // in the publisher's chunks.
                                    if (streamPublisher.decrementRemainingSpace(1) == 0) {
                                        streamPublisher.flush(); // TODO: this throws away transactionality?
                                        // The chunks we just flushed are no longer usable, so null-out references to
                                        // them,
                                        // including for nested adapters (to help prevent writing to the wrong chunks;
                                        // NPEs will be thrown instead).
                                        setChunks(null);
                                    }
                                }
                            }

                            if (finalHolder.getFlags() == Row.Flags.EndTransaction
                                    || finalHolder.getFlags() == Row.Flags.SingleRow) {
                                nextMsgNo.incrementAndGet();
                            }
                        }
                        cleanedMessages++;

                    }
                }
                // Compact the array. The next message in the sequence has not been processed yet,
                // we will try again shortly
                if (cleanedMessages > 0) {
                    // Note that the toIndex of removeRange is exclusive, so (0,1) will remove only
                    // the message at position 0.
                    pendingCleanup.removeRange(0, cleanedMessages);

                    /*
                     * Don't flush here; there is no real benefit when just let the UpdateGraph do it on its own time.
                     */
                    // streamPublisher.flush();
                }

                final long afterPoll = System.nanoTime();
                final long intervalNanos = afterPoll - beforePoll;
                log.debug().append("JSONToStreamPublisherAdapter cleanup - flushed ")
                        .append(cleanedMessages - badMessages)
                        .append(" in ").append(intervalNanos / 1000_000L).append("ms, ")
                        .appendDouble(1000000000.0 * intervalMessages / intervalNanos, 4)
                        .append(" msgs/sec, remaining pending messages=").append(pendingCleanup.size())
                        .append(", messages with errors=").append(badMessages).endl();
            }
        }
    }

    /**
     * Close the table writer for this adapter and any subtable adapters.
     *
     * @throws IOException If a table writer cannot be closed
     */
    public void closePublishers() throws IOException {
        log.debug().append("JSONToStreamPublisherAdapter closePublishers - closing stream publishers").endl();

        log.debug().append("JSONToStreamPublisherAdapter closePublishers - shutting down subtable stream publishers")
                .endl();
        for (JSONToStreamPublisherAdapter jsonToStreamPublisherAdapter : subtableFieldsToAdapters.values()) {
            jsonToStreamPublisherAdapter.closePublishers();
        }

        log.debug().append("JSONToStreamPublisherAdapter closePublishers - closing local stream publishers").endl();
        streamPublisher.shutdown();
    }

    private void populateNowTimeChunk() {
        if (nowTimeColIdx >= 0) {
            chunks[nowTimeColIdx].asWritableLongChunk().add(Clock.system().currentTimeNanos());
        }
    }

    /**
     * Shuts down the adapter and waits for consumer threads to exit.
     */
    @Override
    public void shutdown() {
        log.info().append("JSONToStreamPublisherAdapter shutdown").endl();
        final boolean wasAlreadyShutdown = !isShutdown.compareAndSet(false, true);
        if (wasAlreadyShutdown) {
            // just log an exception - doesn't really matter
            final IllegalStateException logOnlyException = new IllegalStateException("Already shut down.");
            log.warn().append("JSONToStreamPublisherAdapter shutdown: already shut down.")
                    .nl()
                    .append(logOnlyException)
                    .endl();
            return;
        } else {
            deregisterShutdownTask();

            // Shut down the adapters
            log.debug().append("JSONToStreamPublisherAdapter shutdown - shutting down nested adapters").endl();
            nestedAdapters.forEach(JSONToStreamPublisherAdapter::shutdown);

            log.debug().append("JSONToStreamPublisherAdapter shutdown - shutting down subtable adapters").endl();
            subtableFieldsToAdapters.values().forEach(JSONToStreamPublisherAdapter::shutdown);
        }

        // If using threads, wait for the consumer threads to finish:
        if (consumerThreadsCountDownLatch != null) {
            try {
                log.debug().append("JSONToStreamPublisherAdapter shutdown - awaiting termination").endl();
                if (!consumerThreadsCountDownLatch.await(SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS)) {
                    throw new JSONIngesterException("Timed out while awaiting shutdown! " + getThreadingStatusStr());
                }
            } catch (InterruptedException ex) {
                throw new JSONIngesterException("Interrupted while awaiting shutdown! " + getThreadingStatusStr(), ex);
            }
        }

        log.debug().append("JSONToStreamPublisherAdapter shutdown - complete").endl();
    }

    @Override
    public void setOwner(@Nullable final StringMessageToBlinkTableAdapter<?> parent) {
        if (owner != null) {
            throw new IllegalStateException("owner already set!");
        }

        this.owner = Require.neqNull(parent, "parent");

        final String messageIdColumn = owner.getMessageIdColumn();
        final String sendTimeColumn = owner.getSendTimeColumn();
        final String receiveTimeColumn = owner.getReceiveTimeColumn();
        final String nowTimeColumn = owner.getNowTimeColumn();

        if (messageIdColumn != null) {
            final int colIdx = colNameToColIdx.get(messageIdColumn);
            final Class<?> colType = colNameToType.get(messageIdColumn);
            Assert.equals(colType, "colType", String.class);
            final Consumer<InMemoryRowHolder> chunkPopulator = (InMemoryRowHolder holder) -> chunks[colIdx]
                    .asWritableObjectChunk().add(holder.getObject(messageIdColumn));
            chunkPopulators.put(messageIdColumn, chunkPopulator);
        }
        if (sendTimeColumn != null) {
            final int colIdx = colNameToColIdx.get(sendTimeColumn);
            final Class<?> colType = colNameToType.get(sendTimeColumn);
            Assert.equals(colType, "colType", Instant.class);
            final Consumer<InMemoryRowHolder> chunkPopulator = (InMemoryRowHolder holder) -> chunks[colIdx]
                    .asWritableLongChunk().add(holder.getLong(sendTimeColumn));
            chunkPopulators.put(sendTimeColumn, chunkPopulator);
        }
        if (receiveTimeColumn != null) {
            final int colIdx = colNameToColIdx.get(receiveTimeColumn);
            final Class<?> colType = colNameToType.get(receiveTimeColumn);
            Assert.equals(colType, "colType", Instant.class);
            final Consumer<InMemoryRowHolder> chunkPopulator = (InMemoryRowHolder holder) -> chunks[colIdx]
                    .asWritableLongChunk().add(holder.getLong(receiveTimeColumn));
            chunkPopulators.put(receiveTimeColumn, chunkPopulator);
        }

        if (nowTimeColumn != null) {
            // Just remove the existing null chunkpopulator (from JSONToStreamPublisherAdapter.addNullFieldSetter())
            // and set the nowTimeColIdx. Then populateNowTimeChunk() can add the time to the chunk after the row is
            // processed.
            chunkPopulators.remove(nowTimeColumn);
            nowTimeColIdx = colNameToColIdx.get(nowTimeColumn);
            final Class<?> colType = colNameToType.get(nowTimeColumn);
            Assert.equals(colType, "colType", Instant.class);
        }
    }

    private String getThreadingStatusStr() {
        return "(Threads still running: "
                + consumerThreadsCountDownLatch.getCount() +
                ", messagesQueued=" + messagesQueued.get() +
                ", messagesProcessed=" + messagesProcessed.longValue() +
                ')';
    }

    private class ConsumerThread extends Thread {
        private final int holderNum;

        public ConsumerThread(final ThreadGroup threadGroup, final int instanceId, final int holderNum) {
            super(threadGroup,
                    JSONToStreamPublisherAdapter.class.getSimpleName() + instanceId + "_ConsumerThread-" + holderNum);
            this.holderNum = holderNum;
        }

        @Override
        public void run() {
            try {
                processMessages(holderNum);
            } finally {
                consumerThreadsCountDownLatch.countDown();
            }
        }
    }

    private void processMessages(final int thisHolder) {
        // Continue processing messages until the adapter is shut down and all waitingMessages have been processed
        boolean isShutdown;
        while (!(isShutdown = this.isShutdown.get()) || !waitingMessages.isEmpty()) {
            try {
                final int pollWaitMillis = isShutdown ? 0 : CONSUMER_WAIT_INTERVAL_MS;
                pollOnce(thisHolder, Duration.ofNanos(pollWaitMillis * NANOS_PER_MILLI));
            } catch (final Exception ioe) {
                /*
                 * This thread doesn't inherently cause the query to stop if it fails, so we want to stop everything on
                 * any error.
                 */

                // new exception to improve reported stack trace:
                final Exception newException = new JSONIngesterException("Error processing JSON message!", ioe);

                log.error().append("Error processing JSON message: ").append(newException).endl();
                ProcessEnvironment.getGlobalFatalErrorReporter().report("Error processing JSON message!", newException);
                System.exit(ERROR_PROCESSING); // Make the worker stop if we failed.
            }
            try {
                final long afterPoll = System.nanoTime();
                synchronized (messagesProcessed) {
                    if (afterPoll > nextReportTime) {
                        final long intervalMessages = messagesProcessed.longValue() - lastProcessed;
                        final long intervalNanos = afterPoll - lastReportNanos;
                        if (intervalMessages > 0) {
                            log.info().append("JSONToStreamPublisherAdapter: Processed ").append(intervalMessages)
                                    .append(" in ")
                                    .append(intervalNanos / NANOS_PER_MILLI).append("ms, ")
                                    .appendDouble(1_000_000_000.0 * intervalMessages / intervalNanos, 6)
                                    .append(" msgs/sec").endl();
                        } else {
                            log.debug().append("Processed 0 messages in last").append(intervalNanos / NANOS_PER_MILLI)
                                    .append("ms").endl();
                        }
                        lastReportNanos = afterPoll;
                        lastProcessed = messagesProcessed.longValue();
                        before = lastReportNanos;
                        nextReportTime = before + reportIntervalNanos;
                    }
                }
            } catch (final Exception e) {
                final Exception newException = new JSONIngesterException("Error reporting on message timing!", e);
                log.error().append("Error reporting on message timing: ").append(newException).endl();
                ProcessEnvironment.getGlobalFatalErrorReporter().report("Error reporting JSON message timing!",
                        newException);
                System.exit(ERROR_REPORTING);
            }
        }
        log.debug().append(Thread.currentThread().getName()).append(" - processMessages - complete").endl();
    }

    @SuppressWarnings("RedundantThrows")
    private void pollOnce(final int holder, final Duration timeout) throws IOException, JSONIngesterException {
        final JsonMessage msgData;
        synchronized (waitingMessages) {
            try {
                msgData = waitingMessages.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
            } catch (final InterruptedException ie) {
                // Do nothing; it's okay to not have received any messages.
                return;
            }
            if (msgData == null) {
                return;
            }
        }
        processSingleMessage(holder, msgData);
    }

    /**
     * Process one message, using the specified message holder.
     *
     * @param holder Index of the message holder into which the {@code msgData} should be processed.
     * @param msg The message.
     */
    private void processSingleMessage(final int holder, @NotNull final JsonMessage msg) {
        final long messageNumber = msg.getMsgNo();

        // The JSON parsing is time-consuming, so we can multi-thread that.
        final JsonNode record;
        try {
            record = msg.getJson();
        } catch (final JsonNodeUtil.JsonStringParseException parseException) {
            // handle JSON parse exception and keep going
            processExceptionRecord(holder, messageNumber, parseException, msg, msg.getOriginalText());
            return;
        }
        processMsgJsonContent(this, record, holder, msg, messageNumber);
    }

    private static void processMsgJsonContent(@NotNull final JSONToStreamPublisherAdapter adapter,
            @NotNull final JsonNode msgJson,
            final int holderIdx,
            @Nullable final MessageMetadata msgMetadata,
            final long messageNumber) {
        if (adapter.processArrays && msgJson.isArray()) {
            if (msgJson.isEmpty()) {
                // A holder must be processed for each value of 'messageNumber'! Gaps are not allowed.
                // This is because cleanup() will hang if it thinks there is a gap.
                adapter.addEmptyHolder(holderIdx);
            } else {
                final Iterator<JsonNode> iterator = msgJson.iterator();
                boolean isFirst = true;
                boolean isLast = !iterator.hasNext();
                while (!isLast) {
                    adapter.holders[holderIdx].setMessageNumber(messageNumber);

                    final JsonNode nextJson = iterator.next();
                    isLast = !iterator.hasNext();
                    adapter.processOneRecordTopLevel(holderIdx, msgMetadata, nextJson, isFirst, isLast);
                    isFirst = false;
                }
            }
        } else {
            adapter.holders[holderIdx].setMessageNumber(messageNumber);
            adapter.processOneRecordTopLevel(holderIdx, msgMetadata, msgJson, true, true);
        }
    }

    /**
     * Handle an exception while processing a message.
     *
     * @param holder Index of the holder into which the exception should be processed
     * @param msgNumber The message's sequence number
     * @param iae The exception the occurred while processing the message
     * @param msgData The message metadata
     * @param messageText The message text, if available
     */
    private void processExceptionRecord(final int holder,
            final long msgNumber,
            @NotNull final IllegalArgumentException iae,
            @NotNull final MessageMetadata msgData,
            @Nullable final String messageText) {
        holders[holder].setMessageNumber(msgNumber);
        holders[holder].setParseException(iae);
        holders[holder].setOriginalText(messageText);
        if (!isSubtableAdapter) {
            processMetadata(msgData, holder);
        }
        holders[holder].singleRow();
        processHolder(holder, true);
    }

    private void processOneRecordTopLevel(final int holder,
            @Nullable final MessageMetadata msgData,
            @Nullable final JsonNode record,
            final boolean isFirst, final boolean isLast) {

        // process the row so that it's ready when our turn comes to write.
        fieldProcessors.forEach(fc -> fc.accept(record, holder));

        if (!isSubtableAdapter) {
            processMetadata(msgData, holder);
        }

        // Get current consumer thread's subtable processing queue
        final Queue<SubtableData<JSONToStreamPublisherAdapter>> subtableProcessingQueue =
                subtableProcessingQueueThreadLocal.get();

        for (SubtableData<JSONToStreamPublisherAdapter> subtableFieldToProcess =
                subtableProcessingQueue.poll(); subtableFieldToProcess != null; subtableFieldToProcess =
                        subtableProcessingQueue.poll()) {
            final SubtableProcessingParameters<JSONToStreamPublisherAdapter> subtableParameters =
                    subtableFieldToProcess.subtableParameters;
            final JsonNode subtableFieldValue = subtableFieldToProcess.subtableNode;
            final String subtableFieldName = subtableParameters.fieldName;
            final JSONToStreamPublisherAdapter subtableAdapter = subtableParameters.subtableAdapter;
            final AtomicLong subtableMessageCounter = subtableParameters.subtableMessageCounter;

            // The holder index is always 0. Subtable adapters have their own message holders, and are always
            // single-threaded. Since there is one holder per processing thread, that means just one holder.
            // (Multithreading in subtable adapters is not currently supported, since it would take some work to ensure
            // the subtable rows appear in the same order as the parent table rows, which is desirable).
            final int subtableHolderIdx = 0;

            if (subtableFieldValue == null || subtableFieldValue.isMissingNode()) {
                if (subtableParameters.subtableKeyAllowedMissing) {
                    continue;
                } else {
                    throw new JSONIngesterException(
                            "Subtable node \"" + subtableFieldName + "\" is missing but allowMissingKeys is false");
                }
            }

            if (subtableFieldValue.isNull()) {
                if (subtableParameters.subtableKeyAllowedNull) {
                    continue;
                } else {
                    throw new JSONIngesterException(
                            "Subtable node \"" + subtableFieldName + "\" is null but allowNullValues is false");
                }
            }

            if (subtableParameters.isArrayNodeExpected) {
                /* nested subtable (passing one field to another adapter) -- subtable node should be an array node */
                if (!(subtableFieldValue.isArray())) {
                    final String fieldType = subtableFieldValue.getClass().getName();
                    throw new JSONIngesterException(
                            "Expected array node for subtable field \"" + subtableFieldName + "\" but was "
                                    + fieldType);
                }
                final ArrayNode subtableArrNode = ((ArrayNode) subtableFieldValue);

                if (subtableArrNode.isEmpty()) {
                    // (Note that we don't need to add an empty holder if we don't increment the message counter)
                    continue;
                }

                /* The subtableMsgNo **must** be a monotonic increasing sequence number with no gaps. */
                final long subtableMsgNo = subtableMessageCounter.getAndIncrement();

                final Iterator<JsonNode> iterator = subtableArrNode.iterator();
                boolean isSubtableFirst = true;
                boolean isSubtableLast = !iterator.hasNext();
                while (!isSubtableLast) {
                    final JsonNode subtableRecord = iterator.next();
                    isSubtableLast = !iterator.hasNext();
                    processSingleSubtableRecord(subtableAdapter, subtableFieldName, subtableHolderIdx,
                            subtableMsgNo, subtableRecord, isSubtableFirst, isSubtableLast);
                    isSubtableFirst = false;
                }
            } else {
                /* The subtableMsgNo **must** be a monotonic increasing sequence number with no gaps. */
                final long thisSubtableMsgNo = subtableMessageCounter.getAndIncrement();
                processSingleSubtableRecord(subtableAdapter, subtableFieldName, subtableHolderIdx, thisSubtableMsgNo,
                        subtableFieldValue, true, true);
            }
        }

        // after performing all of the field processing for regular or simple nested fields, we process the array fields
        // the array fields are presumed to be parallel, and may in turn be nested fields. After processing each array
        // element across our record, we copy the beginning holder elements to a new holder, thus allowing us to expand
        // the non-array elements to all of the logged rows. Each set of rows from the same message is a transaction.
        if (!arrayFieldNames.isEmpty()) {
            if (record == null && !allowMissingKeys) {
                throw new JSONIngesterException("allowMissingKeys is false but array fields are missing: "
                        + Arrays.toString(arrayFieldNames.toArray(new String[0])));
            }
            final int nArrayFields = arrayFieldNames.size();
            final ArrayNode[] nodes = new ArrayNode[nArrayFields];
            int expectedLength = -1;
            String lengthFound = null;
            if (record != null) {
                for (int ii = 0; ii < nArrayFields; ++ii) {
                    final String fieldName = arrayFieldNames.get(ii);
                    final Object object = JsonNodeUtil.getValue(record, fieldName, true, true);
                    if (object == null) {
                        continue;
                    }
                    if (!(object instanceof ArrayNode)) {
                        throw new JSONIngesterException(
                                "Expected array node for " + fieldName + " but was " + object.getClass());
                    }
                    final ArrayNode arrayNode = (ArrayNode) object;
                    final int arrayLength = arrayNode.size();
                    if (expectedLength >= 0) {
                        if (expectedLength != arrayLength) {
                            throw new JSONIngesterException(
                                    "Array nodes do not have a consistent length: " + lengthFound + " has length of "
                                            + expectedLength + ", " + fieldName + " has length of " + arrayLength);
                        }
                    } else {
                        lengthFound = fieldName;
                        expectedLength = arrayLength;
                    }
                    nodes[ii] = arrayNode;
                }
            }

            if (expectedLength <= 0) {
                for (int ii = 0; ii < nArrayFields; ++ii) {
                    arrayFieldProcessors.get(ii).accept(NullNode.getInstance(), holder);
                }
                holders[holder].singleRow();
                processHolder(holder, true);
            } else {
                for (int expandedRow = 0; expandedRow < expectedLength; ++expandedRow) {
                    if (isFirst && expandedRow == 0) {
                        if (expectedLength > 1 || !isLast) {
                            holders[holder].startTransaction();
                        } else {
                            holders[holder].singleRow();
                        }
                    } else if (isLast && (expandedRow == expectedLength - 1)) {
                        holders[holder].endTransaction();
                    } else {
                        holders[holder].inTransaction();
                    }
                    final int startingPosition = holders[holder].getDataPosition();
                    for (int ii = 0; ii < nArrayFields; ++ii) {
                        final ArrayNode node = nodes[ii];
                        try {
                            if (node == null) {
                                arrayFieldProcessors.get(ii).accept(NullNode.getInstance(), holder);
                            } else {
                                arrayFieldProcessors.get(ii).accept(node.get(expandedRow), holder);
                            }
                        } catch (Exception ex) {
                            throw new JSONIngesterException("Exception occurred while processing array record at index "
                                    + expandedRow + " (of " + expectedLength + ')', ex);
                        }
                    }
                    if (expandedRow == expectedLength - 1) {
                        processHolder(holder, isLast);
                    } else {
                        copyAndProcessHolder(holder, startingPosition);
                    }
                }
            }
        } else {
            if (isFirst && isLast) {
                holders[holder].singleRow();
            } else if (isFirst) {
                holders[holder].startTransaction();
            } else if (isLast) {
                holders[holder].endTransaction();
            } else {
                holders[holder].inTransaction();
            }
            processHolder(holder, true);
        }

        if (postProcessConsumer != null) {
            log.debug().append(Thread.currentThread().getName()).append(": Running postProcessConsumer function")
                    .endl();
            postProcessConsumer.accept(msgData, record);
        }
    }

    private static void processSingleSubtableRecord(JSONToStreamPublisherAdapter subtableAdapter,
            String subtableFieldName,
            int subtableHolderIdx, long thisSubtableMsgNo, JsonNode subtableRecord, boolean isSubtableFirst,
            boolean isSubtableLast) {
        try {
            subtableAdapter.holders[subtableHolderIdx].setMessageNumber(thisSubtableMsgNo);

            if (isSubtableFirst && isSubtableLast) {
                subtableAdapter.holders[subtableHolderIdx].singleRow();
            } else if (isSubtableFirst) {
                subtableAdapter.holders[subtableHolderIdx].startTransaction();
            } else if (isSubtableLast) {
                subtableAdapter.holders[subtableHolderIdx].endTransaction();
            } else {
                subtableAdapter.holders[subtableHolderIdx].inTransaction();
            }

            // process the record (and reset the holder at holders[holderIdx])
            // the MessageMetadata can be null because subtables don't process it anyway.
            final MessageMetadata subtableMsgMetadata = null;
            subtableAdapter.processOneRecordTopLevel(
                    subtableHolderIdx,
                    subtableMsgMetadata,
                    subtableRecord,
                    isSubtableFirst,
                    isSubtableLast);
        } catch (Exception ex) {
            throw new JSONIngesterException("Failed processing subtable field \"" + subtableFieldName + '"', ex);
        }
    }

    /**
     * Handle an empty array of inbound messages. This is to prevent gaps in the sequence numbers processed by
     * {@link #cleanup()}.
     *
     * @param holder The holder number to mark as empty.
     */
    private void addEmptyHolder(final int holder) {
        holders[holder].setIsEmpty(true);
        holders[holder].singleRow();
        processedMessages.add(holders[holder]);
        messagesProcessed.increment();
        // And now get ready for another row.
        holders[holder] = createRowHolder();
    }

    private void processHolder(final int holder, final boolean messageCompleted) {
        processedMessages.add(holders[holder]);
        if (messageCompleted) {
            messagesProcessed.increment();
        }
        // And now get ready for another row.
        holders[holder] = createRowHolder();
    }

    private void copyAndProcessHolder(final int holder, final int startingPosition) {
        final InMemoryRowHolder existingHolder = holders[holder];
        holders[holder] = createRowHolder();
        holders[holder].copyDataFrom(existingHolder, startingPosition);

        processedMessages.add(existingHolder);
        // And now get ready for another row.
    }

    @NotNull
    private InMemoryRowHolder createRowHolder() {
        // the row holder needs to have room for the message metadata as well
        // (these setters are stored in this adapter's 'owner'
        return new InMemoryRowHolder(chunkPopulators.size() + TextMessage.numberOfMetadataFields());
    }

    @Override
    public void waitForProcessing(final long timeoutMillis) throws InterruptedException, TimeoutException {
        if (numThreads == 0) {
            // processing is synchronous so there is nothing to wait for
            return;
        }

        // use a loop with sleep instead of wait/notify to avoid any contention with processing threads
        final long startTime = System.currentTimeMillis();
        while (hasUnprocessedMessages()
                && (System.currentTimeMillis() - startTime) < timeoutMillis) {
            // noinspection BusyWait
            Thread.sleep(1);
        }
        if (hasUnprocessedMessages()) {
            throw new TimeoutException();
        }
    }

    private boolean hasUnprocessedMessages() {
        return messagesQueued.longValue() > messagesProcessed.longValue();
    }

    /**
     * Ensure that the chunks are initialized, including for all nested adpaters. This must only be called on the
     * topmost parent adapter!
     */
    private void ensureChunksInitialized() {
        setChunks(streamPublisher.getChunks());
    }

    /**
     * Set the chunks to write to, including for nested adapters.
     * 
     * @param chunks The new chunks
     */
    public void setChunks(final WritableChunk<Values>[] chunks) {
        this.chunks = chunks;
        for (JSONToStreamPublisherAdapter nestedAdapter : nestedAdapters) {
            nestedAdapter.setChunks(chunks);
        }
    }

    /**
     * Stores the message metadata in the row holder. (It is copied to the table writer's setters by
     * {@link #populateNowTimeChunk}).
     *
     * @param metadata The message metadata.
     * @param holderIdx The index of the target message holder.
     */
    private void processMetadata(final MessageMetadata metadata, final int holderIdx) {
        if (owner != null) {
            final InMemoryRowHolder holder = holders[holderIdx];
            if (owner.getMessageIdColumn() != null) {
                holder.getSetter(owner.getMessageIdColumn(), String.class).set(metadata.getMessageId());
            }
            if (owner.getSendTimeColumn() != null) {
                holder.getSetter(owner.getSendTimeColumn(), long.class).set(metadata.getSentTime());
            }
            if (owner.getReceiveTimeColumn() != null) {
                holder.getSetter(owner.getReceiveTimeColumn(), long.class).set(metadata.getReceiveTime());
            }
            // Do not set the 'now' time - we want that to be set as the very last step before flushing data (to disk or
            // otherwise)
        }
    }

    private static class PermissiveArrayList<T> extends ArrayList<T> {
        @Override
        public void removeRange(final int fromIndex, final int toIndex) {
            super.removeRange(fromIndex, toIndex);
        }
    }

    static class RoutedAdapterInfo {
        @NotNull
        private final JSONToStreamPublisherAdapterBuilder adapterBuilder;
        @Nullable
        private final Predicate<JsonNode> predicate;

        RoutedAdapterInfo(@NotNull JSONToStreamPublisherAdapterBuilder adapterBuilder,
                @Nullable Predicate<JsonNode> predicate) {
            this.adapterBuilder = adapterBuilder;
            this.predicate = predicate;
        }
    }
}
