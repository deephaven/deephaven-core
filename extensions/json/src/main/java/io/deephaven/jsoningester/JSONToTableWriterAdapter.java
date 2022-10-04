/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.base.Pair;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.ingest.JsonNodeUtil;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.time.DateTime;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
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
 * A factory for adapter should be created using the nested Builder class.  Each adapter instance is bound to the
 * TableWriter that is passed to the factory method.
 */
public class JSONToTableWriterAdapter implements StringToTableWriterAdapter {
    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final int ERROR_REPORTING = 96;
    private static final int ERROR_PROCESSING = 98;
    private static final int MAX_UNPARSEABLE_LOG_MESSAGES = Configuration.getInstance().getIntegerWithDefault("JSONToTableWriterAdapter.maxUnparseableLogMessages", 100);
    private static final int SHUTDOWN_TIMEOUT_SECS = 30;
    public static final String SUBTABLE_RECORD_ID_COL = "SubtableRecordId";
    static final int N_CONSUMER_THREADS_DEFAULT = Configuration.getInstance().getIntegerWithDefault("JSONToTableWriterAdapter.consumerThreads", 1);

    @SuppressWarnings("FieldCanBeLocal")
    private final ThreadGroup consumerThreadGroup;
    private final List<DataToTableWriterAdapter> allSubtableAdapters;

    private StringMessageToTableAdapter<?> owner;

    private final TableWriter<?> writer;
    private final Logger log;
    private final boolean allowMissingKeys;
    private final boolean allowNullValues;
    private final List<Consumer<InMemoryRowHolder>> fieldSetters = new ArrayList<>();
    private final List<ObjIntConsumer<JsonNode>> fieldProcessors = new ArrayList<>();
    private final List<String> arrayFieldNames = new ArrayList<>();
    private final List<ObjIntConsumer<JsonNode>> arrayFieldProcessors = new ArrayList<>();
    private final List<JSONToTableWriterAdapter> nestedAdapters = new ArrayList<>();
    private final boolean processArrays;

    /**
     * Number of JSON processing threads. If {@code numThreads == 0}, then {@link #consumeString} will
     * process messages synchronously (instead of enqueuing them on the {@link #waitingMessages} queue to be
     * processed by the {@link #consumerThreadGroup consumer threads}).
     */
    private final int numThreads;

    /**
     * Latch used to wait for JSON threads to finish during shutdown (if threads are used)
     */
    private final CountDownLatch consumerThreadsCountDownLatch;

    private final BlockingQueue<TextMessageMetadata> waitingMessages = new LinkedBlockingQueue<>();
    private final BlockingQueue<InMemoryRowHolder> processedMessages = new LinkedBlockingQueue<>();
    private final int CONSUMER_WAIT_INTERVAL_MS = Configuration.getInstance().getIntegerWithDefault("JSONToTableWriterAdapter.consumerWaitInterval", 100);
    private final int CONSUMER_REPORT_INTERVAL_MS = Configuration.getInstance().getIntegerWithDefault("JSONToTableWriterAdapter.consumerReportInterval", 60000);
    private InMemoryRowHolder[] holders;
    private final AtomicLong messagesQueued = new AtomicLong();
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

    private final boolean skipUnparseableMessages = Configuration.getInstance().getBooleanWithDefault("JSONToTableWriterAdapter.skipUnparseableMessages", true);
    private int unparseableMessagesLogged = 0;

    /**
     * Whether this adapter has been shut down. An exception will be thrown if messages are received
     * (by {@link #consumeString}) after shutdown, since it may be too late for them be processed.
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Instance count. Used for thread names.
     */
    private static final AtomicInteger instanceCounter = new AtomicInteger();
    private Map<String, JSONToTableWriterAdapter> subtableFieldsToAdapters = new LinkedHashMap<>();

    private final Queue<SubtableData> subtableProcessingQueue;

    JSONToTableWriterAdapter(final TableWriter<?> writer,
                             @NotNull final Logger log,
                             final boolean allowMissingKeys,
                             final boolean allowNullValues,
                             final boolean processArrays,
                             final int nConsumerThreads,
                             final Map<String, String> columnToJsonField,
                             final Map<String, ToIntFunction<JsonNode>> columnToIntFunctions,
                             final Map<String, ToLongFunction<JsonNode>> columnToLongFunctions,
                             final Map<String, ToDoubleFunction<JsonNode>> columnToDoubleFunctions,
                             final Map<String, Pair<Class<?>, Function<JsonNode, ?>>> columnToObjectFunctions,
                             final Map<String, JSONToTableWriterAdapterBuilder> nestedFieldBuilders,
                             final Map<String, String> columnToParallelField,
                             final Map<String, JSONToTableWriterAdapterBuilder> parallelNestedFieldBuilders,
                             final Map<String, Pair<JSONToTableWriterAdapterBuilder, TableWriter<?>>> fieldToSubtableBuilders,
                             final Set<String> columnsUnmapped,
                             final boolean autoValueMapping,
                             final boolean createHolders
    ) {
        this(writer, log, allowMissingKeys, allowNullValues, processArrays,
                nConsumerThreads,
                columnToJsonField,
                columnToIntFunctions,
                columnToLongFunctions,
                columnToDoubleFunctions,
                columnToObjectFunctions,
                nestedFieldBuilders,
                columnToParallelField,
                parallelNestedFieldBuilders,
                fieldToSubtableBuilders,
                columnsUnmapped,
                autoValueMapping,
                createHolders,
                new ConcurrentLinkedDeque<>());
    }

    /**
     *
     * @param writer
     * @param log
     * @param allowMissingKeys
     * @param allowNullValues
     * @param processArrays
     * @param nThreads
     * @param columnToJsonField
     * @param columnToIntFunctions
     * @param columnToLongFunctions
     * @param columnToDoubleFunctions
     * @param columnToObjectFunctions
     * @param nestedFieldBuilders
     * @param columnToParallelField
     * @param parallelNestedFieldBuilders
     * @param fieldToSubtableBuilders
     * @param allowedUnmappedColumns
     * @param autoValueMapping
     * @param createHolders Whether to create the InMemmoryRowHolders and associated thread pool.
     * @param subtableProcessingQueue
     */
     JSONToTableWriterAdapter(final TableWriter<?> writer,
                             @NotNull final Logger log,
                             final boolean allowMissingKeys,
                             final boolean allowNullValues,
                             final boolean processArrays,
                             final int nThreads,
                             final Map<String, String> columnToJsonField,
                             final Map<String, ToIntFunction<JsonNode>> columnToIntFunctions,
                             final Map<String, ToLongFunction<JsonNode>> columnToLongFunctions,
                             final Map<String, ToDoubleFunction<JsonNode>> columnToDoubleFunctions,
                             final Map<String, Pair<Class<?>, Function<JsonNode, ?>>> columnToObjectFunctions,
                             final Map<String, JSONToTableWriterAdapterBuilder> nestedFieldBuilders,
                             final Map<String, String> columnToParallelField,
                             final Map<String, JSONToTableWriterAdapterBuilder> parallelNestedFieldBuilders,
                             final Map<String, Pair<JSONToTableWriterAdapterBuilder, TableWriter<?>>> fieldToSubtableBuilders,
                             final Set<String> allowedUnmappedColumns,
                             final boolean autoValueMapping,
                             final boolean createHolders,
                             final Queue<SubtableData> subtableProcessingQueue
    ) {
        this.log = log;
        this.writer = writer;
        this.allowMissingKeys = allowMissingKeys;
        this.allowNullValues = allowNullValues;
        this.processArrays = processArrays;
        this.numThreads = nThreads;
        this.subtableProcessingQueue = subtableProcessingQueue;

        final int instanceId = instanceCounter.getAndIncrement();

        // Get the list of all the columns that our nested builders provide.
        final Set<String> nestedColumns = Stream.concat(
                        nestedFieldBuilders.values().stream(),
                        parallelNestedFieldBuilders.values().stream()
                )
                .flatMap(builder -> builder.getDefinedColumns().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // The subtable row ID columns are implicitly defined for every field that's mapped to a subtable.
        final Set<String> subtableRowIdColumns = fieldToSubtableBuilders
                .keySet()
                .stream()
                .map(JSONToTableWriterAdapter::getSubtableRowIdColName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        final List<String> outputColumnNames = new LinkedList<>(columnToJsonField.keySet());
        outputColumnNames.addAll(columnToIntFunctions.keySet());
        outputColumnNames.addAll(columnToLongFunctions.keySet());
        outputColumnNames.addAll(columnToDoubleFunctions.keySet());
        outputColumnNames.addAll(columnToObjectFunctions.keySet());
        outputColumnNames.addAll(columnToParallelField.keySet());
        outputColumnNames.addAll(allowedUnmappedColumns);
        outputColumnNames.addAll(nestedColumns);
        outputColumnNames.addAll(subtableRowIdColumns);

        final List<String> allColumns = List.copyOf(outputColumnNames);

        final String[] tableWriterExpectedColNames = writer.getColumnNames();
        final List<String> missingColumns = new ArrayList<>();
        for (final String columnName : tableWriterExpectedColNames) {
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
                makeFieldProcessors(writer, columnName, columnToJsonField.get(columnName));
            } else if (columnToIntFunctions.containsKey(columnName)) {
                makeIntFunctionFieldProcessor(writer, columnName, columnToIntFunctions.get(columnName));
            } else if (columnToLongFunctions.containsKey(columnName)) {
                makeLongFunctionFieldProcessor(writer, columnName, columnToLongFunctions.get(columnName));
            } else if (columnToDoubleFunctions.containsKey(columnName)) {
                makeDoubleFunctionFieldProcessor(writer, columnName, columnToDoubleFunctions.get(columnName));
            } else if (columnToObjectFunctions.containsKey(columnName)) {
                final Pair<Class<?>, Function<JsonNode, ?>> classFunctionPair = columnToObjectFunctions.get(columnName);
                makeFunctionFieldProcessor(writer, columnName, classFunctionPair.first, classFunctionPair.second);
            } else if (columnToParallelField.containsKey(columnName)) {
                makeArrayFieldProcessors(writer, columnName, columnToParallelField.get(columnName));
            } else if (autoValueMapping) {
                makeFieldProcessors(writer, columnName, columnName);
            } else {
                missingColumns.add(columnName);
            }
        }

        // This is the only part of this method/class that works in the sense of mapping JSON fields to DH outputs.
        // Everything else maps DH outputs to the corresponding JSON source field.
        for (Map.Entry<String, Pair<JSONToTableWriterAdapterBuilder, TableWriter<?>>> subtableEntry : fieldToSubtableBuilders.entrySet()) {
            final String fieldName = subtableEntry.getKey();
            outputColumnNames.remove(getSubtableRowIdColName(fieldName));
            final Pair<JSONToTableWriterAdapterBuilder, TableWriter<?>> adapterBuilderAndWriter = subtableEntry.getValue();
            final JSONToTableWriterAdapterBuilder builder = adapterBuilderAndWriter.first;
            outputColumnNames.removeAll(builder.getDefinedColumns());

            makeSubtableFieldProcessor(fieldName, builder, adapterBuilderAndWriter.second);
        }

        for (final Map.Entry<String, JSONToTableWriterAdapterBuilder> nestedFieldEntry : nestedFieldBuilders.entrySet()) {
            outputColumnNames.removeAll(nestedFieldEntry.getValue().getDefinedColumns());
            makeCompositeFieldProcessor(writer, allColumns, nestedFieldEntry.getKey(), nestedFieldEntry.getValue());
        }
        for (final Map.Entry<String, JSONToTableWriterAdapterBuilder> nestedFieldEntry : parallelNestedFieldBuilders.entrySet()) {
            outputColumnNames.removeAll(nestedFieldEntry.getValue().getDefinedColumns());
            makeCompositeParallelFieldProcessor(writer, allColumns, nestedFieldEntry.getKey(), nestedFieldEntry.getValue());
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
            throw new JSONIngesterException("Found mappings that do not correspond to this table: " + outputColumnNames);
        }

        allSubtableAdapters = getAllSubtableAdapters(this);

        if (numThreads > 1 && (!columnToParallelField.isEmpty() || !parallelNestedFieldBuilders.isEmpty() || !subtableFieldsToAdapters.isEmpty())) {
            throw new JSONIngesterException("JSON multithreaded processing does not yet support multiple output rows per message.");
        }

        if (createHolders) {
            // the top level adapter creates the holders, which have spaces for each saved value including in the nested
            // holders.  Now that we've processed all the nested adapters, we can set the holders for those adapters which
            // in turn set it for any nested holder they have.
            final int numHolders = numThreads > 0 ? numThreads : 1;
            holders = new InMemoryRowHolder[numHolders];
            for(int holderIdx = 0; holderIdx < numHolders; holderIdx++) {
                holders[holderIdx] = createRowHolder(); //
            }

            // Create the message processing threads -- one per holder.
            // For subtable adapters, numThreads is zero, and processing is done synchronously by consumeString().
            // This way, subtables from multiple records are parsed under the parent adapter's thread pool.
            if (numThreads > 0
                || numThreads == -1 // numThreads==-1 is used in tests to create an adapter that never processes
            ) {
                consumerThreadGroup = new ThreadGroup(JSONToTableWriterAdapter.class.getSimpleName() + instanceId + "_ThreadGroup");
                consumerThreadGroup.setDaemon(true);

                for (int threadCount = 0; threadCount < numThreads; threadCount++) {
                    final Thread t = new ConsumerThread(consumerThreadGroup, instanceId, threadCount);
                    t.setDaemon(true);
                    t.start();
                }

                {
                    // Start a thread to flush the adapter occasionally occasionally
                    // TODO: this is really the responsibility of whoever owns the TableWriters (i.e. some ingester)
                    final Thread cleanupThread = new Thread(consumerThreadGroup, () -> {
                        final long flushIntervalMillis = 5000L;
                        long lastFlush = 0L;
                        while (!isShutdown.get()) {
                            try {
                                //noinspection BusyWait
                                Thread.sleep(flushIntervalMillis);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(Thread.currentThread().getName() + ": Interrupted while waiting to flush", e);
                            }
                            final long now = System.currentTimeMillis();
                            if(now - lastFlush > flushIntervalMillis) {
                                try {
                                    lastFlush = now;
                                    JSONToTableWriterAdapter.this.cleanup();
                                } catch (IOException e) {
                                    throw new RuntimeException(Thread.currentThread().getName() + ": Exception while flushing data to table writers", e);
                                }
                            }
                        }

                        // wait for consumer threads to exit. (all messages shoudl be processed by then)
                        try {
                            if(!(JSONToTableWriterAdapter.this.consumerThreadsCountDownLatch.await(15, TimeUnit.SECONDS))) {
                                log.warn().append("Consumer threads did not exit within timeout").endl();
                            }
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                        try {
                            JSONToTableWriterAdapter.this.cleanup();
                        } catch (IOException e) {
                            throw new RuntimeException("Exception while flushing data to table writers", e);
                        }
                    }, instanceId + "_cleanupThread");
                    cleanupThread.setDaemon(true);
                    cleanupThread.start();

                    // handle shutdown. (this also should be done by an ingester, but if we are taking responsibility for
                    // flushing the data, then we should also take responsibility for clean shutdown.)
                    ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.FIRST, this::shutdown);
                }
            } else {
                consumerThreadGroup = null;
            }
            setNestedHolders(holders);

            consumerThreadsCountDownLatch = new CountDownLatch(Math.max(0, numThreads));
        } else {
            if (numThreads != 0) {
                throw new IllegalArgumentException("numThreads must be zero when createHolders is false. numThreads: " + numThreads);
            }
            consumerThreadGroup = null;
            consumerThreadsCountDownLatch = null;
        }
    }

    /**
     * Get a column name that will contain an ID used for mapping between the parent table row and the corresponding subtable rows.
     *
     * @param fieldName
     * @return
     */
    @NotNull
    public static String getSubtableRowIdColName(String fieldName) {
        return fieldName + "_id";
    }

    private void setHolders(final InMemoryRowHolder[] holders) {
        this.holders = holders;
        setNestedHolders(holders);
    }

    private void setNestedHolders(final InMemoryRowHolder[] holders) {
        nestedAdapters.forEach(na -> na.setHolders(holders));
    }

    private void makeCompositeFieldProcessor(final TableWriter<?> writer,
                                             final List<String> allColumns,
                                             final String fieldName,
                                             final JSONToTableWriterAdapterBuilder nestedBuilder) {

        // Build a new set of columns that are allowed to be unmapped, as far as the nested field processor is concerned.
        final Set<String> newAllowedUnmapped = new HashSet<>(allColumns);

        // anything defined by the nested builder must be mapped.
        newAllowedUnmapped.removeAll(nestedBuilder.getDefinedColumns());

//        // implicit subtable ID columns defined in the parent are not relevant to the nested adapter
//        newAllowedUnmapped.removeAll(subtableFieldsToAdapters.keySet());

        final JSONToTableWriterAdapter nestedAdapter = nestedBuilder.makeNestedAdapter(log, writer, newAllowedUnmapped, subtableProcessingQueue);
        nestedAdapters.add(nestedAdapter);

        // we make a single field processor that in turn calls the nested adapter's field processors after extracting
        // the correct record from this JSON and making a new one.  The nested adapter has as many field setters as
        // needed for each of it's fields (meaning the processors and setter array lists are not actually parallel).
        fieldProcessors.add(((jsonRecord, holder) -> {
            try {
                final Object field = JsonNodeUtil.getValue(jsonRecord, fieldName, allowMissingKeys, allowNullValues);
                if (field == null) {
                    final JsonNode record = NullNode.getInstance();
                    nestedAdapter.fieldProcessors.forEach(fc -> fc.accept(record, holder));
                } else if (field instanceof ObjectNode) {
                    final JsonNode record = (ObjectNode) field;
                    nestedAdapter.fieldProcessors.forEach(fc -> fc.accept(record, holder));

                    // TODO: think this can be deleted -- is handled by makeSubtableAdatper() now
//                for (Map.Entry<String, JSONToTableWriterAdapter> nestedSubtableEntry : nestedAdapter.subtableFieldsToAdapters.entrySet()) {
//                    final String nestedSubtableFieldName = nestedSubtableEntry.getKey();
//                    final JSONToTableWriterAdapter nestedSubtableAdapter = nestedSubtableEntry.getValue();
//                    final JsonNode nestedSubtableFieldValue = ((ObjectNode) field).get(nestedSubtableFieldName);
//
//                    // Enqueue the subtable node to be processed by the subtable adapter (this happens after all the main
//                    // fieldProcessors have been processed)
//                    nestedSubtableProcessingQueue.add(new SubtableData(fieldName, nestedSubtableAdapter, nestedSubtableFieldValue));
//                }
                } else {
                    throw new JSONIngesterException("Field is of unexpected type " + field.getClass() + ", expected ObjectNode");
                }
            } catch (Exception ex) {
                throw new JSONIngesterException("Exception while processing nested field \"" + fieldName + "\"", ex);
            }
        }));
        fieldSetters.addAll(nestedAdapter.fieldSetters);
    }

    private void makeCompositeParallelFieldProcessor(final TableWriter<?> writer,
                                                     final List<String> allColumns,
                                                     final String fieldName,
                                                     final JSONToTableWriterAdapterBuilder nestedBuilder) {
        final Set<String> newUnmapped = new HashSet<>(allColumns);
        newUnmapped.removeAll(nestedBuilder.getDefinedColumns());

        final JSONToTableWriterAdapter nestedAdapter = nestedBuilder.makeNestedAdapter(log, writer, newUnmapped, subtableProcessingQueue);
        nestedAdapters.add(nestedAdapter);

        arrayFieldNames.add(fieldName);
        arrayFieldProcessors.add(((jsonNode, holder) -> {
            if (jsonNode == null || jsonNode.isNull()) {
                final JsonNode record = NullNode.getInstance();
                nestedAdapter.fieldProcessors.forEach(fc -> fc.accept(record, holder));
            } else if (jsonNode.isObject()) {
                final JsonNode record = jsonNode;
                nestedAdapter.fieldProcessors.forEach(fc -> fc.accept(record, holder));
            } else {
                throw new JSONIngesterException("Nested parallel array field \"" + fieldName + "\" is of unexpected type " + jsonNode.getClass() + ", expected ObjectNode");
            }
        }));
        fieldSetters.addAll(nestedAdapter.fieldSetters);
    }


    /**
     * This is like {@link #makeCompositeFieldProcessor} except it processes fields into a different table.
     *
     * @param fieldName
     * @param subtableBuilder
     * @param subtableWriter
     */
    private void makeSubtableFieldProcessor(String fieldName,
                                            JSONToTableWriterAdapterBuilder subtableBuilder,
                                            TableWriter<?> subtableWriter) {

        // Subtable record counter, mapping each row of the parent table to the corresponding rows of the subtable.
        // TODO: it would be better to use a unique parent message ID if available
        final AtomicLong subtableRecordIdCounter = new AtomicLong(0);

        final JSONToTableWriterAdapter subtableAdapter = subtableBuilder
                .makeSubtableAdapter(
                        log,
                        subtableWriter,
                        Collections.emptySet(),
                        subtableProcessingQueue,
                        subtableRecordIdCounter
                );
        subtableFieldsToAdapters.put(fieldName, subtableAdapter);

        // monotonic increasing message numbers for subtables are automatically generated
        //
        // This is distinct from the counter above because this is only used internally
        // and is only required to increase during the lifespan of this adapter.
        // The ID above is displayed in tables and ideally would be unique even if persisted/reread.
        final AtomicLong subtableMessageCounter = new AtomicLong(0);

        // Field name in the *parent* Table giving the corresponding ID of the row(s) in the subtable
        final String subtableRowIdFieldName = getSubtableRowIdColName(fieldName);

        // setter for the *subtable* that will contain that same ID
        final RowSetter<Long> setter = subtableWriter.getSetter(SUBTABLE_RECORD_ID_COL, long.class);
        final Class<?> setterType = setter.getType();

        final ObjIntConsumer<JsonNode> fieldProcessor;
        final MutableInt position = new MutableInt();
        fieldProcessor = (JsonNode record, int holderNumber) -> {
            // index of the next record being processed into the subtable
            final long subtableRecordIdxVal = subtableRecordIdCounter.getAndIncrement();

            // store the idx in the rowSetter (later, the fieldSetter will add it to the table)
            // note that this will only work correctly when single-threaded
            final InMemoryRowHolder.SingleRowSetter rowSetter = getSingleRowSetterAndCapturePosition(subtableRowIdFieldName, setterType, position, holderNumber);
            rowSetter.setLong(subtableRecordIdxVal);

            final JsonNode subtableFieldValue = record.get(fieldName);

            // Enqueue the subtable node to be processed by the subtable adapter (this happens after all the main
            // fieldProcessors have been processed)
            subtableProcessingQueue.add(new SubtableData(fieldName, subtableAdapter, subtableFieldValue, subtableMessageCounter));
        };

        fieldProcessors.add(fieldProcessor);

        // Add a fieldSetter that updates a column in the row in the parent table with the subtable row ID
        final RowSetter<Long> subtableRowIdFieldSetter = writer.getSetter(subtableRowIdFieldName, long.class);
        final Consumer<InMemoryRowHolder> fieldSetterParent = (InMemoryRowHolder holder) -> subtableRowIdFieldSetter.setLong(holder.getLong(position.intValue()));
        fieldSetters.add(fieldSetterParent);
    }

    private void makeFunctionFieldProcessor(final TableWriter<?> writer,
                                            final String columnName,
                                            final Class<?> returnType,
                                            final Function<JsonNode, ?> function) {
        @SuppressWarnings("rawtypes") final RowSetter setter = writer.getSetter(columnName);
        final Class<?> setterType = setter.getType();
        if (!setterType.isAssignableFrom(returnType)) {
            throw new JSONIngesterException("Column " + columnName + " is of type " + setterType + ", can not assign function of type: " + returnType);
        }
        final Consumer<InMemoryRowHolder> fieldSetter;
        final ObjIntConsumer<JsonNode> fieldConsumer;
        final MutableInt position = new MutableInt(0);

        if (setterType == boolean.class || setterType == Boolean.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).setBoolean(TypeUtils.unbox((Boolean) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setBoolean(holder.getBoolean(position.intValue()));
        } else if (setterType == char.class || setterType == Character.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).setChar(TypeUtils.unbox((Character) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setChar(holder.getChar(position.intValue()));
        } else if (setterType == byte.class || setterType == Byte.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).setByte(TypeUtils.unbox((Byte) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setByte(holder.getByte(position.intValue()));
        } else if (setterType == short.class || setterType == Short.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).setShort(TypeUtils.unbox((Short) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setShort(holder.getShort(position.intValue()));
        } else if (setterType == int.class || setterType == Integer.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).setInt(TypeUtils.unbox((Integer) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setInt(holder.getInt(position.intValue()));
        } else if (setterType == long.class || setterType == Long.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).setLong(TypeUtils.unbox((Long) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setLong(holder.getLong(position.intValue()));
        } else if (setterType == float.class || setterType == Float.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).setFloat(TypeUtils.unbox((Float) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setFloat(holder.getFloat(position.intValue()));
        } else if (setterType == double.class || setterType == Double.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).setDouble(TypeUtils.unbox((Double) function.apply(record)));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setDouble(holder.getDouble(position.intValue()));
        } else {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, returnType, position, holderNumber).set(function.apply(record));
            //noinspection unchecked
            fieldSetter = (InMemoryRowHolder holder) -> setter.set(holder.getObject(position.intValue()));
        }
        fieldProcessors.add(fieldConsumer);
        fieldSetters.add(fieldSetter);
    }

    private void makeIntFunctionFieldProcessor(final TableWriter<?> writer,
                                               final String columnName,
                                               final ToIntFunction<JsonNode> function) {
        //noinspection rawtypes
        final RowSetter setter = writer.getSetter(columnName);
        final Class<?> setterType = setter.getType();
        if (setterType != int.class) {
            throw new JSONIngesterException("Column " + columnName + " is of type " + setterType + ", can not assign ToIntFunction.");
        }
        final MutableInt position = new MutableInt();
        final ObjIntConsumer<JsonNode> fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, int.class, position, holderNumber).setInt(TypeUtils.unbox(function.applyAsInt(record)));
        final Consumer<InMemoryRowHolder> fieldProcessor = (InMemoryRowHolder holder) -> setter.setInt(holder.getInt(position.intValue()));
        fieldProcessors.add(fieldConsumer);
        fieldSetters.add(fieldProcessor);
    }

    private void makeLongFunctionFieldProcessor(final TableWriter<?> writer,
                                                final String columnName,
                                                final ToLongFunction<JsonNode> function) {
        @SuppressWarnings("rawtypes") final RowSetter setter = writer.getSetter(columnName);
        final Class<?> setterType = setter.getType();
        if (setterType != long.class) {
            throw new JSONIngesterException("Column " + columnName + " is of type " + setterType + ", can not assign ToLongFunction.");
        }
        final MutableInt position = new MutableInt();
        final ObjIntConsumer<JsonNode> fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, long.class, position, holderNumber).setLong(TypeUtils.unbox(function.applyAsLong(record)));
        final Consumer<InMemoryRowHolder> fieldProcessor = (InMemoryRowHolder holder) -> setter.setLong(holder.getLong(position.intValue()));
        fieldProcessors.add(fieldConsumer);
        fieldSetters.add(fieldProcessor);
    }

    private void makeDoubleFunctionFieldProcessor(final TableWriter<?> writer,
                                                  final String columnName,
                                                  final ToDoubleFunction<JsonNode> function) {
        @SuppressWarnings("rawtypes") final RowSetter setter = writer.getSetter(columnName);
        final Class<?> setterType = setter.getType();
        if (setterType != double.class) {
            throw new JSONIngesterException("Column " + columnName + " is of type " + setterType + ", can not assign ToDoubleFunction.");
        }
        final MutableInt position = new MutableInt();
        final ObjIntConsumer<JsonNode> fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, double.class, position, holderNumber).setDouble(TypeUtils.unbox(function.applyAsDouble(record)));
        final Consumer<InMemoryRowHolder> fieldProcessor = (InMemoryRowHolder holder) -> setter.setDouble(holder.getDouble(position.intValue()));
        fieldProcessors.add(fieldConsumer);
        fieldSetters.add(fieldProcessor);
    }

    private void makeFieldProcessors(final TableWriter<?> writer,
                                     final String columnName,
                                     final String fieldName) {
        final Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> p = makeFieldProcessorAndSetter(writer, columnName, fieldName);
        fieldProcessors.add(p.first);
        fieldSetters.add(p.second);
    }

    private Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> makeFieldProcessorAndSetter(final TableWriter<?> writer,
                                                                                                    final String columnName,
                                                                                                    final String fieldName) {
        @SuppressWarnings("rawtypes") final RowSetter setter = writer.getSetter(columnName);
        final Class<?> setterType = setter.getType();

        final Consumer<InMemoryRowHolder> fieldSetter;
        final ObjIntConsumer<JsonNode> fieldConsumer;
        final MutableInt position = new MutableInt();
        // TODO: how does this work with threading? aren't the consumers run by multiple threads at once?
        if (setterType == boolean.class || setterType == Boolean.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setBoolean(JsonNodeUtil.getBoolean(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setBoolean(holder.getBoolean(position.intValue()));
        } else if (setterType == char.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setChar(JsonNodeUtil.getChar(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setChar(holder.getChar(position.intValue()));
        } else if (setterType == byte.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setByte(JsonNodeUtil.getByte(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setByte(holder.getByte(position.intValue()));
        } else if (setterType == short.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setShort(JsonNodeUtil.getShort(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setShort(holder.getShort(position.intValue()));
        } else if (setterType == int.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setInt(JsonNodeUtil.getInt(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setInt(holder.getInt(position.intValue()));
        } else if (setterType == long.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setLong(JsonNodeUtil.getLong(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setLong(holder.getLong(position.intValue()));
        } else if (setterType == float.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setFloat(JsonNodeUtil.getFloat(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setFloat(holder.getFloat(position.intValue()));
        } else if (setterType == double.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setDouble(JsonNodeUtil.getDouble(record, fieldName, allowMissingKeys, allowNullValues));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setDouble(holder.getDouble(position.intValue()));
        } else if (setterType == String.class) {
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).set(JsonNodeUtil.getString(record, fieldName, allowMissingKeys, allowNullValues));
            //noinspection unchecked
            fieldSetter = (InMemoryRowHolder holder) -> setter.set(holder.getObject(position.intValue()));
        } else if (setterType == DateTime.class) {
            // Note that the preferred way to handle DateTimes is to store them as longs, not DateTimes,
            // but if someone explicitly made a column of type DateTime, this will handle it.
            // If they want to provide a DateTime in an import file but convert it to a long, they have to
            // provide that as an explicit function.
            fieldConsumer = (JsonNode record, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).set(JsonNodeUtil.getDateTime(record, fieldName, allowMissingKeys, allowNullValues));
            //noinspection unchecked
            fieldSetter = (InMemoryRowHolder holder) -> setter.set(holder.getObject(position.intValue()));
        } else {
            throw new UnsupportedOperationException("Can not convert JSON field to " + setterType + " for column " + columnName + " (field " + fieldName + ")");
        }
        return new Pair<>(fieldConsumer, fieldSetter);
    }

    private Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> makeFieldProcessorAndSetterNode(final TableWriter<?> writer,
                                                                                                        final String columnName) {
        @SuppressWarnings("rawtypes") final RowSetter setter = writer.getSetter(columnName);
        final Class<?> setterType = setter.getType();

        final Consumer<InMemoryRowHolder> fieldSetter;
        final ObjIntConsumer<JsonNode> fieldConsumer;
        final MutableInt position = new MutableInt();
        if (setterType == boolean.class || setterType == Boolean.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setBoolean(JsonNodeUtil.getBoolean(node));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setBoolean(holder.getBoolean(position.intValue()));
        } else if (setterType == char.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setChar(JsonNodeUtil.getChar(node));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setChar(holder.getChar(position.intValue()));
        } else if (setterType == byte.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setByte(JsonNodeUtil.getByte(node));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setByte(holder.getByte(position.intValue()));
        } else if (setterType == short.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setShort(JsonNodeUtil.getShort(node));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setShort(holder.getShort(position.intValue()));
        } else if (setterType == int.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setInt(JsonNodeUtil.getInt(node));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setInt(holder.getInt(position.intValue()));
        } else if (setterType == long.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setLong(JsonNodeUtil.getLong(node));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setLong(holder.getLong(position.intValue()));
        } else if (setterType == float.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setFloat(JsonNodeUtil.getFloat(node));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setFloat(holder.getFloat(position.intValue()));
        } else if (setterType == double.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).setDouble(JsonNodeUtil.getDouble(node));
            fieldSetter = (InMemoryRowHolder holder) -> setter.setDouble(holder.getDouble(position.intValue()));
        } else if (setterType == String.class) {
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).set(JsonNodeUtil.getString(node));
            //noinspection unchecked
            fieldSetter = (InMemoryRowHolder holder) -> setter.set(holder.getObject(position.intValue()));
        } else if (setterType == DateTime.class) {
            // Note that the preferred way to handle DateTimes is to store them as longs, not DateTimes,
            // but if someone explicitly made a column of type DateTime, this will handle it.
            // If they want to provide a DateTime in an import file but convert it to a long, they have to
            // provide that as an explicit function.
            fieldConsumer = (JsonNode node, int holderNumber) -> getSingleRowSetterAndCapturePosition(columnName, setterType, position, holderNumber).set(JsonNodeUtil.getDateTime(node));
            //noinspection unchecked
            fieldSetter = (InMemoryRowHolder holder) -> setter.set(holder.getObject(position.intValue()));
        } else {
            throw new UnsupportedOperationException("Can not convert JSON field to " + setterType + " for column " + columnName);
        }
        return new Pair<>(fieldConsumer, fieldSetter);
    }

    private void makeArrayFieldProcessors(final TableWriter<?> writer,
                                          final String columnName,
                                          final String fieldName) {
        final Pair<ObjIntConsumer<JsonNode>, Consumer<InMemoryRowHolder>> p = makeFieldProcessorAndSetterNode(writer, columnName);
        arrayFieldNames.add(fieldName);
        arrayFieldProcessors.add(p.first);
        fieldSetters.add(p.second);
    }

    /**
     * Get a SingleRowSetter and capture the position within the relevant holder's data array for the object controlled by that setter.
     *
     * @param columnName   The name of the column being populated
     * @param setterType   The class of the column being populated
     * @param position     The AtomicInteger whose value will be set (this must be final to be used in the lambdas where this can be called)
     * @param holderNumber Which RowHolder this is operating on
     * @return The resultant SingleRowSetter from the RowHolder for this column.
     */
    @NotNull
    private InMemoryRowHolder.SingleRowSetter getSingleRowSetterAndCapturePosition(final String columnName, final Class<?> setterType, final MutableInt position, final int holderNumber) {
        final InMemoryRowHolder.SingleRowSetter str = holders[holderNumber].getSetter(columnName, setterType);
        position.setValue(str.getThisPosition());
        return str;
    }

    /**
     * Recursively build a list of subtable adapters, from the given {@code adapter} and all its children. All of these must be
     * {@link DataToTableWriterAdapter#cleanup() cleaned up} after rows are written.
     *
     * @return A list of adapters for all subtables
     */
    private static List<DataToTableWriterAdapter> getAllSubtableAdapters(JSONToTableWriterAdapter adapter) {
        final List<DataToTableWriterAdapter> subtableAdatpers = new ArrayList<>(adapter.subtableFieldsToAdapters.values());
        for (JSONToTableWriterAdapter nestedAdapter : adapter.nestedAdapters) {
            subtableAdatpers.addAll(getAllSubtableAdapters(nestedAdapter));
        }
        return Collections.unmodifiableList(subtableAdatpers);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void consumeString(final TextMessageMetadata msgData,
                              final String input) throws IOException {
        if (isShutdown.get()) {
            throw new IllegalStateException("Message received after adapter shutdown!");
        }
        if (numThreads == 0) {
            // process synchronously
            processSingleMessage(0, msgData);

            // note: cleanup() must be run manually
            return;
        }
        final long queuedMessages = messagesQueued.getAndIncrement();
        if (queuedMessages != msgData.getMsgNo()) {
            throw new IllegalStateException("Unexpected message number " + msgData.getMsgNo() + ", previously queued messages " + queuedMessages);
        }
        waitingMessages.add(msgData);
    }

    @Override
    public void cleanup() throws IOException {
        synchronized (pendingCleanup) {
            final long beforePoll = System.nanoTime();
            final long intervalMessages = processedMessages.size();
            log.debug().append("JSONToTableWriterAdapter cleanup: Cleanup called with ").append(intervalMessages).append(" messages to clean up.").endl();
            if (intervalMessages > 0) {
                int cleanedMessages = 0;
                int badMessages = 0;
                processedMessages.drainTo(pendingCleanup);
                pendingCleanup.sort(Comparator.comparingLong(InMemoryRowHolder::getMessageNumber));

                if (!pendingCleanup.isEmpty()) {
                    final long firstMessageNumber = pendingCleanup.get(0).getMessageNumber();
                    log.debug().append("JSONToTableWriterAdapter cleanup: Cleaning up starting with ").append(firstMessageNumber).endl();
                    if (firstMessageNumber < nextMsgNo.longValue()) {
                        throw new IllegalStateException("Unexpected back-in-time message: " + firstMessageNumber + " is less than " + nextMsgNo);
                    }
                }

                while (!pendingCleanup.isEmpty()
                        && cleanedMessages < pendingCleanup.size()
                        && pendingCleanup.get(cleanedMessages).getMessageNumber() == nextMsgNo.get()) {
                    final InMemoryRowHolder finalHolder = pendingCleanup.get(cleanedMessages);
                    if (finalHolder.getParseException() != null) {
                        // there was a parsing exception for this message; we should log the error and proceed to the next message
                        if (unparseableMessagesLogged++ < MAX_UNPARSEABLE_LOG_MESSAGES) {
                            log.error().append("Unable to parse JSON message: \"" + finalHolder.getOriginalText() + "\": ").append(finalHolder.getParseException()).endl();
                        }
                        if (!skipUnparseableMessages) {
                            throw new JSONIngesterException("Unable to parse JSON message", finalHolder.getParseException());
                        }
                        nextMsgNo.incrementAndGet();
                    } else {
                        if (!finalHolder.getIsEmpty()) {

                            // First, set all the fields for which there is a 1-1 message/row ratio.
                            fieldSetters.forEach(fp -> fp.accept(finalHolder));

                            // Then run cleanup() for any subtable adapters before, we commit this row
                            // (The parent row has IDs referring to subtable rows, so subtable rows must be written
                            // first to ensure that anything querying the parent table can refer to the subtable data.)
                            for (DataToTableWriterAdapter subtableAdapter : allSubtableAdapters) {
                                subtableAdapter.cleanup();
                            }

                            cleanupMetadata(finalHolder);
                            writer.setFlags(finalHolder.getFlags());
                            writer.writeRow();
                            writer.flush();
                        }

                        if (finalHolder.getFlags() == Row.Flags.EndTransaction || finalHolder.getFlags() == Row.Flags.SingleRow) {
                            nextMsgNo.incrementAndGet();
                        }
                    }
                    cleanedMessages++;
                }
                // Compact the array. The next message in the sequence has not been processed yet, we will try again shortly
                if (cleanedMessages > 0) {
                    // Note that the toIndex of removeRange is exclusive, so (0,1) will remove only the message at position 0.
                    pendingCleanup.removeRange(0, cleanedMessages);
                }

                final long afterPoll = System.nanoTime();
                final long intervalNanos = afterPoll - beforePoll;
                log.debug().append("JSONToTableWriterAdapter cleanup - wrote ").append(cleanedMessages - badMessages).append(" to disk in ").append(intervalNanos / 1000_000L).append("ms, ").appendDouble(1000000000.0 * intervalMessages / intervalNanos, 4).append(" msgs/sec, remaining pending messages=").append(pendingCleanup.size()).append(", messages with errors=").append(badMessages).endl();
            }
        }
    }


    private void cleanupMetadata(final InMemoryRowHolder holder) {
//  log.warn("cleanupMetadata() not implemented; doing nothing");
//        if (owner.getMessageIdSetter() != null) {
//            owner.getMessageIdSetter().set((String)holder.getObject(owner.getMessageIdColumn()));
//        }
//        if (owner.getSendTimeSetter() != null) {
//            owner.getSendTimeSetter().set((DateTime) holder.getObject(owner.getSendTimeColumn()));
//        }
//        if (owner.getReceiveTimeSetter() != null) {
//            owner.getReceiveTimeSetter().set((DateTime)holder.getObject(owner.getReceiveTimeColumn()));
//        }
//        if (owner.getNowSetter() != null) {
//            owner.getNowSetter().set(DateTime.now());
//        }
    }

    /**
     * Shuts down the adapter and waits for consumer threads to exit.
     */
    @Override
    public void shutdown() {
        log.info().append("JSONToTableWriterAdapter shutdown").endl();
        final boolean wasAlreadyShutdown = !isShutdown.compareAndSet(false, true);
        if (wasAlreadyShutdown) {
            // just log an exception - doesn't really matter
            final IllegalStateException logOnlyException = new IllegalStateException("Already shut down.");
            log.warn().append("JSONToTableWriterAdapter shutdown: already shut down.")
                    .nl()
                    .append(logOnlyException)
                    .endl();
            return;
        } else {
            // Shut down the adapters
            log.debug().append("JSONToTableWriterAdapter shutdown - shutting down nested adapters").endl();
            nestedAdapters.forEach(JSONToTableWriterAdapter::shutdown);

            log.debug().append("JSONToTableWriterAdapter shutdown - shutting down subtable adapters").endl();
            subtableFieldsToAdapters.values().forEach(JSONToTableWriterAdapter::shutdown);
        }

        // IF using threads, wait for the consumer threads to finish:
        if (consumerThreadsCountDownLatch != null) {
            try {
                log.debug().append("JSONToTableWriterAdapter shutdown - awaiting termination").endl();
                if (!consumerThreadsCountDownLatch.await(SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS)) {
                    throw new JSONIngesterException("Timed out while awaiting shutdown! (Threads still running: " + consumerThreadsCountDownLatch.getCount() + ')');
                }
            } catch (InterruptedException ex) {
                throw new JSONIngesterException("Interrupted while awaiting shutdown!", ex);
            }
        }

        log.debug().append("JSONToTableWriterAdapter shutdown - complete").endl();
    }

    @Override
    public void setOwner(final StringMessageToTableAdapter<?> parent) {
        this.owner = parent;
    }

    private class ConsumerThread extends Thread {
        private final int holderNum;

        public ConsumerThread(final ThreadGroup threadGroup, final int instanceId, final int holderNum) {
            super(threadGroup, JSONToTableWriterAdapter.class.getSimpleName() + instanceId + "_ConsumerThread-" + holderNum);
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
                 This thread doesn't inherently cause the query to stop if it fails, so we want to stop everything
                 on any error.
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
                            log.info().append("JSONToTableWriterAdapter: Processed ").append(intervalMessages).append(" in ")
                                    .append(intervalNanos / NANOS_PER_MILLI).append("ms, ")
                                    .appendDouble(1_000_000_000.0 * intervalMessages / intervalNanos, 6).append(" msgs/sec").endl();
                        } else {
                            log.debug().append("Processed 0 messages in last").append(intervalNanos / NANOS_PER_MILLI).append("ms").endl();
                        }
                        lastReportNanos = afterPoll;
                        lastProcessed = messagesProcessed.longValue();
                        before = lastReportNanos;
                        nextReportTime = before + reportIntervalNanos;
                    }
                }
            } catch (final Exception e) {
                final Exception newException = new JSONIngesterException("Error reporting on message timing!", e);
                log.error().append("Error reporting on message timing: ").append( newException).endl();
                ProcessEnvironment.getGlobalFatalErrorReporter().report("Error reporting JSON message timing!",  newException);
                System.exit(ERROR_REPORTING);
            }
        }
        log.debug().append(Thread.currentThread().getName()).append(" - processMessages - complete").endl();
    }

    @SuppressWarnings("RedundantThrows")
    private void pollOnce(final int holder, final Duration timeout) throws IOException, JSONIngesterException {
        final TextMessageMetadata msgData;
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

    private void processSingleMessage(int holder, TextMessageMetadata msgData) {
        final String msg = msgData.getText();
        holders[holder].setMessageNumber(msgData.getMsgNo());
        // The JSON parsing is time-consuming, so we can multi-thread that.
        final JsonNode record;
        try {
            record = JsonNodeUtil.makeJsonNode(msg);
        } catch (final JsonNodeUtil.JsonStringParseException parseException) {
            // handle JSON parse exception and keep going
            processExceptionRecord(holder, parseException, msgData, msg);
            return;
        }
        if (processArrays && record.isArray()) {
            final int arraySize = record.size();
            if (arraySize == 0) {
                addEmptyHolder(holder);
            } else {
                for (int ii = 0; ii < arraySize; ++ii) {
                    holders[holder].setMessageNumber(msgData.getMsgNo());
                    processOneRecordTopLevel(holder, msgData, record.get(ii), ii == 0, ii == arraySize - 1);
                }
            }
        } else {
            processOneRecordTopLevel(holder, msgData, record, true, true);
        }
    }

    private void processExceptionRecord(final int holder, final IllegalArgumentException iae, final TextMessageMetadata msgData, final String messageText) {
        holders[holder].setParseException(iae);
        holders[holder].setOriginalText(messageText);
        processMetadata(msgData, holder);
        holders[holder].singleRow();
        processHolder(holder, true);
    }

    private void processOneRecordTopLevel(final int holder, final MessageMetadata msgData, final JsonNode record, final boolean isFirst, final boolean isLast) {
        // process the row so that it's ready when our turn comes to write.
        fieldProcessors.forEach(fc -> fc.accept(record, holder));

        final MessageMetadata subtableMessageMetadata = new MessageMetadata() {
            @Override
            public DateTime getSentTime() {
                return msgData.getSentTime();
            }

            @Override
            public DateTime getReceiveTime() {
                return msgData.getReceiveTime();
            }

            @Override
            public DateTime getIngestTime() {
                return msgData.getIngestTime();
            }

            @Override
            public String getMessageId() {
                return msgData.getMessageId();
            }

            @Override
            public long getMsgNo() {
                throw new UnsupportedOperationException("getMsgNo() not supported on subtable message data");
            }
        };

        // TODO: skip this when the current adapter is a subtable adapter
        processMetadata(subtableMessageMetadata, holder);

        for (SubtableData subtableFieldToProcess = subtableProcessingQueue.poll();
             subtableFieldToProcess != null;
             subtableFieldToProcess = subtableProcessingQueue.poll()) {
            final String subtableFieldName = subtableFieldToProcess.fieldName;
            final JSONToTableWriterAdapter subtableAdapter = subtableFieldToProcess.subtableAdapter;
            final JsonNode fieldValue = subtableFieldToProcess.subtableNode;
            final AtomicLong subtableMessageCounter = subtableFieldToProcess.subtableMessageCounter;

            // holder is always 0 (multithreading in subtable adapter not currently supported, as it
            // is more work to ensure the subtable rows appear in the same order as the parent table
            // rows, which is desirable).
            // also note that the subtable holders are distinct from the parent table holders
            final int subtableHolderIdx = 0;


            if (fieldValue == null || fieldValue.isMissingNode()) {
                if (allowMissingKeys) {
                    continue;
                } else {
                    throw new JSONIngesterException("Subtable node \"" + subtableFieldName + "\" is missing but allowMissingKeys is false");
                }
            }

            if (fieldValue.isNull()) {
                if (allowNullValues) {
                    continue;
                } else {
                    throw new JSONIngesterException("Subtable node \"" + subtableFieldName + "\" is null but allowNullValues is false");
                }
            }

            if (!(fieldValue instanceof ArrayNode)) {
                final String fieldType = fieldValue.getClass().getName();
                throw new JSONIngesterException("Expected array node for subtable field \"" + subtableFieldName + "\" but was " + fieldType);
            }
            final ArrayNode subtableArrNode = ((ArrayNode) fieldValue);

            final int nNodes = subtableArrNode.size();
            if (nNodes == 0) {
                continue;
            }

            // A holder must be processed for each value of 'thisSubtableMsgNo'! Gaps are not allowed
            final long thisSubtableMsgNo = subtableMessageCounter.getAndIncrement();
            for (int nodeIdx = 0; nodeIdx < nNodes; nodeIdx++) {
                try {
                    JsonNode subtableRecord = subtableArrNode.get(nodeIdx);

                    final boolean isSubtableFirst = nodeIdx == 0;
                    final boolean isSubtableLast = nodeIdx == nNodes - 1;


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
                    subtableAdapter.processOneRecordTopLevel(subtableHolderIdx, subtableMessageMetadata, subtableRecord, isSubtableFirst, isSubtableLast);
                } catch (Exception ex) {
                    throw new JSONIngesterException("Failed processing subtable field \"" + subtableFieldName + '"', ex);
                }
            }
        }

        // after performing all of the field processing for regular or simple nested fields, we process the array fields
        // the array fields are presumed to be parallel, and may in turn be nested fields.  After processing each array
        // element across our record, we copy the beginning holder elements to a new holder, thus allowing us to expand
        // the non-array elements to all of the logged rows.  Each set of rows from the same message is a transaction.
        if (!arrayFieldNames.isEmpty()) {
            final int nArrayFields = arrayFieldNames.size();
            final ArrayNode[] nodes = new ArrayNode[nArrayFields];
            int expectedLength = -1;
            String lengthFound = null;
            for (int ii = 0; ii < nArrayFields; ++ii) {
                final String fieldName = arrayFieldNames.get(ii);
                final Object object = JsonNodeUtil.getValue(record, fieldName, true, true);
                if (object == null) {
                    continue;
                }
                if (!(object instanceof ArrayNode)) {
                    throw new JSONIngesterException("Expected array node for " + fieldName + " but was " + object.getClass());
                }
                final ArrayNode arrayNode = (ArrayNode) object;
                final int arrayLength = arrayNode.size();
                if (expectedLength >= 0) {
                    if (expectedLength != arrayLength) {
                        throw new JSONIngesterException("Array nodes do not have a consistent length: " + lengthFound + " has length of " + expectedLength + ", " + fieldName + " has length of " + arrayLength);
                    }
                } else {
                    lengthFound = fieldName;
                    expectedLength = arrayLength;
                }
                nodes[ii] = arrayNode;
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
                            throw new JSONIngesterException("Exception occurred while processing array record at index " + expandedRow + " (of " + expectedLength + ')', ex);
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
    }

    /**
     * Handle an empty array of inbound messages.
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
        return new InMemoryRowHolder(fieldSetters.size() + TextMessageMetadata.numberOfMetadataFields());
    }

    @Override
    public void waitForProcessing(final long timeoutMillis) throws InterruptedException, TimeoutException {
        if(numThreads == 0) {
            return;
        }

        // use a loop with sleep instead of wait/notify to avoid any contention with processing threads
        final long startTime = System.currentTimeMillis();
        while (hasUnprocessedMessages()
                && (System.currentTimeMillis() - startTime) < timeoutMillis) {
            //noinspection BusyWait
            Thread.sleep(1);
        }
        if (hasUnprocessedMessages()) {
            throw new TimeoutException();
        }
    }

    private boolean hasUnprocessedMessages() {
        return messagesQueued.longValue() > messagesProcessed.longValue();
    }

    private void processMetadata(final MessageMetadata metadata, final int holderNum) {
//        log.warn("processMetadata() not implemented; doing nothing");
        /*final InMemoryRowHolder holder = holders[holderNum];
        if (owner.getMessageIdColumn() != null) {
            holder.getSetter(owner.getMessageIdColumn(), String.class).set(metadata.getMessageId());
        }
        if (owner.getSendTimeColumn() != null) {
            holder.getSetter(owner.getSendTimeColumn(), long.class).set(metadata.getSentTime());
        }
        if (owner.getReceiveTimeColumn() != null) {
            holder.getSetter(owner.getReceiveTimeColumn(), long.class).set(metadata.getReceiveTime());
        }*/
        // Do not set the 'now' time - we want that to be set as the very last step before writing to disk.
    }

    private static class PermissiveArrayList<T> extends ArrayList<T> {
        @Override
        public void removeRange(final int fromIndex, final int toIndex) {
            super.removeRange(fromIndex, toIndex);
        }
    }

    protected static class SubtableData {
        @NotNull
        private final String fieldName;
        @NotNull
        private final JSONToTableWriterAdapter subtableAdapter;
        @Nullable
        private final JsonNode subtableNode;
        @NotNull
        private final AtomicLong subtableMessageCounter;

        public SubtableData(@NotNull String fieldName,
                            @NotNull JSONToTableWriterAdapter subtableAdapter,
                            @Nullable JsonNode subtableNode,
                            @NotNull AtomicLong subtableMessageCounter) {
            this.fieldName = fieldName;
            this.subtableAdapter = subtableAdapter;
            this.subtableNode = subtableNode;
            this.subtableMessageCounter = subtableMessageCounter;
        }
    }
}