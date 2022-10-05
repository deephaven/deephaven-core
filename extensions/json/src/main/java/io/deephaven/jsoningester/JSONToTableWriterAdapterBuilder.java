package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.base.Pair;
import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import static io.deephaven.jsoningester.JSONToTableWriterAdapter.SUBTABLE_RECORD_ID_COL;

/**
 * The builder configures a factory for StringToTableWriterAdapters that accept JSON strings and
 * writes a table.
 */
@SuppressWarnings("unused")
@ScriptApi
public class JSONToTableWriterAdapterBuilder extends StringMessageToTableAdapter.Builder<StringMessageToTableAdapter<StringMessageHolder>> {
    private int nConsumerThreads = JSONToTableWriterAdapter.N_CONSUMER_THREADS_DEFAULT;

    private StringToTableWriterAdapter stringAdapter;
    private final Map<String, String> columnToJsonField = new HashMap<>();
    private final Map<String, String> columnToParallelField = new HashMap<>();
    private final Map<String, ToIntFunction<JsonNode>> columnToIntFunction = new HashMap<>();
    private final Map<String, ToLongFunction<JsonNode>> columnToLongFunction = new HashMap<>();
    private final Map<String, ToDoubleFunction<JsonNode>> columnToDoubleFunction = new HashMap<>();

    private final Map<String, Pair<JSONToTableWriterAdapterBuilder, TableWriter<?>>> fieldToSubtableBuilders = new HashMap<>();
    private final Map<String, Pair<Class<?>, Function<JsonNode, ?>>> columnToObjectFunction = new HashMap<>();
    private final Set<String> allowedUnmappedColumns = new HashSet<>();
    private final Set<String> nestedColumns = new HashSet<>();
    private final Map<String, JSONToTableWriterAdapterBuilder> nestedFieldBuilders = new HashMap<>();
    private final Map<String, JSONToTableWriterAdapterBuilder> parallelNestedFieldBuilders = new HashMap<>();
    private boolean autoValueMapping = true;
    private boolean allowMissingKeys;
    private boolean allowNullValues;
    private boolean processArrays;

    /**
     * Maps field in the input JSON to column in the output table.
     *
     * @param column name of the column in the output table
     * @param field  name of the field in the input JSON
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addColumnFromField(final String column, final String field) {
        checkAlreadyDefined(column);
        columnToJsonField.put(column, field);
        return this;
    }

    /**
     * Add a parallel array field to this table.
     * <p>
     * All array fields must be parallel (i.e. the same size).  Each element of the array produces a
     * new row.  Non-array fields are replicated to each output row from the message.
     * <p>
     * If an array element is null, then it is ignored.  If all array elements in this table are null, they are null
     * filled to a single output row.
     *
     * @param column the output column
     * @param field  the input field
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addFieldParallel(final String column,
                                                            final String field) {
        checkAlreadyDefined(column);
        columnToParallelField.put(column, field);
        return this;
    }

    /**
     * Add a nested parallel array field to this table.
     * <p>
     * All array fields must be parallel (i.e. the same size).  Each element of the array produces a
     * new row.  Non-array fields are replicated to each output row from the message.  You may have multiply nested
     * fields, but may not have parallel fields within your nested field.
     * <p>
     * If an array element is null, then it is ignored.  If all array elements in this table are null, they are null
     * filled to a single output row.
     *
     * @param field   the input field
     * @param builder the builder for the nested field
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addNestedFieldParallel(final String field, final JSONToTableWriterAdapterBuilder builder) {
        checkNestedBuilder(builder, field);
        addNestedColumns(builder);
        parallelNestedFieldBuilders.put(field, builder);
        return this;
    }

    /**
     * Add a nested field to this table.
     * <p>
     * A nested field is itself a JSON object, with columns outputs defined by the builder.  You may have arbitrarily
     * deeply nested fields.  The nested builder may not have auto value mapping enabled or include any parallel arrays.
     * <p>
     * If a nested field is null, and null values are permitted then all columns derived from the nested field will
     * be null.
     *
     * @param field   the input field
     * @param builder the builder for the nested field
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addNestedField(final String field, final JSONToTableWriterAdapterBuilder builder) {
        checkNestedBuilder(builder, field);
        addNestedColumns(builder);
        nestedFieldBuilders.put(field, builder);
        return this;
    }

    /**
     * Maps field in the input JSON to a new JSONToTableWriterAdapterBuilder, to produce a different table from
     * the JSON.
     * <p>
     * @param field JSON field that is mapped to a different table
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addFieldToSubTableMapping(final String field,
                                                                     final JSONToTableWriterAdapterBuilder subtableBuilder,
                                                                     final TableWriter<?> subtableWriter
    ) {
        checkSubtableBuilder(subtableBuilder, field);
        addNestedColumns(subtableBuilder);
        fieldToSubtableBuilders.put(field, new Pair<>(subtableBuilder, subtableWriter));
        return this;
    }

    private void addNestedColumns(final JSONToTableWriterAdapterBuilder builder) {
        final Collection<String> nestedColumns = builder.getDefinedColumns();
        nestedColumns.forEach(this::checkAlreadyDefined);
        this.nestedColumns.addAll(nestedColumns);
    }

    private void checkNestedBuilder(final JSONToTableWriterAdapterBuilder builder, final String field) {
        if (builder.autoValueMapping) {
            throw new JSONIngesterException("Auto value mapping is not supported for nested field, " + field + "!");
        }
        if (!builder.columnToParallelField.isEmpty() || !builder.parallelNestedFieldBuilders.isEmpty()) {
            throw new JSONIngesterException("Nested fields may not contain parallel array fields, " + field + "!");
        }
        if (!builder.allowedUnmappedColumns.isEmpty()) {
            throw new JSONIngesterException("Nested fields may not define unmapped fields, " + field + "!");
        }
        if (!builder.getInternalColumns().isEmpty()) {
            throw new JSONIngesterException("Nested fields may not define message header columns field " + field + ", columns=" + builder.getInternalColumns());
        }
    }

    private void checkSubtableBuilder(final JSONToTableWriterAdapterBuilder builder, final String field) {
        if (!builder.allowedUnmappedColumns.isEmpty()) {
            throw new JSONIngesterException("Nested fields may not define unmapped fields, " + field + "!");
        }
        if (!builder.getInternalColumns().isEmpty()) {
            throw new JSONIngesterException("Nested fields may not define message header columns field " + field + ", columns=" + builder.getInternalColumns());
        }
    }

    /**
     * Maps an int column in the output table to the result of a ToIntFunction.
     * <p>
     * Using simple column to field mappings does not allow you to reference anything but simple flat fields in the
     * JsonNode, transform the type of your result, use multiple input fields for each column, or perform complex
     * logic.  The ToIntFunction allows you to read arbitrary fields from the JsonNode to produce an integer value.
     * <p>
     * If your function returns {@code io.deephaven.util.QueryConstants#NULL_INT}, then the result column will be null.
     *
     * @param column        name of the column in the output table
     * @param toIntFunction function to apply to the JsonNode to determine this column's value
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addColumnFromIntFunction(final String column,
                                                                    final ToIntFunction<JsonNode> toIntFunction) {
        checkAlreadyDefined(column);
        columnToIntFunction.put(column, toIntFunction);
        return this;
    }

    /**
     * Maps a long column in the output table to the result of a ToLongFunction.
     * <p>
     * Using simple column to field mappings does not allow you to reference anything but simple flat fields in the
     * JsonNode, transform the type of your result, use multiple input fields for each column, or perform complex
     * logic.  The ToLongFunction allows you to read arbitrary fields from the JsonNode to produce a numeric value.
     * <p>
     * If your function returns {@code io.deephaven.util.QueryConstants#NULL_LONG}, then the result column will be null.
     *
     * @param column         name of the column in the output table
     * @param toLongFunction function to apply to the JsonNode to determine this column's value
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addColumnFromLongFunction(final String column,
                                                                     final ToLongFunction<JsonNode> toLongFunction) {
        checkAlreadyDefined(column);
        columnToLongFunction.put(column, toLongFunction);
        return this;
    }

    /**
     * Maps a double column in the output table to the result of a ToDoubleFunction.
     * <p>
     * Using simple column to field mappings does not allow you to reference anything but simple flat fields in the
     * JsonNode, transform the type of your result, use multiple input fields for each column, or perform complex
     * logic.  The ToDoubleFunction allows you to read arbitrary fields from the JsonNode to produce a numeric value.
     * <p>
     * If your function returns {@code io.deephaven.util.QueryConstants#NULL_DOUBLE}, then the result column will be null.
     *
     * @param column           name of the column in the output table
     * @param toDoubleFunction function to apply to the JsonNode to determine this column's value
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addColumnFromDoubleFunction(final String column,
                                                                       final ToDoubleFunction<JsonNode> toDoubleFunction) {
        checkAlreadyDefined(column);
        columnToDoubleFunction.put(column, toDoubleFunction);
        return this;
    }

    /**
     * Maps a column in the output table to the result of a variable-output-type Function.
     * <p>
     * Using simple column to field mappings does not allow you to reference anything but simple flat fields in the
     * JsonNode, transform the type of your result, use multiple input fields for each column, or perform complex
     * logic.  The ToLongFunction allows you to read arbitrary fields from the JsonNode to produce a value of various types.
     *
     * @param column     name of the column in the output table
     * @param returnType the return type of the function, which must match the type of the column in the table
     * @param function   function to apply to the JsonNode to determine this column's value
     * @param <R>        the return type of function
     * @return this builder
     */
    @ScriptApi
    public <R> JSONToTableWriterAdapterBuilder addColumnFromFunction(final String column,
                                                                     final Class<R> returnType,
                                                                     final Function<JsonNode, R> function) {
        checkAlreadyDefined(column);
        columnToObjectFunction.put(column, new Pair<>(returnType, function));
        return this;
    }

    /**
     * Indicates that a column is unmapped (and therefore null filled).
     *
     * @param column name of the column in the output table
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder allowUnmapped(final String column) {
        checkAlreadyDefined(column);
        allowedUnmappedColumns.add(column);
        return this;
    }

    /**
     * Indicates that if our top level JSON is an array we should process each element as a record
     *
     * @param processArrays should we allow arrays as our JSON input
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder processArrays(final boolean processArrays) {
        this.processArrays = processArrays;
        return this;
    }

    private void checkAlreadyDefined(final String column) {
        if (isDefined(column)) {
            throw new JSONIngesterException("Column is already defined " + column);
        }
    }

    private boolean isDefined(final String column) {
        return allowedUnmappedColumns.contains(column) ||
                columnToJsonField.containsKey(column) ||
                columnToIntFunction.containsKey(column) ||
                columnToLongFunction.containsKey(column) ||
                columnToDoubleFunction.containsKey(column) ||
                columnToObjectFunction.containsKey(column) ||
                columnToParallelField.containsKey(column);
    }

    protected Collection<String> getDefinedColumns() {
        final List<String> definedColumns = new ArrayList<>();
        definedColumns.addAll(allowedUnmappedColumns);
        definedColumns.addAll(columnToJsonField.keySet());
        definedColumns.addAll(columnToIntFunction.keySet());
        definedColumns.addAll(columnToLongFunction.keySet());
        definedColumns.addAll(columnToDoubleFunction.keySet());
        definedColumns.addAll(columnToObjectFunction.keySet());
        definedColumns.addAll(columnToParallelField.keySet());
        definedColumns.addAll(nestedColumns);
        fieldToSubtableBuilders.keySet().stream().map(JSONToTableWriterAdapter::getSubtableRowIdColName).forEach(definedColumns::add);
        return Collections.unmodifiableList(definedColumns);
    }

    /**
     * Enables or disables automatic value mapping (true by default).
     * <p>
     * If auto value mapping is enabled, any column that was not defined [either by allowUnmapped,
     * setTimestampColumnName, setMessageIdColumnName, or addColumnFromField] is automatically mapped to a JSON
     * field of the same name.
     *
     * @param autoValueMapping should automatic value mapping be enabled
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder autoValueMapping(final boolean autoValueMapping) {
        this.autoValueMapping = autoValueMapping;
        return this;
    }

    /**
     * Set whether to allow missing keys in JSON records.
     * <p>
     * If set to true, then JSON records that have missing keys produce a null value in the output table. If
     * set to false, then JSON records that have missing keys result in an exception.
     *
     * @param allowMissingKeys if JSON records should be permitted to have missing keys.
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder allowMissingKeys(final boolean allowMissingKeys) {
        this.allowMissingKeys = allowMissingKeys;
        return this;
    }

    /**
     * Set whether to allow null values in JSON records.
     * <p>
     * If set to true, then JSON records that have null values produce a null value in the output table. If set
     * to false, then JSON records that have null values result in an exception.
     *
     * @param allowNullValues if JSON records should be permitted to have missing keys.
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder allowNullValues(final boolean allowNullValues) {
        this.allowNullValues = allowNullValues;
        return this;
    }

    @Override
    public JSONToTableWriterAdapterBuilder sendTimestampColumnName(final String sendTimestampColumnName) {
        return (JSONToTableWriterAdapterBuilder) super.sendTimestampColumnName(sendTimestampColumnName);
    }

    @Override
    public JSONToTableWriterAdapterBuilder receiveTimestampColumnName(final String receiveTimestampColumnName) {
        return (JSONToTableWriterAdapterBuilder) super.receiveTimestampColumnName(receiveTimestampColumnName);
    }

    @Override
    public JSONToTableWriterAdapterBuilder timestampColumnName(final String timestampColumnName) {
        return (JSONToTableWriterAdapterBuilder) super.timestampColumnName(timestampColumnName);
    }

    @Override
    public JSONToTableWriterAdapterBuilder messageIdColumnName(final String messageIdColumnName) {
        return (JSONToTableWriterAdapterBuilder) super.messageIdColumnName(messageIdColumnName);
    }

    public JSONToTableWriterAdapterBuilder nConsumerThreads(final int nConsumerThreads) {
        this.nConsumerThreads = nConsumerThreads;
        return this;
    }

    /**
     * Create a factory according to the specifications of this builder.
     *
     * @return a factory for creating a StringTableWriterAdapter from a TableWriter
     */
    public Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> buildFactory(@NotNull final Logger log) {
        // TODO: buildFactory() should happen in StringMessageToTableAdapter and take a StringMessageToTableAdapter.Builder as an argument
        return buildFactory(log,
                StringMessageHolder.class,
                StringMessageHolder::getMsg,
                StringMessageHolder::getSendTimeMicros,
                StringMessageHolder::getRecvTimeMicros);
    }



    public <M> Function<TableWriter<?>, StringMessageToTableAdapter<M>> buildFactory(@NotNull final Logger log,
                                                                                     final Class<M> messageType,
                                                                                     final Function<M, String> messageToText,
                                                                                     final ToLongFunction<M> messageToSendTimeMicros,
                                                                                     final ToLongFunction<M> messageToRecvTimeMicros
                                                                                     ) {
        //noinspection ConstantConditions
        if (log == null) {
            throw new NullPointerException("Log passed to buildFactory must not be null!");
        }
        return (tw) -> {
            final Set<String> allUnmapped = new HashSet<>(allowedUnmappedColumns);
            allUnmapped.addAll(getInternalColumns());
            final StringToTableWriterAdapter stringAdapter = makeAdapter(log, tw, allUnmapped, true);
            return buildInternal(tw,
                    stringAdapter,
                    messageToText,
                    messageToSendTimeMicros,
                    messageToRecvTimeMicros);
        };
    }

    @NotNull
    protected JSONToTableWriterAdapter makeAdapter(@NotNull final Logger log,
                                                   final TableWriter<?> tw,
                                                   final Set<String> allUnmapped,
                                                   final boolean createHolders) {
        return new JSONToTableWriterAdapter(tw, log, allowMissingKeys, allowNullValues, processArrays,
                nConsumerThreads,
                columnToJsonField,
                columnToIntFunction,
                columnToLongFunction,
                columnToDoubleFunction,
                columnToObjectFunction,
                nestedFieldBuilders,
                columnToParallelField,
                parallelNestedFieldBuilders,
                fieldToSubtableBuilders,
                allUnmapped,
                autoValueMapping,
                createHolders);
    }

    /**
     * Creates an adapter to be used as a nested adapter. Nested  adapters always have a single consumer thread,
     * must use the same subtableProcessingQueue as the parent adapter, and always create new holders.
     * @param log
     * @param tw
     * @param allUnmapped
     * @param subtableProcessingQueue
     * @return
     */
    @NotNull
    protected JSONToTableWriterAdapter makeNestedAdapter(@NotNull final Logger log,
                                                   final TableWriter<?> tw,
                                                   final Set<String> allUnmapped,
                                                   final ThreadLocal<Queue<JSONToTableWriterAdapter.SubtableData>> subtableProcessingQueue
    ) {
        final int nThreads = 0; // nested adapters don't need threads
        final boolean createHolders = false; // parent adapters create the holders

        // nested adapters must allow missing keys, because all nested keys will appear as 'missing' if the
        // parent adapters allow missing keys or values and the nested adapter's field is missing.s
        // TODO: allowMissingKeys for nested adapters should be handled independently
        final boolean allowMissingKeys = true;

        return new JSONToTableWriterAdapter(tw,
                log,
                allowMissingKeys,
                allowNullValues,
                processArrays,
                nThreads,
                columnToJsonField,
                columnToIntFunction,
                columnToLongFunction,
                columnToDoubleFunction,
                columnToObjectFunction,
                nestedFieldBuilders,
                columnToParallelField,
                parallelNestedFieldBuilders,
                fieldToSubtableBuilders,
                allUnmapped,
                autoValueMapping,
                createHolders,
                subtableProcessingQueue
                );
    }

    /**
     * Creates an adapter to be used as a subtable adapter. Subtable adapters always have a single consumer thread,
     * must use the same subtableProcessingQueue as the parent adapter, and always create new holders.
     *
     * @param log Logger to use
     * @param tw Subtable's table writer
     * @param allUnmapped Columns that are not required to be mapped to JSON data
     * @param subtableProcessingQueue The queue to which subtable records to process are added
     * @param subtableRecordCounter
     * @return
     */
    @NotNull
    protected JSONToTableWriterAdapter makeSubtableAdapter(@NotNull final Logger log,
                                                           final TableWriter<?> tw,
                                                           final Set<String> allUnmapped,
                                                           final ThreadLocal<Queue<JSONToTableWriterAdapter.SubtableData>> subtableProcessingQueue,
                                                           final ThreadLocal<MutableLong> subtableRecordCounter) {

        // make a copy of the columnToLong (do not mutate the map from the original builder)
        final HashMap<String, ToLongFunction<JsonNode>> columnToLongFunctionForSubtableAdapter = new HashMap<>(columnToLongFunction);

        // add a processor that records the subtableRecordCounter (which maps to the row in the parent).
        columnToLongFunctionForSubtableAdapter.put(
                SUBTABLE_RECORD_ID_COL, value -> {
                    // just return the subtable record ID that's set by the parent's field processor
                    return subtableRecordCounter.get().longValue();
                }
        );


        return new JSONToTableWriterAdapter(tw, log, allowMissingKeys, allowNullValues, processArrays,
                0,
                columnToJsonField, columnToIntFunction, columnToLongFunctionForSubtableAdapter, columnToDoubleFunction, columnToObjectFunction,
                nestedFieldBuilders,
                columnToParallelField,
                parallelNestedFieldBuilders,
                fieldToSubtableBuilders,
                Collections.emptySet(),
                autoValueMapping,
                true,
                subtableProcessingQueue);
    }

}
