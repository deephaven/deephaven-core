package io.deephaven.jsoningester;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.base.Pair;
import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

import static io.deephaven.jsoningester.JSONToTableWriterAdapter.SUBTABLE_RECORD_ID_COL;

/**
 * The builder configures a factory for StringToTableWriterAdapters that accept JSON strings and writes a table.
 */
public class JSONToTableWriterAdapterBuilder extends StringMessageToTableAdapter.Builder<JSONToTableWriterAdapter> {
    private int nConsumerThreads = JSONToTableWriterAdapter.N_CONSUMER_THREADS_DEFAULT;

    private StringToTableWriterAdapter stringAdapter;
    private final Map<String, String> columnToJsonField = new HashMap<>();

    private final Map<String, JsonPointer> columnToJsonPointer = new HashMap<>();
    private final Map<String, String> columnToParallelField = new HashMap<>();
    private final Map<String, ToIntFunction<JsonNode>> columnToIntFunction = new HashMap<>();
    private final Map<String, ToLongFunction<JsonNode>> columnToLongFunction = new HashMap<>();
    private final Map<String, ToDoubleFunction<JsonNode>> columnToDoubleFunction = new HashMap<>();

    private final Map<String, JSONToTableWriterAdapterBuilder> fieldToSubtableBuilders = new HashMap<>();
    private final Map<String, JSONToTableWriterAdapter.RoutedAdapterInfo> routedTableIdsToBuilders = new HashMap<>();
    private final Map<String, Pair<Class<?>, Function<JsonNode, ?>>> columnToObjectFunction = new HashMap<>();
    private final Set<String> allowedUnmappedColumns = new HashSet<>();
    private final Set<String> nestedColumns = new HashSet<>();
    private final Map<String, JSONToTableWriterAdapterBuilder> nestedFieldBuilders = new HashMap<>();
    private final Map<String, JSONToTableWriterAdapterBuilder> parallelNestedFieldBuilders = new HashMap<>();

    private BiConsumer<MessageMetadata, JsonNode> postProcessConsumer;

    private boolean autoValueMapping = true;
    private boolean allowMissingKeys;
    private boolean allowNullValues;
    private boolean processArrays;

    public JSONToTableWriterAdapterBuilder() {}

    public JSONToTableWriterAdapterBuilder(JSONToTableWriterAdapterBuilder other) {
        this.nConsumerThreads = other.nConsumerThreads;
        this.stringAdapter = other.stringAdapter;
        this.autoValueMapping = other.autoValueMapping;
        this.allowMissingKeys = other.allowMissingKeys;
        this.allowNullValues = other.allowNullValues;
        this.processArrays = other.processArrays;

        this.columnToJsonField.putAll(other.columnToJsonField);
        this.columnToJsonPointer.putAll(other.columnToJsonPointer);
        this.columnToParallelField.putAll(other.columnToParallelField);
        this.columnToIntFunction.putAll(other.columnToIntFunction);
        this.columnToLongFunction.putAll(other.columnToLongFunction);
        this.columnToDoubleFunction.putAll(other.columnToDoubleFunction);

        this.columnToObjectFunction.putAll(other.columnToObjectFunction);
        this.allowedUnmappedColumns.addAll(other.allowedUnmappedColumns);
        this.nestedColumns.addAll(other.nestedColumns);
        this.nestedFieldBuilders.putAll(other.nestedFieldBuilders);
        this.parallelNestedFieldBuilders.putAll(other.parallelNestedFieldBuilders);
        this.postProcessConsumer = other.postProcessConsumer;

        for (Map.Entry<String, JSONToTableWriterAdapterBuilder> entry : other.fieldToSubtableBuilders.entrySet()) {
            final JSONToTableWriterAdapterBuilder subtableBuilderCopy =
                    new JSONToTableWriterAdapterBuilder(entry.getValue());
            this.fieldToSubtableBuilders.put(entry.getKey(), subtableBuilderCopy);
        }
    }

    /**
     * Maps field in the input JSON to column in the output table.
     *
     * @param column name of the column in the output table
     * @param field name of the field in the input JSON
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
     * All array fields must be parallel (i.e. the same size). Each element of the array produces a new row. Non-array
     * fields are replicated to each output row from the message.
     * <p>
     * If an array element is null, then it is ignored. If all array elements in this table are null, they are null
     * filled to a single output row.
     *
     * @param column the output column
     * @param field the input field
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
     * All array fields must be parallel (i.e. the same size). Each element of the array produces a new row. Non-array
     * fields are replicated to each output row from the message. You may have multiply nested fields, but may not have
     * parallel fields within your nested field.
     * <p>
     * If an array element is null, then it is ignored. If all array elements in this table are null, they are null
     * filled to a single output row.
     *
     * @param field the input field
     * @param builder the builder for the nested field
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addNestedFieldParallel(final String field,
            final JSONToTableWriterAdapterBuilder builder) {
        checkNestedBuilder(builder, field);
        addNestedColumns(builder);
        parallelNestedFieldBuilders.put(field, builder);
        return this;
    }

    /**
     * Add a nested field to this table.
     * <p>
     * A nested field is itself a JSON object, with columns outputs defined by the builder. You may have arbitrarily
     * deeply nested fields. The nested builder may not have auto value mapping enabled or include any parallel arrays.
     * <p>
     * If a nested field is null, and null values are permitted then all columns derived from the nested field will be
     * null.
     *
     * @param field the input field
     * @param builder the builder for the nested field
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addNestedField(
            final String field,
            final JSONToTableWriterAdapterBuilder builder) {
        checkNestedBuilder(builder, field);
        addNestedColumns(builder);
        nestedFieldBuilders.put(field, builder);
        return this;
    }

    /**
     * Maps field in the input JSON to a new JSONToTableWriterAdapterBuilder, to produce a different table from the
     * JSON.
     * <p>
     *
     * @param field JSON field that is mapped to a different table
     * @return this builder
     */
    @ScriptApi
    public JSONToTableWriterAdapterBuilder addFieldToSubTableMapping(
            final String field,
            final JSONToTableWriterAdapterBuilder subtableBuilder) {
        checkSubtableBuilder(subtableBuilder, field);
        fieldToSubtableBuilders.put(field, subtableBuilder);
        return this;
    }

    @ScriptApi
    public JSONToTableWriterAdapterBuilder addRoutedTableAdapter(
            final String routedTableIdentifier,
            final Predicate<JsonNode> routingPredicate,
            final JSONToTableWriterAdapterBuilder subtableBuilder) {
        checkSubtableBuilder(subtableBuilder, routedTableIdentifier);
        routedTableIdsToBuilders.put(routedTableIdentifier,
                new JSONToTableWriterAdapter.RoutedAdapterInfo(subtableBuilder, routingPredicate));
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
            throw new JSONIngesterException("Nested fields may not define message header columns field " + field
                    + ", columns=" + builder.getInternalColumns());
        }
    }

    private void checkSubtableBuilder(final JSONToTableWriterAdapterBuilder builder, final String field) {
        if (!builder.getInternalColumns().isEmpty()) {
            throw new JSONIngesterException("Subtables may not define message header columns field " + field
                    + ", columns=" + builder.getInternalColumns());
        }
    }

    /**
     * Maps an int column in the output table to the result of a ToIntFunction.
     * <p>
     * Using simple column to field mappings does not allow you to reference anything but simple flat fields in the
     * JsonNode, transform the type of your result, use multiple input fields for each column, or perform complex logic.
     * The ToIntFunction allows you to read arbitrary fields from the JsonNode to produce an integer value.
     * <p>
     * If your function returns {@code io.deephaven.util.QueryConstants#NULL_INT}, then the result column will be null.
     *
     * @param column name of the column in the output table
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
     * JsonNode, transform the type of your result, use multiple input fields for each column, or perform complex logic.
     * The ToLongFunction allows you to read arbitrary fields from the JsonNode to produce a numeric value.
     * <p>
     * If your function returns {@code io.deephaven.util.QueryConstants#NULL_LONG}, then the result column will be null.
     *
     * @param column name of the column in the output table
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
     * JsonNode, transform the type of your result, use multiple input fields for each column, or perform complex logic.
     * The ToDoubleFunction allows you to read arbitrary fields from the JsonNode to produce a numeric value.
     * <p>
     * If your function returns {@code io.deephaven.util.QueryConstants#NULL_DOUBLE}, then the result column will be
     * null.
     *
     * @param column name of the column in the output table
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
     * JsonNode, transform the type of your result, use multiple input fields for each column, or perform complex logic.
     * The ToLongFunction allows you to read arbitrary fields from the JsonNode to produce a value of various types.
     *
     * @param column name of the column in the output table
     * @param returnType the return type of the function, which must match the type of the column in the table
     * @param function function to apply to the JsonNode to determine this column's value
     * @param <R> the return type of function
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

    public JSONToTableWriterAdapterBuilder addColumnFromPointer(final String column, final String jsonPointer) {
        return addColumnFromPointer(column, JsonPointer.compile(jsonPointer));
    }

    public JSONToTableWriterAdapterBuilder addColumnFromPointer(final String column, final JsonPointer jsonPointer) {
        columnToJsonPointer.put(column, jsonPointer);
        return this;
    }

    public void setPostProcessConsumer(BiConsumer<MessageMetadata, JsonNode> postProcessConsumer) {
        this.postProcessConsumer = postProcessConsumer;
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
        if (allowedUnmappedColumns.contains(column)) {
            throw new JSONIngesterException("Column \"" + column + "\" is already defined: allowed unmapped");
        } else if (columnToJsonField.containsKey(column)) {
            throw new JSONIngesterException("Column \"" + column + "\" is already defined: mapped to field \""
                    + columnToJsonField.get(column) + '"');
        } else if (columnToJsonPointer.containsKey(column)) {
            throw new JSONIngesterException("Column \"" + column + "\" is already defined: mapped to JsonPointer \""
                    + columnToJsonPointer.get(column) + '"');
        } else if (columnToIntFunction.containsKey(column)) {
            throw new JSONIngesterException("Column \"" + column + "\" is already defined: mapped to int function");
        } else if (columnToLongFunction.containsKey(column)) {
            throw new JSONIngesterException("Column \"" + column + "\" is already defined: mapped to long function");
        } else if (columnToDoubleFunction.containsKey(column)) {
            throw new JSONIngesterException("Column \"" + column + "\" is already defined: mapped to double function");
        } else if (columnToObjectFunction.containsKey(column)) {
            throw new JSONIngesterException("Column \"" + column + "\" is already defined: mapped to object function");
        } else if (columnToParallelField.containsKey(column)) {
            throw new JSONIngesterException(
                    "Column \"" + column + "\" is already defined: mapped to parallel array field \""
                            + columnToParallelField.get(column) + '"');
        }
    }

    protected Collection<String> getDefinedColumns() {
        final List<String> definedColumns = new ArrayList<>();
        definedColumns.addAll(allowedUnmappedColumns);
        definedColumns.addAll(columnToJsonField.keySet());
        definedColumns.addAll(columnToJsonPointer.keySet());
        definedColumns.addAll(columnToIntFunction.keySet());
        definedColumns.addAll(columnToLongFunction.keySet());
        definedColumns.addAll(columnToDoubleFunction.keySet());
        definedColumns.addAll(columnToObjectFunction.keySet());
        definedColumns.addAll(columnToParallelField.keySet());
        definedColumns.addAll(nestedColumns);
        Stream.concat(fieldToSubtableBuilders.keySet().stream(), routedTableIdsToBuilders.keySet().stream())
                .map(JSONToTableWriterAdapter::getSubtableRowIdColName)
                .forEach(definedColumns::add);
        return Collections.unmodifiableList(definedColumns);
    }

    /**
     * Enables or disables automatic value mapping (true by default).
     * <p>
     * If auto value mapping is enabled, any column that was not defined [either by allowUnmapped,
     * setTimestampColumnName, setMessageIdColumnName, or addColumnFromField] is automatically mapped to a JSON field of
     * the same name.
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
     * If set to true, then JSON records that have missing keys produce a null value in the output table. If set to
     * false, then JSON records that have missing keys result in an exception.
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
     * If set to true, then JSON records that have null values produce a null value in the output table. If set to
     * false, then JSON records that have null values result in an exception.
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

    @NotNull
    public JSONToTableWriterAdapter makeAdapter(@NotNull final Logger log,
            final TableWriter<?> tw) {
        return makeAdapter(log, tw, Collections.emptyMap());
    }

    @NotNull
    public JSONToTableWriterAdapter makeAdapter(@NotNull final Logger log,
            @NotNull final TableWriter<?> tw,
            @NotNull final Map<String, TableWriter<?>> subtableFieldToTableWriters) {
        final Set<String> allUnmapped = new HashSet<>(allowedUnmappedColumns);
        allUnmapped.addAll(getInternalColumns());

        // Always create message holders in the "outer-level" adapter. Only nested adapters do not create holders.
        final boolean createHolders = true;
        return new JSONToTableWriterAdapter(tw, log, allowMissingKeys, allowNullValues, processArrays,
                nConsumerThreads,
                columnToJsonField,
                columnToJsonPointer,
                columnToIntFunction,
                columnToLongFunction,
                columnToDoubleFunction,
                columnToObjectFunction,
                nestedFieldBuilders,
                columnToParallelField,
                parallelNestedFieldBuilders,
                subtableFieldToTableWriters,
                fieldToSubtableBuilders,
                routedTableIdsToBuilders,
                allUnmapped,
                autoValueMapping,
                createHolders,
                postProcessConsumer);
    }

    /**
     * Creates an adapter to be used as a nested adapter. Nested adapters always have a single consumer thread, must use
     * the same subtableProcessingQueue as the parent adapter, and always create new holders.
     *
     * @param log Logger to use
     * @param tw The table writer (same as parent)
     * @param allUnmapped Columns that are not required to be mapped to JSON data
     * @param subtableProcessingQueue The queue to which subtable records to process are added (i.e. the parent's queue)
     * @return An adapter that provides field processors for nested fields within a JSON message
     */
    @NotNull
    JSONToTableWriterAdapter makeNestedAdapter(@NotNull final Logger log,
            @NotNull final TableWriter<?> tw,
            @NotNull final Map<String, TableWriter<?>> fieldToSubtableWriters,
            @NotNull final Set<String> allUnmapped,
            @NotNull final ThreadLocal<Queue<JSONToTableWriterAdapter.SubtableData>> subtableProcessingQueue) {
        final int nThreads = 0; // nested adapters don't need threads

        final boolean createHolders = false; // parent adapters create the holders

        // nested adapters must allow missing keys, because all nested keys will appear as 'missing' if the
        // parent adapters allow missing keys or values and the nested adapter's field is missing.s
        // TODO: allowMissingKeys for nested adapters should be handled independently
        final boolean allowMissingKeys = true;

        final boolean isSubtableAdapter = false;

        return new JSONToTableWriterAdapter(tw,
                log,
                allowMissingKeys,
                allowNullValues,
                processArrays,
                nThreads,
                columnToJsonField,
                columnToJsonPointer,
                columnToIntFunction,
                columnToLongFunction,
                columnToDoubleFunction,
                columnToObjectFunction,
                nestedFieldBuilders,
                columnToParallelField,
                parallelNestedFieldBuilders,
                fieldToSubtableWriters,
                fieldToSubtableBuilders,
                routedTableIdsToBuilders,
                allUnmapped,
                autoValueMapping,
                createHolders,
                subtableProcessingQueue,
                postProcessConsumer,
                isSubtableAdapter);
    }

    /**
     * Creates an adapter to be used as a subtable adapter. Subtable adapters always have a single consumer thread, must
     * use the same subtableProcessingQueue as the parent adapter, and always create new holders.
     *
     * @param log Logger to use
     * @param tw Subtable's table writer
     * @param allUnmapped Columns that are not required to be mapped to JSON data
     * @param subtableProcessingQueue The queue to which subtable records to proces are added (i.e. the parent's queue)
     * @param subtableRecordCounter A ThreadLocal reference to a long, which will contain this subtable's record ID.
     * @return An adapter that logs records to a separate table
     */
    @NotNull
    JSONToTableWriterAdapter makeSubtableAdapter(@NotNull final Logger log,
            @NotNull final TableWriter<?> tw,
            @NotNull final Map<String, TableWriter<?>> fieldToSubtableWriters,
            final Set<String> allUnmapped,
            final ThreadLocal<Queue<JSONToTableWriterAdapter.SubtableData>> subtableProcessingQueue,
            final ThreadLocal<MutableLong> subtableRecordCounter) {

        // make a copy of the columnToLongFunction map (do not mutate the map from the original builder)
        final HashMap<String, ToLongFunction<JsonNode>> columnToLongFunctionForSubtableAdapter =
                new HashMap<>(columnToLongFunction);

        // add a processor that records the subtableRecordCounter (which maps to the row in the parent).
        columnToLongFunctionForSubtableAdapter.put(
                SUBTABLE_RECORD_ID_COL, value -> {
                    // just return the subtable record ID that's set by the parent's field processor
                    return subtableRecordCounter.get().longValue();
                });

        final boolean createHolders = true;

        final boolean isSubtableAdapter = true;

        return new JSONToTableWriterAdapter(tw, log, allowMissingKeys, allowNullValues, processArrays,
                0,
                columnToJsonField, columnToJsonPointer, columnToIntFunction, columnToLongFunctionForSubtableAdapter,
                columnToDoubleFunction,
                columnToObjectFunction,
                nestedFieldBuilders,
                columnToParallelField,
                parallelNestedFieldBuilders,
                fieldToSubtableWriters,
                fieldToSubtableBuilders,
                routedTableIdsToBuilders,
                Collections.emptySet(),
                autoValueMapping,
                createHolders,
                subtableProcessingQueue,
                postProcessConsumer,
                isSubtableAdapter);
    }

}
