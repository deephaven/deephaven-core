package io.deephaven.jsoningester;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.DynamicTableWriter;
import io.deephaven.io.logger.Logger;
import io.deephaven.qst.type.Type;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

/**
 * Created by rbasralian on 10/14/22
 */
public class JSONToInMemoryTableAdapterBuilder {

    private static final AtomicLong adapterCounter = new AtomicLong(0);

    private final String mainResultTableName;

    private final JSONToTableWriterAdapterBuilder jsonAdpaterBuilder = new JSONToTableWriterAdapterBuilder()
            .autoValueMapping(false);

    private final Map<String, JSONToInMemoryTableAdapterBuilder> subtableFieldsToBuilders = new LinkedHashMap<>();

    private final Map<String, JSONToInMemoryTableAdapterBuilder> routedTableIdsToBuilders = new HashMap<>();

    private final List<String> colNames = new ArrayList<>();
    private final List<Class<?>> colTypes = new ArrayList<>();
    private long flushIntervalMillis = 5000L;

    public static class TableAndAdapterBuilderResult {

        public final Map<String, Table> resultTables;

        public final Map<String, TableWriter<?>> tableWriters;

        public final JSONToTableWriterAdapter tableWriterAdapter;

        public TableAndAdapterBuilderResult(Map<String, Table> resultTables, Map<String, TableWriter<?>> tableWriters,
                JSONToTableWriterAdapter tableWriterAdapter) {
            this.resultTables = Collections.unmodifiableMap(resultTables);
            this.tableWriters = Collections.unmodifiableMap(tableWriters);
            this.tableWriterAdapter = tableWriterAdapter;
        }

        /**
         * Calls {@link JSONToTableWriterAdapter#shutdown() shutdown()} on the table writer adapter, which will also
         * {@link TableWriter#close() close()} the TableWriters.
         */
        public void shutdown() {
            tableWriterAdapter.shutdown();
        }
    }

    public JSONToInMemoryTableAdapterBuilder() {
        this(null);
    }

    public JSONToInMemoryTableAdapterBuilder(final String mainResultTableName) {
        this.mainResultTableName = mainResultTableName;
    }

    public TableAndAdapterBuilderResult build(Logger log) {
        final String mainResultTableName;
        if (this.mainResultTableName == null) {
            mainResultTableName = "tableFromJSON_" + adapterCounter.getAndIncrement();
        } else {
            mainResultTableName = this.mainResultTableName;
        }

        final Map<String, Table> resultTables = new LinkedHashMap<>();
        final Map<String, TableWriter<?>> resultTableWriters = new LinkedHashMap<>();

        // Map of all subtable writers for the main builder and its nested/subtable builders. (This includes routed
        // table adapters.) Note that since this is keyed off of a field name and is a global map for the entire JSON
        // tree, it means a given key (i.e. field name or routed table identifier) can only be used for one subtable
        // anywhere in the tree. So, if two nested fields both have an array field of the same name, only one of them
        // would be able to be used as a subtable.
        final Map<String, TableWriter<?>> routedAndSubtableWriters = new LinkedHashMap<>();

        // Make this table's tablewriter
        final DynamicTableWriter thisTableWriter = getTableWriter(false);
        resultTableWriters.put(mainResultTableName, thisTableWriter);
        resultTables.put(mainResultTableName, thisTableWriter.getTable());

        subtableFieldsToBuilders.forEach((k, v) -> {
            final DynamicTableWriter subtableTableWriter = v.getTableWriter(true);
            routedAndSubtableWriters.put(k, subtableTableWriter);
            resultTableWriters.put(k, subtableTableWriter);
            resultTables.put(k, subtableTableWriter.getTable());
        });

        routedTableIdsToBuilders.forEach((k, v) -> {
            final DynamicTableWriter routedTableWriter = v.getTableWriter(true);
            if (routedAndSubtableWriters.put(k, routedTableWriter) != null) {
                throw new IllegalStateException("key " + k + " used for both routed table and subtable");
            }
            resultTableWriters.put(k, routedTableWriter);
            resultTables.put(k, routedTableWriter.getTable());
        });

        final JSONToTableWriterAdapter jsonAdapter =
                jsonAdpaterBuilder.makeAdapter(log, thisTableWriter, routedAndSubtableWriters);

        if (flushIntervalMillis > 0) {
            jsonAdapter.createCleanupThread(flushIntervalMillis);
        }

        return new TableAndAdapterBuilderResult(resultTables, resultTableWriters, jsonAdapter);
    }

    @NotNull
    private DynamicTableWriter getTableWriter(final boolean subtable) {
        String[] colNames = this.colNames.toArray(new String[0]);
        Type<?>[] colTypes = Type.fromClasses(this.colTypes.toArray(new Class[0]));

        if (subtable) {
            colNames = ArrayUtils.insert(0, colNames, JSONToTableWriterAdapter.SUBTABLE_RECORD_ID_COL);
            colTypes = ArrayUtils.insert(0, colTypes, Type.longType());
        }

        return new DynamicTableWriter(
                colNames,
                colTypes);
    }

    private void addCol(final String colName, final Class<?> colType) {
        colNames.add(NameValidator.validateColumnName(colName));
        colTypes.add(colType);
    }

    private void addCols(final List<String> colNames, final List<Class<?>> colTypes) {
        Require.equals(
                colNames.size(), "colNames.size()",
                colTypes.size(), "colTypes.size()");
        for (String colName : colNames) {
            NameValidator.validateColumnName(colName);
        }
        this.colNames.addAll(colNames);
        this.colTypes.addAll(colTypes);
    }

    public JSONToInMemoryTableAdapterBuilder addColumnFromField(final String field,
            final Class<?> type) {
        return addColumnFromField(field, field, type);
    }

    public JSONToInMemoryTableAdapterBuilder addColumnFromField(final String column, final String field,
            final Class<?> type) {
        addCol(column, type);
        jsonAdpaterBuilder.addColumnFromField(column, field);

        return this;
    }

    public JSONToInMemoryTableAdapterBuilder addFieldParallel(final String column,
            final String field, final Class<?> type) {
        addCol(column, type);
        jsonAdpaterBuilder.addFieldParallel(column, field);

        return this;
    }

    public JSONToInMemoryTableAdapterBuilder addNestedFieldParallel(final String field,
            final JSONToInMemoryTableAdapterBuilder builder) {
        addCols(builder.colNames, builder.colTypes);

        subtableFieldsToBuilders.putAll(builder.subtableFieldsToBuilders);
        jsonAdpaterBuilder.addNestedFieldParallel(field, builder.jsonAdpaterBuilder);

        return this;
    }

    public JSONToInMemoryTableAdapterBuilder addNestedField(
            final String field,
            final JSONToInMemoryTableAdapterBuilder builder) {
        addCols(builder.colNames, builder.colTypes);

        subtableFieldsToBuilders.putAll(builder.subtableFieldsToBuilders);
        jsonAdpaterBuilder.addNestedField(field, builder.jsonAdpaterBuilder);

        return this;
    }

    public JSONToInMemoryTableAdapterBuilder addFieldToSubTableMapping(final String field,
            final JSONToInMemoryTableAdapterBuilder subtableBuilder) {
        colNames.add(JSONToTableWriterAdapter.getSubtableRowIdColName(field));
        colTypes.add(long.class);

        // include subtables of this subtable as well
        subtableFieldsToBuilders.putAll(subtableBuilder.subtableFieldsToBuilders);
        routedTableIdsToBuilders.putAll(subtableBuilder.routedTableIdsToBuilders);
        subtableFieldsToBuilders.put(field, subtableBuilder);
        jsonAdpaterBuilder.addFieldToSubTableMapping(field, subtableBuilder.jsonAdpaterBuilder);
        return this;
    }

    @ScriptApi
    public JSONToInMemoryTableAdapterBuilder addRoutedTableAdapter(
            final String routedTableIdentifier,
            final Predicate<JsonNode> routingPredicate,
            final JSONToInMemoryTableAdapterBuilder routedBuilder) {
        // routed tables are basically subtables -- still need a subtable row ID col.
        colNames.add(JSONToTableWriterAdapter.getSubtableRowIdColName(routedTableIdentifier));
        colTypes.add(long.class);

        subtableFieldsToBuilders.putAll(routedBuilder.subtableFieldsToBuilders);
        routedTableIdsToBuilders.putAll(routedBuilder.routedTableIdsToBuilders);
        routedTableIdsToBuilders.put(routedTableIdentifier, routedBuilder);
        jsonAdpaterBuilder.addRoutedTableAdapter(routedTableIdentifier, routingPredicate,
                routedBuilder.jsonAdpaterBuilder);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder addColumnFromIntFunction(final String column,
            final ToIntFunction<JsonNode> toIntFunction) {
        addCol(column, int.class);

        jsonAdpaterBuilder.addColumnFromIntFunction(column, toIntFunction);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder addColumnFromLongFunction(final String column,
            final ToLongFunction<JsonNode> toLongFunction) {
        addCol(column, long.class);

        jsonAdpaterBuilder.addColumnFromLongFunction(column, toLongFunction);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder addColumnFromDoubleFunction(final String column,
            final ToDoubleFunction<JsonNode> toDoubleFunction) {
        addCol(column, double.class);

        jsonAdpaterBuilder.addColumnFromDoubleFunction(column, toDoubleFunction);

        return this;
    }

    public <R> JSONToInMemoryTableAdapterBuilder addColumnFromFunction(final String column,
            final Class<R> returnType,
            final Function<JsonNode, R> function) {
        addCol(column, returnType);

        jsonAdpaterBuilder.addColumnFromFunction(column, returnType, function);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder addColumnFromPointer(final String column, final Class<?> type,
            final String jsonPointer) {
        return addColumnFromPointer(column, type, JsonPointer.compile(jsonPointer));
    }

    public JSONToInMemoryTableAdapterBuilder addColumnFromPointer(final String column, final Class<?> type,
            final JsonPointer jsonPointer) {
        addCol(column, type);
        jsonAdpaterBuilder.addColumnFromPointer(column, jsonPointer);

        return this;
    }

    public JSONToInMemoryTableAdapterBuilder nConsumerThreads(final int nConsumerThreads) {
        jsonAdpaterBuilder.nConsumerThreads(nConsumerThreads);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder setPostProcessConsumer(
            BiConsumer<MessageMetadata, JsonNode> postProcessConsumer) {
        jsonAdpaterBuilder.setPostProcessConsumer(postProcessConsumer);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder allowUnmapped(final String column) {
        jsonAdpaterBuilder.allowUnmapped(column);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder processArrays(final boolean processArrays) {
        jsonAdpaterBuilder.processArrays(processArrays);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder allowMissingKeys(boolean allowMissingKeys) {
        jsonAdpaterBuilder.allowMissingKeys(allowMissingKeys);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder allowNullValues(boolean allowNullValues) {
        jsonAdpaterBuilder.allowNullValues(allowNullValues);
        return this;
    }

    public JSONToInMemoryTableAdapterBuilder flushIntervalMillis(long flushIntervalMillis) {
        this.flushIntervalMillis = flushIntervalMillis;
        return this;
    }

}
