package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.DynamicTableWriter;
import io.deephaven.io.logger.Logger;
import io.deephaven.qst.type.Type;
import io.deephaven.tablelogger.TableWriter;
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

    private final JSONToTableWriterAdapterBuilder jsonAdpaterBuilder = new JSONToTableWriterAdapterBuilder();

    private final Map<String, JSONToInMemoryTableAdapterBuilder> subtableFieldsToBuilders = new LinkedHashMap<>();

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
    }

    public JSONToInMemoryTableAdapterBuilder() {
        this.mainResultTableName = null;
    }

    public JSONToInMemoryTableAdapterBuilder(final String mainResultTableName) {
        Require.neqNull(mainResultTableName, "mainResultTableName");
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

        // Map of all subtable writers for the main builder and its nested/subtable builders. Note that since this
        // is keyed off of a field name and is a global map for the entire JSON tree, it means a given key (i.e. field
        // name) can only be used for one subtable anywhere in the tree. So, if two nested fields both have an array
        // field of the same name, only one of them would be able to be used as a subtable.
        final Map<String, TableWriter<?>> subtableWriters = new LinkedHashMap<>();

        // Make this table's tablewriter
        final DynamicTableWriter thisTableWriter = getTableWriter(false);
        resultTableWriters.put(mainResultTableName, thisTableWriter);
        resultTables.put(mainResultTableName, thisTableWriter.getTable());

        subtableFieldsToBuilders.forEach((k, v) -> {
            final DynamicTableWriter subtableTableWriter = v.getTableWriter(true);
            subtableWriters.put(k, subtableTableWriter);
            resultTableWriters.put(k, subtableTableWriter);
            resultTables.put(k, subtableTableWriter.getTable());
        });

        final JSONToTableWriterAdapter jsonAdapter =
                jsonAdpaterBuilder.makeAdapter(log, thisTableWriter, subtableWriters);

        if(flushIntervalMillis > 0) {
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
        colNames.add(colName);
        colTypes.add(colType);
    }

    private void addCols(final List<String> colName, final List<Class<?>> colType) {
        colNames.addAll(colName);
        colTypes.addAll(colType);
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

    public JSONToInMemoryTableAdapterBuilder addNestedField(final String field,
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
        subtableFieldsToBuilders.put(field, subtableBuilder);
        jsonAdpaterBuilder.addFieldToSubTableMapping(field, subtableBuilder.jsonAdpaterBuilder);
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

    public JSONToInMemoryTableAdapterBuilder nConsumerThreads(final int nConsumerThreads) {
        jsonAdpaterBuilder.nConsumerThreads(nConsumerThreads);
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

    public JSONToInMemoryTableAdapterBuilder autoValueMapping(boolean autoValueMapping) {
        jsonAdpaterBuilder.autoValueMapping(autoValueMapping);
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
