package io.deephaven.jsoningester;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.io.logger.Logger;
import io.deephaven.jsoningester.msg.MessageMetadata;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
import java.util.stream.Collectors;

/**
 * This is a wrapper for {@link JSONToStreamPublisherAdapter} that also handles setting up the output blink tables.
 */
public class JSONToBlinkTableAdapterBuilder {

    private static final AtomicLong adapterCounter = new AtomicLong(0);

    private final String mainResultTableName;

    private final JSONToStreamPublisherAdapterBuilder jsonAdpaterBuilder = new JSONToStreamPublisherAdapterBuilder()
            .autoValueMapping(false);

    private final Map<String, JSONToBlinkTableAdapterBuilder> subtableFieldsToBuilders = new LinkedHashMap<>();

    private final Map<String, JSONToBlinkTableAdapterBuilder> routedTableIdsToBuilders = new HashMap<>();

    private final List<String> colNames = new ArrayList<>();
    private final List<Class<?>> colTypes = new ArrayList<>();
    private long flushIntervalMillis = 5000L;

    public static class TableAndAdapterBuilderResult {
        private final Map<String, StreamPublisherAndTable> resultPublishersAndAdapters;
        public final JSONToStreamPublisherAdapter streamPublisherAdapter;

        private final Map<String, Table> resultAppendOnlyTables;

        public TableAndAdapterBuilderResult(
                Map<String, StreamPublisherAndTable> resultPublishersAndAdapters,
                JSONToStreamPublisherAdapter streamPublisherAdapter,
                final boolean createAppendOnlyTables) {
            this.resultPublishersAndAdapters = Collections.unmodifiableMap(resultPublishersAndAdapters);
            this.streamPublisherAdapter = streamPublisherAdapter;

            if (createAppendOnlyTables) {
                resultAppendOnlyTables = resultPublishersAndAdapters.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        en -> BlinkTableTools.blinkToAppendOnly(en.getValue().getBlinkTable())));
            } else {
                resultAppendOnlyTables = null;
            }
        }

        public Map<String, Table> getResultBlinkTables() {
            return Maps.transformValues(resultPublishersAndAdapters, StreamPublisherAndTable::getBlinkTable);
        }

        public Map<String, Table> getResultAppendOnlyTables() {
            if (resultAppendOnlyTables == null) {
                throw new IllegalStateException("Append-only tables were not created for this result set");
            }
            return resultAppendOnlyTables;
        }

        /**
         * Calls {@link JSONToStreamPublisherAdapter#shutdown() shutdown()} on the stream publisher adapter, which will
         * also {@link StreamToBlinkTableAdapter#close() close()} the StreamToBlinkTableAdapters.
         */
        public void shutdown() {
            streamPublisherAdapter.shutdown();
            resultPublishersAndAdapters.values().forEach(StreamPublisherAndTable::close);
        }
    }

    public JSONToBlinkTableAdapterBuilder() {
        this(null);
    }

    public JSONToBlinkTableAdapterBuilder(final String mainResultTableName) {
        this.mainResultTableName = mainResultTableName;
    }

    /**
     *
     * @param log A logger
     * @param createAppendOnlyTables Whether to create append-only tables from the result blink tables
     * @return
     */
    public TableAndAdapterBuilderResult build(Logger log,
            boolean createAppendOnlyTables) {
        return build(log,
                ExecutionContext.getContext().getUpdateGraph(),
                createAppendOnlyTables);
    }

    /**
     * @param log A logger
     * @param updateSourceRegistrar An update source registrar (typically an
     *        {@link io.deephaven.engine.updategraph.UpdateGraph), such as
     *        {@code ExecutionContext.getContext().getUpdateGraph()}})
     * @param createAppendOnlyTables Whether to create append-only tables from the result blink tables
     * @return
     */
    public TableAndAdapterBuilderResult build(Logger log,
            UpdateSourceRegistrar updateSourceRegistrar,
            boolean createAppendOnlyTables) {
        final String mainResultTableName;
        if (this.mainResultTableName == null) {
            mainResultTableName = "tableFromJSON_" + adapterCounter.getAndIncrement();
        } else {
            mainResultTableName = this.mainResultTableName;
        }


        // Map of all subtable writers for the main builder and its nested/subtable builders. (This includes routed
        // table adapters.) Note that since this is keyed off of a field name and is a global map for the entire JSON
        // tree, it means a given key (i.e. field name or routed table identifier) can only be used for one subtable
        // anywhere in the tree. So, if two nested fields both have an array field of the same name, only one of them
        // would be able to be used as a subtable.
        final Map<String, SimpleStreamPublisher> routedAndSubtablePublishers = new LinkedHashMap<>();

        // Make this table's StreamPublisher
        Map<String, StreamPublisherAndTable> results =
                new HashMap<>(1 + subtableFieldsToBuilders.size() + routedTableIdsToBuilders.size());

        final StreamPublisherAndTable mainResultPublisherAndTable =
                getStreamPublisher(false, mainResultTableName, updateSourceRegistrar);
        results.put(mainResultTableName, mainResultPublisherAndTable);

        subtableFieldsToBuilders.forEach((k, v) -> {

            final StreamPublisherAndTable subtableAdapterAndPublisher =
                    v.getStreamPublisher(true, mainResultTableName + "_" + k, updateSourceRegistrar);
            routedAndSubtablePublishers.put(k, subtableAdapterAndPublisher.getPublisher());

            results.put(k, subtableAdapterAndPublisher);
        });

        routedTableIdsToBuilders.forEach((k, v) -> {
            final StreamPublisherAndTable routedAdapterAndPublisher =
                    v.getStreamPublisher(true, mainResultTableName + "_" + k, updateSourceRegistrar);
            if (routedAndSubtablePublishers.put(k, routedAdapterAndPublisher.getPublisher()) != null) {
                throw new IllegalStateException("key " + k + " used for both routed table and subtable");
            }

            results.put(k, routedAdapterAndPublisher);
        });

        final JSONToStreamPublisherAdapter jsonAdapter =
                jsonAdpaterBuilder.makeAdapter(log,
                        mainResultPublisherAndTable.getPublisher(),
                        routedAndSubtablePublishers);

        if (flushIntervalMillis > 0) {
            jsonAdapter.createCleanupThread(flushIntervalMillis);
        }

        return new TableAndAdapterBuilderResult(results, jsonAdapter, createAppendOnlyTables);
    }

    private StreamPublisherAndTable getStreamPublisher(final boolean subtable,
            final String adapterName,
            final UpdateSourceRegistrar updateSourceRegistrar) {
        final TableDefinition tableDef = getTableDefinition(subtable);

        return StreamPublisherAndTable.createStreamPublisherAndTable(
                tableDef,
                adapterName,
                updateSourceRegistrar);
    }

    /**
     * @param subtable Whether the table is a subtable (which requires adding a
     *        {@link JSONToStreamPublisherAdapter#SUBTABLE_RECORD_ID_COL}) to the table definition).
     * @return
     */
    @NotNull
    private TableDefinition getTableDefinition(final boolean subtable) {
        List<String> colNames = new ArrayList<>(this.colNames);
        List<Class<?>> colTypes = new ArrayList<>(this.colTypes);

        if (subtable) {
            colNames.add(0, JSONToStreamPublisherAdapter.SUBTABLE_RECORD_ID_COL);
            colTypes.add(0, long.class);
        }

        return TableDefinition.of(colNames, colTypes);
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

    public JSONToBlinkTableAdapterBuilder addColumnFromField(final String field,
            final Class<?> type) {
        return addColumnFromField(field, field, type);
    }

    public JSONToBlinkTableAdapterBuilder addColumnFromField(final String column, final String field,
            final Class<?> type) {
        addCol(column, type);
        jsonAdpaterBuilder.addColumnFromField(column, field);

        return this;
    }

    public JSONToBlinkTableAdapterBuilder addFieldParallel(final String column,
            final String field, final Class<?> type) {
        addCol(column, type);
        jsonAdpaterBuilder.addFieldParallel(column, field);

        return this;
    }

    public JSONToBlinkTableAdapterBuilder addNestedFieldParallel(final String field,
            final JSONToBlinkTableAdapterBuilder builder) {
        addCols(builder.colNames, builder.colTypes);

        subtableFieldsToBuilders.putAll(builder.subtableFieldsToBuilders);
        jsonAdpaterBuilder.addNestedFieldParallel(field, builder.jsonAdpaterBuilder);

        return this;
    }

    public JSONToBlinkTableAdapterBuilder addNestedField(
            final String field,
            final JSONToBlinkTableAdapterBuilder builder) {
        addCols(builder.colNames, builder.colTypes);

        subtableFieldsToBuilders.putAll(builder.subtableFieldsToBuilders);
        jsonAdpaterBuilder.addNestedField(field, builder.jsonAdpaterBuilder);

        return this;
    }

    public JSONToBlinkTableAdapterBuilder addFieldToSubTableMapping(final String field,
            final JSONToBlinkTableAdapterBuilder subtableBuilder) {
        colNames.add(JSONToStreamPublisherAdapter.getSubtableRowIdColName(field));
        colTypes.add(long.class);

        // include subtables of this subtable as well
        subtableFieldsToBuilders.putAll(subtableBuilder.subtableFieldsToBuilders);
        routedTableIdsToBuilders.putAll(subtableBuilder.routedTableIdsToBuilders);
        subtableFieldsToBuilders.put(field, subtableBuilder);
        jsonAdpaterBuilder.addFieldToSubTableMapping(field, subtableBuilder.jsonAdpaterBuilder);
        return this;
    }

    @ScriptApi
    public JSONToBlinkTableAdapterBuilder addRoutedTableAdapter(
            final String routedTableIdentifier,
            final Predicate<JsonNode> routingPredicate,
            final JSONToBlinkTableAdapterBuilder routedBuilder) {
        // routed tables are basically subtables -- still need a subtable row ID col.
        colNames.add(JSONToStreamPublisherAdapter.getSubtableRowIdColName(routedTableIdentifier));
        colTypes.add(long.class);

        subtableFieldsToBuilders.putAll(routedBuilder.subtableFieldsToBuilders);
        routedTableIdsToBuilders.putAll(routedBuilder.routedTableIdsToBuilders);
        routedTableIdsToBuilders.put(routedTableIdentifier, routedBuilder);
        jsonAdpaterBuilder.addRoutedTableAdapter(routedTableIdentifier, routingPredicate,
                routedBuilder.jsonAdpaterBuilder);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder addColumnFromIntFunction(final String column,
            final ToIntFunction<JsonNode> toIntFunction) {
        addCol(column, int.class);

        jsonAdpaterBuilder.addColumnFromIntFunction(column, toIntFunction);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder addColumnFromLongFunction(final String column,
            final ToLongFunction<JsonNode> toLongFunction) {
        addCol(column, long.class);

        jsonAdpaterBuilder.addColumnFromLongFunction(column, toLongFunction);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder addColumnFromDoubleFunction(final String column,
            final ToDoubleFunction<JsonNode> toDoubleFunction) {
        addCol(column, double.class);

        jsonAdpaterBuilder.addColumnFromDoubleFunction(column, toDoubleFunction);

        return this;
    }

    public <R> JSONToBlinkTableAdapterBuilder addColumnFromFunction(final String column,
            final Class<R> returnType,
            final Function<JsonNode, R> function) {
        addCol(column, returnType);

        jsonAdpaterBuilder.addColumnFromFunction(column, returnType, function);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder addColumnFromPointer(final String column, final Class<?> type,
            final String jsonPointer) {
        return addColumnFromPointer(column, type, JsonPointer.compile(jsonPointer));
    }

    public JSONToBlinkTableAdapterBuilder addColumnFromPointer(final String column, final Class<?> type,
            final JsonPointer jsonPointer) {
        addCol(column, type);
        jsonAdpaterBuilder.addColumnFromPointer(column, jsonPointer);

        return this;
    }

    public JSONToBlinkTableAdapterBuilder nConsumerThreads(final int nConsumerThreads) {
        jsonAdpaterBuilder.nConsumerThreads(nConsumerThreads);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder setPostProcessConsumer(
            BiConsumer<MessageMetadata, JsonNode> postProcessConsumer) {
        jsonAdpaterBuilder.setPostProcessConsumer(postProcessConsumer);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder allowUnmapped(final String column) {
        jsonAdpaterBuilder.allowUnmapped(column);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder processArrays(final boolean processArrays) {
        jsonAdpaterBuilder.processArrays(processArrays);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder allowMissingKeys(boolean allowMissingKeys) {
        jsonAdpaterBuilder.allowMissingKeys(allowMissingKeys);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder allowNullValues(boolean allowNullValues) {
        jsonAdpaterBuilder.allowNullValues(allowNullValues);
        return this;
    }

    public JSONToBlinkTableAdapterBuilder flushIntervalMillis(long flushIntervalMillis) {
        this.flushIntervalMillis = flushIntervalMillis;
        return this;
    }

}
