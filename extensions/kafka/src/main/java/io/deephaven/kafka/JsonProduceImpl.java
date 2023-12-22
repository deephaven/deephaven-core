package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.publish.JsonKeyOrValueSerializer;
import io.deephaven.kafka.publish.KeyOrValueSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * JSON spec.
 */
final class JsonProduceImpl extends KafkaTools.Produce.KeyOrValueSpec {
    private final String[] includeColumns;
    private final Predicate<String> excludeColumns;
    private final Map<String, String> columnNameToFieldName;
    private final String nestedObjectDelimiter;
    private final boolean outputNulls;
    private final String timestampFieldName;

    JsonProduceImpl(final String[] includeColumns,
                    final Predicate<String> excludeColumns,
                    final Map<String, String> columnNameToFieldName,
                    final String nestedObjectDelimiter,
                    final boolean outputNulls,
                    final String timestampFieldName) {
        this.includeColumns = includeColumns;
        this.excludeColumns = excludeColumns;
        this.columnNameToFieldName = columnNameToFieldName;
        this.nestedObjectDelimiter = nestedObjectDelimiter;
        this.outputNulls = outputNulls;
        this.timestampFieldName = timestampFieldName;
    }

    @Override
    public Optional<SchemaProvider> getSchemaProvider() {
        return Optional.empty();
    }

    @Override
    Serializer<?> getSerializer(SchemaRegistryClient schemaRegistryClient, TableDefinition definition) {
        return new StringSerializer();
    }

    @Override
    String[] getColumnNames(@NotNull Table t, SchemaRegistryClient schemaRegistryClient) {
        if (excludeColumns != null && includeColumns != null) {
            throw new IllegalArgumentException(
                    "Can't have both excludeColumns and includeColumns not null");
        }
        final String[] tableColumnNames = t.getDefinition().getColumnNamesArray();
        if (excludeColumns == null && includeColumns == null) {
            return tableColumnNames;
        }
        final Set<String> tableColumnsSet = new HashSet<>(Arrays.asList(tableColumnNames));
        if (includeColumns != null) {
            // Validate includes
            final List<String> missing = Arrays.stream(includeColumns)
                    .filter(cn -> !tableColumnsSet.contains(cn)).collect(Collectors.toList());
            if (!missing.isEmpty()) {
                throw new IllegalArgumentException(
                        "includeColumns contains names not found in table columns: " + missing);
            }
            return includeColumns;
        }
        return Arrays.stream(tableColumnNames)
                .filter(cn -> !excludeColumns.test(cn)).toArray(String[]::new);
    }

    @Override
    KeyOrValueSerializer<?> getKeyOrValueSerializer(@NotNull Table t, @NotNull String[] columnNames) {
        final String[] fieldNames = getFieldNames(columnNames);
        return new JsonKeyOrValueSerializer(
                t, columnNames, fieldNames,
                timestampFieldName, nestedObjectDelimiter, outputNulls);
    }

    String[] getFieldNames(final String[] columnNames) {
        final String[] fieldNames = new String[columnNames.length];
        for (int i = 0; i < columnNames.length; ++i) {
            if (columnNameToFieldName == null) {
                fieldNames[i] = columnNames[i];
            } else {
                fieldNames[i] = columnNameToFieldName.getOrDefault(columnNames[i], columnNames[i]);
            }
        }
        return fieldNames;
    }
}
