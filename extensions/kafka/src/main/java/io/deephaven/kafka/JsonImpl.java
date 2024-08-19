//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.KafkaTools.Produce;
import io.deephaven.kafka.ingest.JsonNodeChunkAdapter;
import io.deephaven.kafka.ingest.JsonNodeUtil;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.publish.JsonKeyOrValueSerializer;
import io.deephaven.kafka.publish.KeyOrValueSerializer;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.util.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class JsonImpl {
    /**
     * JSON spec.
     */
    static final class JsonConsume extends Consume.KeyOrValueSpec {
        @Nullable
        private final ObjectMapper objectMapper;
        private final ColumnDefinition<?>[] columnDefinitions;
        private final Map<String, String> fieldToColumnName;

        JsonConsume(
                @NotNull final ColumnDefinition<?>[] columnDefinitions,
                @Nullable final Map<String, String> fieldNameToColumnName,
                @Nullable final ObjectMapper objectMapper) {
            this.columnDefinitions = columnDefinitions;
            this.fieldToColumnName = mapNonPointers(fieldNameToColumnName);
            this.objectMapper = objectMapper;
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.empty();
        }

        @Override
        protected Deserializer<?> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            return new StringDeserializer();
        }

        @Override
        protected KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue,
                SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs, MutableInt nextColumnIndexMut,
                List<ColumnDefinition<?>> columnDefinitionsOut) {
            final KeyOrValueIngestData data = new KeyOrValueIngestData();
            data.toObjectChunkMapper = jsonToObjectChunkMapper(objectMapper);
            columnDefinitionsOut.addAll(Arrays.asList(columnDefinitions));
            // Populate out field to column name mapping from two potential sources.
            data.fieldPathToColumnName = new HashMap<>(columnDefinitions.length);
            final Set<String> coveredColumns = new HashSet<>(columnDefinitions.length);
            if (fieldToColumnName != null) {
                for (final Map.Entry<String, String> entry : fieldToColumnName.entrySet()) {
                    final String colName = entry.getValue();
                    data.fieldPathToColumnName.put(entry.getKey(), colName);
                    coveredColumns.add(colName);
                }
            }
            for (final ColumnDefinition<?> colDef : columnDefinitions) {
                final String colName = colDef.getName();
                if (!coveredColumns.contains(colName)) {
                    final String jsonPtrStr =
                            JsonImpl.JsonConsume.mapFieldNameToJsonPointerStr(colName);
                    data.fieldPathToColumnName.put(jsonPtrStr, colName);
                }
            }
            return data;
        }

        @Override
        protected KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            return JsonNodeChunkAdapter.make(
                    tableDef,
                    ci -> StreamChunkUtils.chunkTypeForColumnIndex(tableDef, ci),
                    data.fieldPathToColumnName,
                    true);
        }

        private static Map<String, String> mapNonPointers(final Map<String, String> fieldNameToColumnName) {
            if (fieldNameToColumnName == null) {
                return null;
            }
            final boolean needsMapping =
                    fieldNameToColumnName.keySet().stream().anyMatch(key -> !key.startsWith("/"));
            if (!needsMapping) {
                return fieldNameToColumnName;
            }
            final Map<String, String> ans = new HashMap<>(fieldNameToColumnName.size());
            for (Map.Entry<String, String> entry : fieldNameToColumnName.entrySet()) {
                final String key = entry.getKey();
                if (key.startsWith("/")) {
                    ans.put(key, entry.getValue());
                } else {
                    ans.put(mapFieldNameToJsonPointerStr(key), entry.getValue());
                }
            }
            return ans;
        }

        /***
         * JSON field names (or "key") can be any string, so in principle they can contain the '/' character. JSON
         * Pointers assign special meaning to the '/' character, so actual '/' in the key they need to be encoded
         * differently. The spec for JSON Pointers (see RFC 6901) tells us to encode '/' using "~1". If we need the '~'
         * character we have to encode that as "~0". This method does this simple JSON Pointer encoding.
         *
         * @param key an arbitrary JSON field name, that can potentially contain the '/' or '~' characters.
         * @return a JSON Pointer encoded as a string for the provided key.
         */
        public static String mapFieldNameToJsonPointerStr(final String key) {
            return "/" + key.replace("~", "~0").replace("/", "~1");
        }
    }

    /**
     * JSON spec.
     */
    static final class JsonProduce extends Produce.KeyOrValueSpec {
        private final String[] includeColumns;
        private final Predicate<String> excludeColumns;
        private final Map<String, String> columnNameToFieldName;
        private final String nestedObjectDelimiter;
        private final boolean outputNulls;
        private final String timestampFieldName;

        JsonProduce(final String[] includeColumns,
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

    private static Function<Object, Object> jsonToObjectChunkMapper(@Nullable final ObjectMapper mapper) {
        return (final Object in) -> {
            final String json;
            try {
                json = (String) in;
            } catch (ClassCastException ex) {
                throw new UncheckedDeephavenException("Could not convert input to json string", ex);
            }
            return JsonNodeUtil.makeJsonNode(mapper, json);
        };
    }
}
