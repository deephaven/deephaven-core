/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.DataFormat;
import io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class JsonImpl {
    /**
     * JSON spec.
     */
    static final class JsonConsume extends Consume.KeyOrValueSpec {
        @Nullable
        final ObjectMapper objectMapper;
        final ColumnDefinition<?>[] columnDefinitions;
        final Map<String, String> fieldToColumnName;

        JsonConsume(
                @NotNull final ColumnDefinition<?>[] columnDefinitions,
                @Nullable final Map<String, String> fieldNameToColumnName,
                @Nullable final ObjectMapper objectMapper) {
            this.columnDefinitions = columnDefinitions;
            this.fieldToColumnName = mapNonPointers(fieldNameToColumnName);
            this.objectMapper = objectMapper;
        }

        @Override
        DataFormat dataFormat() {
            return DataFormat.JSON;
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
    static final class JsonProduce extends KeyOrValueSpec {
        final String[] includeColumns;
        final Predicate<String> excludeColumns;
        final Map<String, String> columnNameToFieldName;
        final String nestedObjectDelimiter;
        final boolean outputNulls;
        final String timestampFieldName;

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
        DataFormat dataFormat() {
            return DataFormat.JSON;
        }

        String[] getColumnNames(final Table t) {
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
                if (missing.size() > 0) {
                    throw new IllegalArgumentException(
                            "includeColumns contains names not found in table columns: " + missing);
                }
                return includeColumns;
            }
            return Arrays.stream(tableColumnNames)
                    .filter(cn -> !excludeColumns.test(cn)).toArray(String[]::new);
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
}
