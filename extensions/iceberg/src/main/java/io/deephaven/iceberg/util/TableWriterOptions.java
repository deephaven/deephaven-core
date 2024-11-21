//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.Schema;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Value.Immutable
@BuildableStyle
public abstract class TableWriterOptions {

    /**
     * The {@link TableDefinition} to use when writing Iceberg data files, instead of the one implied by the table being
     * written itself. This definition can be used to skip some columns or add additional columns with {@code null}
     * values.
     */
    public abstract TableDefinition tableDefinition();

    /**
     * Used to extract a {@link Schema} from a table. That schema will be used in conjunction with the
     * {@link #fieldIdToColumnName()} to map Deephaven columns from {@link #tableDefinition()} to Iceberg columns. If
     * {@link #fieldIdToColumnName()} is not provided, the mapping is done by column name.
     * <p>
     * Users can specify how to extract the schema in multiple ways (by ID, snapshot ID, initial schema, etc.).
     * <p>
     * If not provided, we use the current schema from the table.
     */
    @Value.Default
    public SchemaProvider schemaProvider() {
        return SchemaProvider.current();
    }

    /**
     * A one-to-one {@link Map map} from Iceberg field IDs from the {@link #schemaProvider()} to Deephaven column names
     * from the {@link #tableDefinition()}.
     */
    public abstract Map<Integer, String> fieldIdToColumnName();

    /**
     * A reverse mapping of {@link #fieldIdToColumnName()}.
     */
    @Value.Lazy
    Map<String, Integer> dhColumnNameToFieldId() {
        final Map<String, Integer> reversedMap = new HashMap<>(fieldIdToColumnName().size());
        for (final Map.Entry<Integer, String> entry : fieldIdToColumnName().entrySet()) {
            reversedMap.put(entry.getValue(), entry.getKey());
        }
        return reversedMap;
    }

    public static Builder builder() {
        return ImmutableTableWriterOptions.builder();
    }

    public interface Builder {
        Builder tableDefinition(TableDefinition tableDefinition);

        Builder schemaProvider(SchemaProvider schemaProvider);

        Builder putFieldIdToColumnName(int value, String key);

        Builder putAllFieldIdToColumnName(Map<Integer, ? extends String> entries);

        TableWriterOptions build();
    }

    /**
     * Check all column names present in the {@link #fieldIdToColumnName()} map are present in the
     * {@link #tableDefinition()}.
     */
    @Value.Check
    final void checkDhColumnsToIcebergFieldIds() {
        if (!fieldIdToColumnName().isEmpty()) {
            final Set<String> columnNamesFromDefinition = tableDefinition().getColumnNameSet();
            final Map<Integer, String> fieldIdToColumnName = fieldIdToColumnName();
            for (final String columnNameFromMap : fieldIdToColumnName.values()) {
                if (!columnNamesFromDefinition.contains(columnNameFromMap)) {
                    throw new IllegalArgumentException("Column " + columnNameFromMap + " not found in table " +
                            "definition, available columns are: " + columnNamesFromDefinition);
                }
            }
        }
    }

    @Value.Check
    final void checkOneToOneMapping() {
        final Collection<String> columnNames = new HashSet<>(fieldIdToColumnName().size());
        for (final String columnName : fieldIdToColumnName().values()) {
            if (columnNames.contains(columnName)) {
                throw new IllegalArgumentException("Duplicate mapping found: " + columnName + " in field Id to column" +
                        " name map, expected one-to-one mapping");
            }
            columnNames.add(columnName);
        }
    }
}
