//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.Schema;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class TableWriterOptions {

    /**
     * The {@link TableDefinition} to use when writing Iceberg data files, instead of the one implied by the table being
     * written itself. This definition can be used to skip some columns or add additional columns with {@code null}
     * values.
     */
    public abstract TableDefinition tableDefinition();

    /**
     * The data instructions to use for writing the Iceberg data files (might be S3Instructions or other cloud
     * provider-specific instructions). If not provided, data instructions will be derived from the properties of the
     * catalog.
     */
    public abstract Optional<Object> dataInstructions();

    /**
     * Used to extract a {@link Schema} from a table. That schema will be used in conjunction with the
     * {@link #fieldIdToColumnName()} to map Deephaven columns from {@link #tableDefinition()} to Iceberg columns. If
     * {@link #fieldIdToColumnName()} is not provided, the mapping is done by column name.
     * <p>
     * Defaults to {@link SchemaProvider#fromCurrent()}.
     */
    @Value.Default
    public SchemaProvider schemaProvider() {
        return SchemaProvider.fromCurrent();
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

    /**
     * Specifies the {@link org.apache.iceberg.SortOrder} to use for sorting new data when writing to an Iceberg table
     * with this writer. The sort order is determined at the time the writer is created and does not change if the
     * table's sort order changes later.
     * <p>
     * Defaults to {@link SortOrderProvider#useTableDefault()}.
     */
    @Value.Default
    public SortOrderProvider sortOrderProvider() {
        return SortOrderProvider.useTableDefault();
    }

    // @formatter:off
    interface Builder<INSTRUCTIONS_BUILDER extends TableWriterOptions.Builder<INSTRUCTIONS_BUILDER>> {
        // @formatter:on
        INSTRUCTIONS_BUILDER tableDefinition(TableDefinition tableDefinition);

        INSTRUCTIONS_BUILDER dataInstructions(Object s3Instructions);

        INSTRUCTIONS_BUILDER schemaProvider(SchemaProvider schemaProvider);

        INSTRUCTIONS_BUILDER putFieldIdToColumnName(int value, String key);

        INSTRUCTIONS_BUILDER putAllFieldIdToColumnName(Map<Integer, ? extends String> entries);

        INSTRUCTIONS_BUILDER sortOrderProvider(SortOrderProvider sortOrderProvider);
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

    @Value.Check
    final void checkNonEmptyDefinition() {
        if (tableDefinition().numColumns() == 0) {
            throw new IllegalArgumentException("Cannot write to an Iceberg table using empty table definition");
        }
    }
}
