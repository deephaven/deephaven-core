//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.Schema;
import org.immutables.value.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Value.Immutable
@BuildableStyle
public abstract class TableWriterOptions {

    /**
     * The {@link TableDefinition} to use when writing Iceberg data files.
     */
    public abstract TableDefinition tableDefinition();

    /**
     * A one-to-one {@link Map map} from Deephaven column names from the {@link #tableDefinition()} to Iceberg field IDs
     * from the {@link #schema()}.
     */
    public abstract Map<String, Integer> dhColumnsToIcebergFieldIds();

    /**
     * The schema to use when in conjunction with the {@link #dhColumnsToIcebergFieldIds()} to map Deephaven columns
     * from {@link #tableDefinition()} to Iceberg columns.
     * <p>
     * If not provided, we use the latest schema from the table.
     */
    public abstract Optional<Schema> schema();

    public static Builder builder() {
        return ImmutableTableWriterOptions.builder();
    }

    public interface Builder {
        Builder tableDefinition(TableDefinition tableDefinition);

        Builder schema(Schema schema);

        Builder putDhColumnsToIcebergFieldIds(String key, int value);

        Builder putAllDhColumnsToIcebergFieldIds(Map<String, ? extends Integer> entries);

        TableWriterOptions build();
    }

    /**
     * Check all column names present in the {@link #dhColumnsToIcebergFieldIds()} map are present in the
     * {@link #tableDefinition()}.
     */
    @Value.Check
    final void checkDhColumnsToIcebergFieldIds() {
        if (!dhColumnsToIcebergFieldIds().isEmpty()) {
            final List<String> columnNamesFromDefinition = tableDefinition().getColumnNames();
            final Map<String, Integer> dhToIcebergColumns = dhColumnsToIcebergFieldIds();
            for (final String columnNameFromMap : dhToIcebergColumns.keySet()) {
                if (!columnNamesFromDefinition.contains(columnNameFromMap)) {
                    throw new IllegalArgumentException("Column " + columnNameFromMap + " not found in table " +
                            "definition, available columns are: " + columnNamesFromDefinition);
                }
            }
        }
    }
}
