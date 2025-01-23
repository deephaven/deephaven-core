//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementations of BasicDataIndex provide a data index for a {@link Table}. The index is itself a {@link Table} with
 * columns corresponding to the indexed column(s) ("key" columns) and a column of {@link RowSet RowSets} that contain
 * the row keys for each unique combination of key values (that is, the "group" or "bucket"). The index itself is a
 * Table containing the key column(s) and the RowSets associated with each unique combination of values. Implementations
 * may be loaded from persistent storage or created at runtime, e.g. via aggregations.
 */
public interface BasicDataIndex extends LivenessReferent {

    /**
     * Get a map from indexed {@link ColumnSource ColumnSources} to key column names for the index {@link #table()
     * table}. This map must be ordered in the same order presented by {@link #keyColumnNames()} and used for lookup
     * keys.
     *
     * @return A map designating the key column names for each indexed {@link ColumnSource}
     */
    @NotNull
    Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn();

    /**
     * Get a list of the key column names for the index {@link #table() table}.
     *
     * @return The key column names
     */
    @NotNull
    List<String> keyColumnNames();

    /**
     * Get the {@link RowSet} column name for the index {@link #table() table}.
     * 
     * @return The {@link RowSet} column name
     */
    @NotNull
    String rowSetColumnName();

    /**
     * Get the key {@link ColumnSource ColumnSources} of the index {@link #table() table}.
     *
     * @return An array of the key {@link ColumnSource ColumnSources}, to be owned by the caller
     */
    @FinalDefault
    @NotNull
    default ColumnSource<?>[] keyColumns() {
        final Table indexTable = table();
        return keyColumnNames().stream()
                .map(indexTable::getColumnSource)
                .toArray(ColumnSource[]::new);
    }

    /**
     * Get the key {@link ColumnSource ColumnSources} of the index {@link #table() table} in the relative order of
     * {@code indexedColumnSources}.
     *
     * @param indexedColumnSources The indexed {@link ColumnSource ColumnSources} in the desired order; must match the
     *        keys of {@link #keyColumnNamesByIndexedColumn()}
     * @return An array of the key {@link ColumnSource ColumnSources} in the specified order, to be owned by the caller
     */
    @FinalDefault
    @NotNull
    default ColumnSource<?>[] keyColumns(@NotNull final ColumnSource<?>[] indexedColumnSources) {
        final Table indexTable = table();
        final Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn = keyColumnNamesByIndexedColumn();
        // Verify that the provided columns match the indexed columns.
        if (keyColumnNamesByIndexedColumn.size() != indexedColumnSources.length
                || !keyColumnNamesByIndexedColumn.keySet().containsAll(Arrays.asList(indexedColumnSources))) {
            throw new IllegalArgumentException(String.format(
                    "The provided columns %s do not match the index's indexed columns %s",
                    Arrays.toString(indexedColumnSources),
                    keyColumnNamesByIndexedColumn.keySet()));
        }
        return Arrays.stream(indexedColumnSources)
                .map(keyColumnNamesByIndexedColumn::get)
                .map(indexTable::getColumnSource)
                .toArray(ColumnSource[]::new);
    }

    /**
     * Get the {@link RowSet} {@link ColumnSource} of the index {@link #table() table}.
     *
     * @return The {@link RowSet} {@link ColumnSource}
     */
    @FinalDefault
    @NotNull
    default ColumnSource<RowSet> rowSetColumn() {
        return rowSetColumn(DataIndexOptions.DEFAULT);
    }

    /**
     * Get the {@link RowSet} {@link ColumnSource} of the index {@link #table() table}.
     *
     * @param options parameters for controlling how the the table will be built (if necessary) in order to retrieve the
     *        result {@link RowSet} {@link ColumnSource}
     *
     * @return The {@link RowSet} {@link ColumnSource}
     */
    @FinalDefault
    @NotNull
    default ColumnSource<RowSet> rowSetColumn(final DataIndexOptions options) {
        return table(options).getColumnSource(rowSetColumnName(), RowSet.class);
    }

    /**
     * Get the {@link Table} backing this data index.
     *
     * <p>
     * The returned table is fully in-memory, equivalent to {@link #table(DataIndexOptions)} with default options.
     * </p>
     * 
     * @return The {@link Table}
     */
    @NotNull
    default Table table() {
        return table(DataIndexOptions.DEFAULT);
    }

    /**
     * Get the {@link Table} backing this data index.
     *
     * @param options parameters to control the returned table
     *
     * @return The {@link Table}
     */
    @NotNull
    Table table(DataIndexOptions options);

    /**
     * Whether the index {@link #table()} {@link Table#isRefreshing() is refreshing}. Some transformations will force
     * the index to become static even when the source table is refreshing.
     *
     * @return {@code true} if the index {@link #table()} {@link Table#isRefreshing() is refreshing}, {@code false}
     *         otherwise
     */
    boolean isRefreshing();

}
