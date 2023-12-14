package io.deephaven.engine.table;

import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This interface provides a data index for a {@link Table}. The index itself is a Table containing the key column(s)
 * and the RowSets associated with each unique combination of values. DataIndexes may be loaded from persistent storage
 * or created using aggregations
 */
public interface BasicDataIndex extends LivenessReferent {
    /** Get the key column names for the index {@link #table() table}. */
    String[] keyColumnNames();

    /** Get a map from indexed column sources to key column names for the index {@link #table() table}. */
    Map<ColumnSource<?>, String> keyColumnMap();

    /** Get the output row set column name for this index. */
    String rowSetColumnName();

    /** Return the index table key sources in the order of the index table. **/
    @FinalDefault
    default ColumnSource<?>[] indexKeyColumns() {
        // Reverse the map to lookup from column name to column source.
        final Map<String, ColumnSource<?>> map = keyColumnMap().entrySet().stream()
                .collect(LinkedHashMap::new, (m, e) -> m.put(e.getValue(), e.getKey()), Map::putAll);
        final ColumnSource<?>[] columnSources = Arrays.stream(keyColumnNames())
                .map(map::get)
                .toArray(ColumnSource[]::new);
        return indexKeyColumns(columnSources);
        // TODO-RWC: Should this be in a static helper instead of the interface?
    }

    /** Return the index table key sources in the relative order of the indexed sources supplied. **/
    @FinalDefault
    default ColumnSource<?>[] indexKeyColumns(@NotNull final ColumnSource<?>[] columnSources) {
        final Table indexTable = table();
        final Map<ColumnSource<?>, String> keyColumnMap = keyColumnMap();
        // Verify that the provided sources match the sources of the index.
        if (columnSources.length != keyColumnMap.size()
                || !keyColumnMap.keySet().containsAll(Arrays.asList(columnSources))) {
            throw new IllegalArgumentException("The provided columns must match the data index key columns");
        }
        return Arrays.stream(columnSources)
                .map(keyColumnMap::get)
                .map(indexTable::getColumnSource)
                .toArray(ColumnSource[]::new);
        // TODO-RWC: Should this be in a static helper instead of the interface?
    }

    /** Return the index table row set source. **/
    @FinalDefault
    default ColumnSource<RowSet> rowSetColumn() {
        return table().getColumnSource(rowSetColumnName(), RowSet.class);
    }

    /** Get the index as a table. */
    @NotNull
    Table table();

    /**
     * Whether the data index table is refreshing. Some transformations will force the index to become static even when
     * the source table is refreshing.
     *
     * @return true when the index table is refreshing, false otherwise.
     */
    boolean isRefreshing();
}
