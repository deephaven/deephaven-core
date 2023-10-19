package io.deephaven.engine.table;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;

/**
 * This interface provides a data index for a table. The index is a table containing the key column(s) and the RowSets
 * that contain these values. DataIndexes may be loaded from storage or created using aggregations.
 */
public interface DataIndex {
    interface PositionLookup {
        int apply(Object key);
    }

    interface RowSetLookup {
        RowSet apply(Object key);
    }

    /** Get the key column sources for this index. */
    String[] keyColumnNames();

    /** Get the key column sources for this index. */
    Map<ColumnSource<?>, String> keyColumnMap();

    /** Get the output row set column name for this index. */
    String rowSetColumnName();

    /** Return the index table key sources in order of the sources supplied. **/
    @FinalDefault
    default ColumnSource<?>[] indexKeyColumns(ColumnSource<?>[] tableSources) {
        final Table indexTable = table();
        final Map<ColumnSource<?>, String> columnNameMap = this.keyColumnMap();
        return Arrays.stream(tableSources)
                .map(columnNameMap::get)
                .map(indexTable::getColumnSource)
                .toArray(ColumnSource[]::new);
    }

    /** Return the index table row set source. **/
    @FinalDefault
    default ColumnSource<RowSet> rowSetColumn() {
        final Table indexTable = table();
        return indexTable.getColumnSource(rowSetColumnName());
    }

    /** Get the index as a table. */
    @Nullable
    Table table();

    /**
     * Build a lookup function of the index values based on the current configuration of this index.
     *
     * @return a function that provides map-like lookup of positional from a given index key.
     */
    @Nullable
    RowSetLookup rowSetLookup();

    /**
     * Build a lookup function of positions on the current configuration of this index.
     *
     * @return a function that provides map-like lookup of positional from a given index key.
     */
    @NotNull
    PositionLookup positionLookup();

    /** Get the index as a table using previous values and row keys. */
    @Nullable
    Table prevTable();

    /**
     * Transform and return a new {@link DataIndex} with the provided {@link RowSet} operations applied.
     *
     * @param intersectRowSet the {@link RowSet} to {@link RowSet#intersect(RowSet) intersect} with each output row set.
     *        Provide null to skip intersection.
     * @param invertRowSet convert the output rows to positions in the provided {@link RowSet}. Provide null to skip
     *        inversion.
     * @param sortByFirstRowKey sort by the first key within each row set. This can be useful to maintain visitation
     *        order.
     * @param keyColumnRemap a map providing lookup from derived key column sources to original key columns.
     *
     * @return the transformed {@link DataIndex}
     */

    DataIndex apply(@Nullable final RowSet intersectRowSet,
            @Nullable final RowSet invertRowSet,
            final boolean sortByFirstRowKey,
            @Nullable final Map<ColumnSource<?>, ColumnSource<?>> keyColumnRemap,
            final boolean immutableResult);

    /**
     * Transform and return a new {@link DataIndex} with the provided {@link RowSet} operations applied.
     *
     * @param intersectRowSet the {@link RowSet} to {@link RowSet#intersect(RowSet) intersect} with each output row set.
     *
     * @return the transformed {@link DataIndex}
     */

    @FinalDefault
    default DataIndex applyIntersect(@NotNull final RowSet intersectRowSet) {
        return apply(intersectRowSet, null, false, null, false);
    }

    /**
     * Transform and return a new {@link DataIndex} with the provided {@link RowSet} operations applied.
     *
     * @param invertRowSet convert the output rows to positions in the provided {@link RowSet}. Provide null to skip
     *        inversion.
     *
     * @return the transformed {@link DataIndex}
     */

    @FinalDefault
    default DataIndex applyInvert(@NotNull final RowSet invertRowSet) {
        return apply(null, invertRowSet, false, null, false);
    }

    /**
     * Transform and return a new {@link DataIndex} with the provided {@link RowSet} operations applied.
     *
     * @return the transformed {@link DataIndex}
     */

    @FinalDefault
    default DataIndex applySortByFirstRowKey() {
        return apply(null, null, true, null, false);
    }

    /**
     * Transform and return a new {@link DataIndex} with the provided {@link RowSet} operations applied.
     *
     * @return the transformed {@link DataIndex}
     */

    @FinalDefault
    default DataIndex applyColumnRemap(final Map<ColumnSource<?>, ColumnSource<?>> keyColumnRemap) {
        return apply(null, null, false, keyColumnRemap, false);
    }


    /**
     * Transform and return a new {@link DataIndex} with the provided {@link RowSet} operations applied.
     *
     * @return the transformed {@link DataIndex}
     */

    @FinalDefault
    default DataIndex immutable() {
        return apply(null, null, false, null, true);
    }

    /**
     * Return whether the data index is refreshing or static. All storage backed indexes are static, while indexes
     * created from aggregations reflect whether the underlying data table is refreshing.
     *
     * @return true when the underlying index is refreshing, false otherwise.
     */
    boolean isRefreshing();
}

