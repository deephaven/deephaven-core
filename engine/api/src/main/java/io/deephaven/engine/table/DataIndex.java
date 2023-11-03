package io.deephaven.engine.table;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * This interface provides a data index for a {@link Table}. The index itself is a Table containing the key column(s)
 * and the RowSets associated with each unique combination of values. DataIndexes may be loaded from persistent storage
 * or created using aggregations.
 */
public interface DataIndex {
    /** Provides a lookup function from {@code key} to the position in the index table. */
    interface PositionLookup {
        int apply(Object key);
    }

    /** Provides a lookup function from {@code key} to the {@link RowSet} containing the matching table rows. */
    interface RowSetLookup {
        RowSet apply(Object key);
    }

    /** Get the column source names that were indexed to produce this DataIndex. */
    String[] keyColumnNames();

    /** Get the column sources that were indexed to produce this DataIndex. */
    Map<ColumnSource<?>, String> keyColumnMap();

    /** Get the output row set column name for this index. */
    String rowSetColumnName();

    /** Return the index table key sources in order of the sources supplied. **/
    @FinalDefault
    default ColumnSource<?>[] indexKeyColumns(ColumnSource<?>[] tableSources) {
        final Table indexTable = table();
        final Map<ColumnSource<?>, String> columnNameMap = this.keyColumnMap();
        // Assert that the provided sources match the sources of the index.
        Set<ColumnSource<?>> providedSources = new HashSet<>(Arrays.asList(tableSources));
        Assert.eqTrue(Objects.equals(providedSources, columnNameMap.keySet()),
                "provided key column sources == index key column sources");
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
    @NotNull
    @FinalDefault
    default Table table() {
        return table(false);
    }

    /**
     * Get the index as a table.
     *
     * @param usePrev whether to use previous values and row keys.
     *
     * @return the index as a table.
     */
    @NotNull
    Table table(final boolean usePrev);

    /**
     * Build a lookup function of index row sets for this index.
     *
     * @return a function that provides map-like lookup of matching rows from an index key.
     */
    @NotNull
    RowSetLookup rowSetLookup();

    /**
     * Build a lookup function of positions for this index.
     *
     * @return a function that provides map-like lookup of index table positions from an index key.
     */
    @NotNull
    PositionLookup positionLookup();

    /**
     * Transform and return a new {@link DataIndex} with the provided transform operations applied.
     *
     * @param transformer the {@link DataIndexTransformer} containing the desired transformations.
     *
     * @return the transformed {@link DataIndex}
     */
    DataIndex transform(final @NotNull DataIndexTransformer transformer);

    /**
     * Return whether the data index is refreshing (i.e. not static).
     *
     * @return true when the underlying index is refreshing, false otherwise.
     */
    boolean isRefreshing();

    /**
     * Return the underlying table for this index. The resultant table should not be read directly; this method is
     * provided for synchronization purposes when performing concurrent operations on the index.
     *
     * @return the underlying table supplying this index
     */
    @InternalUseOnly
    Table baseTable();
}

