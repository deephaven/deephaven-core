package io.deephaven.engine.table;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * This interface provides a data index for a {@link Table}. The index itself is a Table containing the key column(s)
 * and the RowSets associated with each unique combination of values. DataIndexes may be loaded from persistent storage
 * or created using aggregations.
 */
public interface DataIndex extends LivenessReferent {
    /**
     * Provides a lookup function from {@code key} to the position in the index table. Keys consist of reinterpreted
     * values and are specified as follows:
     * <dl>
     * <dt>No key columns</dt>
     * <dd>"Empty" keys are signified by any zero-length {@code Object[]}</dd>
     * <dt>One key column</dt>
     * <dd>Singular keys are (boxed, if needed) objects</dd>
     * <dt>Multiple key columns</dt>
     * <dd>Compound keys are {@code Object[]} of (boxed, if needed) objects, in the order of the index's key
     * columns</dd>
     * </dl>
     */
    interface PositionLookup {
        /**
         * Get the position in the index table for the provided key.
         *
         * @param key The key to lookup
         * @return The result position
         */
        int apply(Object key, boolean usePrev); // TODO-RWC: Decide about prev impl for the lookups
    }

    /**
     * Provides a lookup function from {@code key} to the {@link RowSet} containing the matching table rows. Keys
     * consist of reinterpreted values and are specified as follows:
     * <dl>
     * <dt>No key columns</dt>
     * <dd>"Empty" keys are signified by any zero-length {@code Object[]}</dd>
     * <dt>One key column</dt>
     * <dd>Singular keys are (boxed, if needed) objects</dd>
     * <dt>Multiple key columns</dt>
     * <dd>Compound keys are {@code Object[]} of (boxed, if needed) objects, in the order of the index's key
     * columns</dd>
     * </dl>
     */
    interface RowSetLookup {
        /**
         * Get the {@link RowSet} for the provided key.
         *
         * @param key The key to lookup
         * @return The result RowSet
         */
        RowSet apply(Object key, boolean usePrev);
    }

    /** Get the key column names for the index {@link #table() table}. */
    @NotNull
    String[] keyColumnNames();

    /** Get a map from indexed column sources to key column names for the index {@link #table() table}. */
    @NotNull
    Map<ColumnSource<?>, String> keyColumnMap();

    /** Get the output row set column name for this index. */
    @NotNull
    String rowSetColumnName();

    /** Return the index table key sources in the order of the index table. **/
    @FinalDefault
    default ColumnSource<?>[] indexKeyColumns() {
        final ColumnSource<?>[] columnSources =
                keyColumnMap().keySet().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        return indexKeyColumns(columnSources);
        // TODO-RWC: Should this be in a static helper instead of the interface?
    }

    /** Return the index table key sources in the relative order of the indexed sources supplied. **/
    @FinalDefault
    @NotNull
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
    @NotNull
    default ColumnSource<RowSet> rowSetColumn() {
        return table().getColumnSource(rowSetColumnName(), RowSet.class);
    }

    /** Get the index as a table. */
    @NotNull
    Table table();

    /**
     * Return a {@link RowSetLookup lookup} function of index row sets for this index. If {@link #isRefreshing()} is
     * true, this lookup function is guaranteed to be accurate only for the current cycle.
     *
     * @return a function that provides map-like lookup of matching rows from an index key.
     */
    @NotNull
    RowSetLookup rowSetLookup();

    /**
     * Return a {@link RowSetLookup lookup} function of index row sets for this index. If {@link #isRefreshing()} is
     * true, this lookup function is guaranteed to be accurate only for the current cycle. The keys provided must be in
     * the order of the {@code lookupSources}.
     *
     * @return a function that provides map-like lookup of matching rows from an index key.
     */
    @NotNull
    @FinalDefault
    default RowSetLookup rowSetLookup(@NotNull final ColumnSource<?>[] lookupSources) {
        if (lookupSources.length == 1) {
            // Trivially ordered.
            return rowSetLookup();
        }

        final ColumnSource<?>[] indexSourceColumns = keyColumnMap().keySet().toArray(ColumnSource[]::new);
        if (Arrays.equals(lookupSources, indexSourceColumns)) {
            // Order matches, so we can use the default lookup function.
            return rowSetLookup();
        }

        // We need to wrap the lookup function with a key remapping function.

        // Maps index keys -> user-supplied keys
        final int[] indexToUserMapping = new int[lookupSources.length];

        // Build an intermediate map (N^2 loop but N is small and this is called rarely).
        for (int ii = 0; ii < indexSourceColumns.length; ++ii) {
            boolean found = false;
            for (int jj = 0; jj < lookupSources.length; ++jj) {
                if (indexSourceColumns[ii] == lookupSources[jj]) {
                    indexToUserMapping[ii] = jj;
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("The provided columns must match the data index key columns");
            }
        }

        return (key, usePrev) -> {
            // This is the key provided by the caller.
            final Object[] keys = (Object[]) key;
            // This is the key we need to provide to the lookup function.
            final Object[] remappedKey = new Object[keys.length];

            for (int ii = 0; ii < remappedKey.length; ++ii) {
                remappedKey[ii] = keys[indexToUserMapping[ii]];
            }

            return rowSetLookup().apply(remappedKey, usePrev);
        };
    }

    /**
     * Build a {@link PositionLookup lookup} of positions for this index. If {@link #isRefreshing()} is true, this
     * lookup function is guaranteed to be accurate only for the current cycle.
     *
     * @return a function that provides map-like lookup of index table positions from an index key.
     */
    @NotNull
    PositionLookup positionLookup();

    /**
     * Transform and return a new {@link DataIndex} with the provided transform operations applied. Some transformations
     * will force the index to become static even when the source table is refreshing. *
     *
     * @param transformer the {@link DataIndexTransformer} containing the desired transformations.
     *
     * @return the transformed {@link DataIndex}
     */
    @NotNull
    DataIndex transform(@NotNull final DataIndexTransformer transformer);

    /**
     * Whether the materialized data index table is refreshing. Some transformations will force the index to become
     * static even when the source table is refreshing.
     *
     * @return true when the materialized index table is refreshing, false otherwise.
     */
    boolean isRefreshing();
}
