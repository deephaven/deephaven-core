//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * This interface provides a data index for a {@link Table}. The index itself is a Table containing the key column(s)
 * and the RowSets associated with each unique combination of values. DataIndexes may be loaded from persistent storage
 * or created using aggregations.
 *
 * This {@link DataIndex} interface provides methods for transforming the index and fast retrieval of row keys from
 * lookup keys.
 */
public interface DataIndex extends BasicDataIndex {
    /**
     * Provides a lookup function from {@code lookup key} to the row key in the index table. Keys consist of
     * reinterpreted values and are specified as follows:
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
    interface RowKeyLookup {

        /**
         * Get the row key in the index table for the provided lookup key.
         *
         * @param key The key to lookup
         * @return The result row key, or {@link RowSequence#NULL_ROW_KEY} if the key is not found.
         */
        long apply(Object key, boolean usePrev); // TODO-RWC: Decide about prev impl for the lookups
    }

    /**
     * Build a {@link RowKeyLookup lookup function} of row keys for this index. If {@link #isRefreshing()} is true, this
     * lookup function is only guaranteed to be accurate for the current cycle.
     *
     * @return A function that provides map-like lookup of index table positions from an index key
     */
    @NotNull
    RowKeyLookup rowKeyLookup();

    /**
     * Return a {@link RowKeyLookup lookup function} function of index row keys for this index. If
     * {@link #isRefreshing()} is true, this lookup function is only guaranteed to be accurate for the current cycle.
     * The keys provided must be in the order of the {@code lookupSources}.
     *
     * @param lookupSources The sources to use for the lookup key, in the order of the caller's key column
     * @return A function that provides map-like lookup of matching rows from an index key. The result must not be used
     *         concurrently by more than one thread.
     */
    @NotNull
    @FinalDefault
    default RowKeyLookup rowKeyLookup(@NotNull final ColumnSource<?>[] lookupSources) {
        if (lookupSources.length == 1) {
            // Trivially ordered.
            return rowKeyLookup();
        }

        // TODO-RWC: Change this when we can impose order on keyColumnMap
        // Get the source columns of the index in the order of the index key columns.
        final Map<String, ColumnSource<?>> reverseMap = new HashMap<>(keyColumnMap().size());
        keyColumnMap().forEach((k, v) -> reverseMap.put(v, k));
        final ColumnSource<?>[] indexSourceColumns = Arrays.stream(keyColumnNames()).map(reverseMap::get)
                .toArray(ColumnSource[]::new);

        if (Arrays.equals(lookupSources, indexSourceColumns)) {
            // Order matches, so we can use the default lookup function.
            return rowKeyLookup();
        }

        // We will need to create an appropriately mapped lookup key for each user-supplied key. Let's create an int[]
        // storing the index of the user-supplied key array that is correct for the lookup function key.
        final int[] indexToUserMapping = new int[lookupSources.length];

        // Build an intermediate map (N^2 loop but N is small and this is called only at creation).
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

        return new RowKeyLookup() {
            // This is the complex key we need to provide to the lookup function.
            final Object[] remappedKey = new Object[indexToUserMapping.length];

            @Override
            public long apply(final Object key, final boolean usePrev) {
                // This is the complex key provided by the caller.
                final Object[] keys = (Object[]) key;

                // Assign the user-supplied keys to the lookup function key in the appropriate order.
                for (int ii = 0; ii < remappedKey.length; ++ii) {
                    remappedKey[ii] = keys[indexToUserMapping[ii]];
                }

                return rowKeyLookup().apply(remappedKey, usePrev);
            }
        };
    }

    /**
     * Transform and return a new {@link BasicDataIndex} with the provided transform operations applied. Some
     * transformations will force the index to become static even when the source table is refreshing.
     *
     * @param transformer the {@link DataIndexTransformer} containing the desired transformations.
     *
     * @return the transformed {@link BasicDataIndex}
     */
    @NotNull
    BasicDataIndex transform(@NotNull DataIndexTransformer transformer);

    /**
     * Create a new {@link DataIndex} by remapping the source table key columns to new columns.
     *
     * @param oldToNewColumnMap map from the old key columns to the new key columns.
     *
     * @return the transformed {@link BasicDataIndex}
     */
    DataIndex remapKeyColumns(@NotNull Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap);
}
