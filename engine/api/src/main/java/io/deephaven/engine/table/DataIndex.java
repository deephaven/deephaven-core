//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Expansion of {@link BasicDataIndex} to include methods for transforming the index and fast retrieval of row keys from
 * lookup keys.
 */
public interface DataIndex extends BasicDataIndex {
    /**
     * Provides a lookup function from a <em>lookup key</em> to the row key in the index table. Lookup keys consist of
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
        long apply(Object key, boolean usePrev);
    }

    /**
     * Build a {@link RowKeyLookup lookup function} of row keys for this index. If {@link #isRefreshing()} is
     * {@code true}, this lookup function is only guaranteed to be accurate for the current cycle. Lookup keys should be
     * in the order of the index's key columns.
     *
     * @return A function that provides map-like lookup of index {@link #table()} row keys from an index lookup key
     */
    @NotNull
    RowKeyLookup rowKeyLookup();

    /**
     * Return a {@link RowKeyLookup lookup function} function of index row keys for this index. If
     * {@link #isRefreshing()} is {@code true}, this lookup function is only guaranteed to be accurate for the current
     * cycle. Lookup keys should be in the order of {@code lookupColumns}.
     *
     * @param lookupColumns The {@link ColumnSource ColumnSources} to use for the lookup key
     * @return A function that provides map-like lookup of index {@link #table()} row keys from an index lookup key. The
     *         result must not be used concurrently by more than one thread.
     */
    @NotNull
    @FinalDefault
    default RowKeyLookup rowKeyLookup(@NotNull final ColumnSource<?>[] lookupColumns) {
        final Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn = keyColumnNamesByIndexedColumn();
        final ColumnSource<?>[] indexedColumns =
                keyColumnNamesByIndexedColumn.keySet().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);

        final Runnable onMismatch = () -> {
            throw new IllegalArgumentException(String.format(
                    "The provided lookup columns %s do not match the indexed columns %s",
                    Arrays.toString(lookupColumns), Arrays.toString(indexedColumns)));
        };

        if (indexedColumns.length != lookupColumns.length) {
            onMismatch.run();
        }

        // We will need to create an appropriately mapped index lookup key for each caller-supplied key. Let's create an
        // int[] mapping the offset into the index's lookup key to the offset into the caller's lookup key.
        final int[] indexToCallerOffsetMap = new int[lookupColumns.length];
        boolean sameOrder = true;
        // This is an N^2 loop but N is expected to be very small and this is called only at creation.
        for (int ii = 0; ii < indexedColumns.length; ++ii) {
            boolean found = false;
            for (int jj = 0; jj < lookupColumns.length; ++jj) {
                if (indexedColumns[ii] == lookupColumns[jj]) {
                    indexToCallerOffsetMap[ii] = jj;
                    sameOrder &= ii == jj;
                    found = true;
                    break;
                }
            }
            if (!found) {
                onMismatch.run();
            }
        }

        if (sameOrder) {
            return rowKeyLookup();
        }
        Assert.neq(indexToCallerOffsetMap.length, "indexToCallerOffsetMap.length", 1);

        return new RowKeyLookup() {
            // This is the complex key we need to provide to our lookup function.
            final Object[] indexKey = new Object[indexToCallerOffsetMap.length];

            @Override
            public long apply(final Object callerKey, final boolean usePrev) {
                // This is the complex key provided by the caller.
                final Object[] callerKeys = (Object[]) callerKey;

                // Assign the caller-supplied keys to the lookup function key in the appropriate order.
                for (int ii = 0; ii < indexKey.length; ++ii) {
                    indexKey[ii] = callerKeys[indexToCallerOffsetMap[ii]];
                }

                return rowKeyLookup().apply(indexKey, usePrev);
            }
        };
    }

    /**
     * Transform and return a new {@link BasicDataIndex} with the provided transform operations applied. Some
     * transformations will force the result to be a static snapshot even when this DataIndex {@link #isRefreshing() is
     * refreshing}.
     *
     * @param transformer The {@link DataIndexTransformer} specifying the desired transformations
     *
     * @return The transformed {@link BasicDataIndex}
     */
    @NotNull
    BasicDataIndex transform(@NotNull DataIndexTransformer transformer);

    /**
     * Create a new {@link DataIndex} using the same index {@link #table()}, with the indexed columns in
     * {@link #keyColumnNamesByIndexedColumn()} remapped according to {@code oldToNewColumnMap}. This is used when it is
     * known that an operation has produced new columns that are equivalent to the old indexed columns. The result index
     * may keep {@code this} index reachable and
     * {@link io.deephaven.engine.liveness.LivenessManager#manage(LivenessReferent) live} for its own lifetime, if
     * necessary.
     *
     * @param oldToNewColumnMap Map from the old indexed {@link ColumnSource ColumnSources} to the new indexed
     *        ColumnSources
     *
     * @return The remapped {@link DataIndex}
     */
    DataIndex remapKeyColumns(@NotNull Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap);
}
