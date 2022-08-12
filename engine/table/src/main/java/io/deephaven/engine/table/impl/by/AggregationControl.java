/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

/**
 * Stateless "control" class for giving external code (e.g. unit tests) knobs to turn w.r.t. to how aggregations should
 * be processed.
 */
@VisibleForTesting
public class AggregationControl {

    private static final int CHUNK_SIZE = ChunkedOperatorAggregationHelper.CHUNK_SIZE;
    private static final int MINIMUM_INITIAL_HASH_SIZE = CHUNK_SIZE;
    private static final double DEFAULT_MAX_LOAD_FACTOR = 0.75;
    private static final double DEFAULT_TARGET_LOAD_FACTOR = 0.70;

    public static final AggregationControl DEFAULT = new AggregationControl();
    public static final AggregationControl DEFAULT_FOR_OPERATOR = new AggregationControl() {
        @Override
        public boolean considerGrouping(@NotNull Table table, @NotNull ColumnSource<?>[] sources) {
            return sources.length == 1;
        }
    };

    public int initialHashTableSize(@NotNull final Table inputTable) {
        // TODO: This approach relies on rehash. Maybe we should consider sampling instead.
        return MINIMUM_INITIAL_HASH_SIZE;
    }

    public double getTargetLoadFactor() {
        return DEFAULT_TARGET_LOAD_FACTOR;
    }

    public double getMaximumLoadFactor() {
        return DEFAULT_MAX_LOAD_FACTOR;
    }

    public boolean considerGrouping(@NotNull final Table inputTable, @NotNull final ColumnSource<?>[] sources) {
        return !inputTable.isRefreshing() && sources.length == 1;
    }

    public boolean shouldProbeShift(final long shiftSize, final int numStates) {
        return shiftSize <= numStates * 2;
    }

    // boolean considerSymbolTables(@NotNull final Table inputTable, final boolean useGrouping, @NotNull final
    // ColumnSource<?>[] sources) {
    // return !inputTable.refreshing()
    // && !useGrouping
    // && sources.length == 1
    // && sources[0] instanceof SymbolTableSource
    // && ((SymbolTableSource) sources[0]).hasSymbolTable(inputTable.getRowSet());
    // }
    //
    // boolean useSymbolTableLookupCaching() {
    // return false;
    // }
    //
    // boolean useSymbolTables(final long inputTableSize, final long symbolTableSize) {
    // return symbolTableSize <= inputTableSize / 2;
    // }
    //
    // boolean useUniqueTable(final boolean uniqueValues, final long maximumUniqueValue, final long minimumUniqueValue)
    // {
    // // We want to have one left over value for "no good" (Integer.MAX_VALUE), and then we need another value to
    // // represent that (max - min + 1) is the number of slots required.
    // return uniqueValues && (maximumUniqueValue - minimumUniqueValue) < (Integer.MAX_VALUE - 2);
    // }
}
