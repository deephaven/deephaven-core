//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    public static final AggregationControl IGNORE_INDEXING = new AggregationControl() {
        @Override
        public DataIndex dataIndexToUse(@NotNull final Table table, @NotNull final String... keyColumnNames) {
            return null;
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

    /**
     * Get a {@link BasicDataIndex} to use for aggregating {@code table} by {@code keyColumnNames}.
     * <p>
     * This call should be enclosed within a {@link io.deephaven.engine.liveness.LivenessScope} to ensure liveness is
     * not unintentionally leaked for any new {@link BasicDataIndex indexes} or {@link Table tables} created. If a
     * non-{@code null} {@link DataIndex#isRefreshing()} is returned, it will have been managed by the enclosing
     * {@link io.deephaven.engine.liveness.LivenessScope}.
     * <p>
     * If a non-{@code null} result is returned, it will have transformed as needed to ensure that the
     * {@link BasicDataIndex#table()} is sorted by first row key.
     *
     * @param table The {@link Table} to aggregate
     * @param keyColumnNames The column names to aggregate by
     * @return The {@link DataIndex} to use, or {@code null} if no index should be used
     */
    @Nullable
    public BasicDataIndex dataIndexToUse(@NotNull final Table table, @NotNull final String... keyColumnNames) {
        final DataIndex preTransformDataIndex = DataIndexer.getDataIndex(table, keyColumnNames);
        if (preTransformDataIndex == null) {
            return null;
        }
        Assert.eq(table.isRefreshing(), "table.isRefreshing()",
                preTransformDataIndex.isRefreshing(), "preTransformDataIndex.isRefreshing()");
        // Note that this transformation uses only concurrent table operations.
        final BasicDataIndex transformedDataIndex = preTransformDataIndex.transform(DataIndexTransformer.builder()
                .sortByFirstRowKey(true)
                .build());
        Assert.eq(table.isRefreshing(), "table.isRefreshing()",
                transformedDataIndex.isRefreshing(), "transformedDataIndex.isRefreshing()");
        return transformedDataIndex;
    }

    boolean considerSymbolTables(@NotNull final Table inputTable, final boolean indexed,
            @NotNull final ColumnSource<?>[] sources) {
        return !inputTable.isRefreshing() && !indexed && sources.length == 1
                && SymbolTableSource.hasSymbolTable(sources[0], inputTable.getRowSet());
    }

    boolean useSymbolTableLookupCaching() {
        return false;
    }

    boolean useSymbolTables(final long inputTableSize, final long symbolTableSize) {
        // the less than vs. leq is important here, so that we do not attempt to use a symbol table for an empty table
        // which fails later on in the SymbolTableCombiner
        return symbolTableSize < inputTableSize / 2;
    }
}
