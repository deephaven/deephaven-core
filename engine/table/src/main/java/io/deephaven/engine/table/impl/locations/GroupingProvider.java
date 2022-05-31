/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.locations;

import io.deephaven.base.Pair;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.impl.ParallelDeferredGroupingProvider;
import io.deephaven.engine.table.impl.sources.DeferredGroupingColumnSource;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Interface used by {@link DeferredGroupingColumnSource} to compute groupings.
 */
public interface GroupingProvider<DATA_TYPE> {

    /**
     * Make a new {@link GroupingProvider} for the specified {@link ColumnDefinition} and current global configuration.
     *
     * @param columnDefinition The column definition
     * @return A new {@link GroupingProvider}
     */
    @NotNull
    static <DATA_TYPE> GroupingProvider<DATA_TYPE> makeGroupingProvider(
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition) {
        return new ParallelDeferredGroupingProvider<>(columnDefinition);
    }

    /**
     * Returns a grouping structure, possibly constructed on-demand.
     *
     * @return a Map from grouping keys to Indices, or null if the group could not be constructed
     */
    Map<DATA_TYPE, RowSet> getGroupToRange();

    /**
     * Returns a grouping structure, possibly constructed on-demand; the grouping is only required to include groupings
     * for values that exist within the hint RowSet; but it may include more. The hint allows the underlying
     * implementation to optionally optimize out groupings that do not overlap hint.
     * <p>
     * The return value is a pair, containing a "complete" indicator. If the complete indicator is true, then the caller
     * may safely cache the resultant Map.
     *
     * @param hint required indices within the resultant Map
     * @return a Pair containing a Map from grouping keys to Indices, which includes at least the hint indices; and a
     *         Boolean which indicates that the grouping is complete
     */
    Pair<Map<DATA_TYPE, RowSet>, Boolean> getGroupToRange(RowSet hint);
}
