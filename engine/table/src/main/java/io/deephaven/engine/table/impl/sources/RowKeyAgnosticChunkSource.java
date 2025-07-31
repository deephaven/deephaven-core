//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseable;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * This is a marker interface for chunk sources that are agnostic of the row key when evaluating the value for a given
 * row key.
 */
public interface RowKeyAgnosticChunkSource<ATTR extends Any> extends FillUnordered<ATTR> {
    static void estimatePushdownFilterCostHelper(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final JobScheduler jobScheduler,
            final LongConsumer onComplete,
            final Consumer<Exception> onError) {
        onComplete.accept(PushdownResult.SINGLE_VALUE_COLUMN_COST);
    }

    static void pushdownFilterHelper(
            final ColumnSource<?> columnSource,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {

        if (selection.isEmpty()) {
            onComplete.accept(PushdownResult.allNoMatch(selection));
            return;
        }

        // Only need to test a single value from this column. Create a single row table, execute the filter, and return
        // `selection` or the empty set depending on the result.
        final String columnName = filter.getColumns().get(0);
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TrackingWritableRowSet rowSet = RowSetFactory.fromKeys(selection.firstRowKey()).toTracking()) {

            // Must create a dummy table since the original table is not available.
            final Map<String, ColumnSource<?>> columnSourceMap = Map.of(columnName, columnSource);
            final Table dummyTable = new QueryTable(rowSet, columnSourceMap);
            try (final RowSet result = filter.filter(rowSet, rowSet, dummyTable, usePrev)) {
                if (result.isEmpty()) {
                    // No rows match the filter, return empty selection
                    onComplete.accept(PushdownResult.allNoMatch(selection));
                } else {
                    // All rows match this filter, return the original selection as `match` rows.
                    onComplete.accept(PushdownResult.allMatch(selection));
                }
            }
        }
    }
}
