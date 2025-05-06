//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.FilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Building block for Deephaven "source" tables, with helper methods for discovering locations and their sizes. A
 * location allows access to columns, size, and possibly other metadata for a single partition that may be included in a
 * source table.
 */
public interface TableLocation extends NamedImplementation, LogOutputAppendable, TableLocationState, LivenessReferent {

    /**
     * Listener interface for anything that wants to know about changes to a location.
     */
    interface Listener extends BasicTableDataListener {

        /**
         * Notify the listener that the table location has been updated. This may be called "spuriously," i.e. in cases
         * where there has been no substantive update since the last handleUpdate() invocation. Implementations should
         * use appropriate measures to avoid reacting to spurious updates.
         */
        void handleUpdate();
    }

    /**
     * @return An {@link ImmutableTableKey} instance for the enclosing table
     */
    @NotNull
    ImmutableTableKey getTableKey();

    /**
     * @return An {@link ImmutableTableLocationKey} instance for this location
     */
    @NotNull
    ImmutableTableLocationKey getKey();

    /**
     * Does this location support subscriptions? That is, can this location ever have ticking data?
     *
     * @return True if this location supports subscriptions
     */
    boolean supportsSubscriptions();

    /**
     * <p>
     * Subscribe to pushed location updates. Subscribing more than once with the same listener without an intervening
     * unsubscribe is an error, and may result in undefined behavior.
     * <p>
     * This is a possibly asynchronous operation - listener will receive 1 or more handleUpdate callbacks, followed by 0
     * or 1 handleException callbacks during invocation and continuing after completion, on a thread determined by the
     * implementation. Don't hold a lock that prevents notification delivery while subscribing!
     * <p>
     * This method only guarantees eventually consistent state. To force a state update, use refresh() after
     * subscription completes.
     *
     * @param listener A listener
     */
    void subscribe(@NotNull Listener listener);

    /**
     * Unsubscribe from pushed location updates.
     *
     * @param listener The listener to forget about
     */
    void unsubscribe(@NotNull Listener listener);

    /**
     * Initialize or run state information.
     */
    void refresh();

    /**
     * Get an ordered list of columns this location is sorted by.
     * 
     * @return A non-null ordered list of {@link SortColumn SortColumns}
     */
    @NotNull
    List<SortColumn> getSortedColumns();

    /**
     * Get a list of the columns by which this location is indexed
     *
     * @return A non-null list of {@code String[]} arrays containing the key column names for each existing index
     */
    @NotNull
    List<String[]> getDataIndexColumns();

    /**
     * Check if this TableLocation has a data index for the specified columns.
     * 
     * @param columns The set of columns to check for
     * @return Whether the TableLocation has an index for the specified columns
     *
     * @apiNote Implementations must guarantee that the result of this method remains constant over the life of an
     *          instance, and is consistent with the result of {@link #getDataIndex(String...)}.
     */
    boolean hasDataIndex(@NotNull String... columns);

    /**
     * Check if this TableLocation has a data index for the specified columns already loaded in memory.
     *
     * @param columns The set of columns to check for
     * @return Whether the TableLocation has a cached index for the specified columns
     *
     * @apiNote Implementations must guarantee that the result of this method remains constant over the life of an
     *          instance, and is consistent with the result of {@link #getDataIndex(String...)}.
     */
    boolean hasCachedDataIndex(@NotNull String... columns);

    /**
     * Get the data index table for the specified set of columns. Note that the order of columns does not matter here.
     *
     * @param columns The key columns for the index
     * @return The index table or null if one does not exist
     * @apiNote If this TableLocation is not static, the returned table must be {@link Table#isRefreshing() refreshing},
     *          and should be updated to reflect changes in a manner that is consistent with the results provided to a
     *          subscriber.
     * @implNote Implementations should attempt to provide a lazily-coalesced result wherever possible, allowing work to
     *           be deferred or parallelized.
     */
    @Nullable
    BasicDataIndex getDataIndex(@NotNull String... columns);

    /**
     * @param name The column name
     * @return The ColumnLocation for the defined column under this table location. The exact same ColumnLocation object
     *         should be returned for the same column name.
     */
    @NotNull
    ColumnLocation getColumnLocation(@NotNull CharSequence name);

    /**
     * Estimate the cost of pushing down this filter on this location. This returns a unitless value that can be used
     * to compare the cost of executing different filters.
     *
     * @param filter The {@link Filter filter} to test.
     * @param selection The set of rows to tests.
     * @param fullSet The full set of rows
     * @param usePrev Whether to use the previous result
     * @param context The {@link FilterContext} to use for the pushdown operation.
     * @return The estimated cost of the push down operation.
     */
    default long estimatePushdownFilterCost(
            WhereFilter filter,
            RowSet selection,
            RowSet fullSet,
            boolean usePrev,
            final FilterContext context) {
        return Long.MAX_VALUE;
    }

    /**
     * Push down the given filter to the underlying table and return the result.
     *
     * @param filter The {@link Filter filter} to apply.
     * @param input The set of rows to test.
     * @param usePrev Whether to use the previous result, if applicable
     * @param context The {@link FilterContext} to use for the pushdown operation.
     * @param costCeiling Execute all possible filters with a cost leq this value.
     * @return The result of the push down operation.
     */
    default PushdownResult pushdownFilter(
            final Map<String, ColumnSource<?>> columnSourceMap,
            final WhereFilter filter,
            final RowSet input,
            final boolean usePrev,
            final FilterContext context,
            final long costCeiling) {
        // Default to returning all results as "maybe"
        return PushdownResult.of(RowSetFactory.empty(), input.copy());
    }

    // ------------------------------------------------------------------------------------------------------------------
    // LogOutputAppendable implementation / toString() override helper
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getTableKey())
                .append(':').append(getImplementationName())
                .append('[').append(getKey())
                .append(']');
    }

    @FinalDefault
    default String toStringHelper() {
        return new LogOutputStringImpl().append(this).toString();
    }

    /**
     * Format the table key without implementation specific bits.
     *
     * @return a formatted string
     */
    @FinalDefault
    default String toGenericString() {
        return "TableLocation[" + getTableKey() + "]:[" + getKey() + ']';
    }

    /**
     * Optional toString path with more implementation detail.
     *
     * @return detailed conversion to string
     */
    @FinalDefault
    default String toStringDetailed() {
        return toStringHelper();
    }
}
