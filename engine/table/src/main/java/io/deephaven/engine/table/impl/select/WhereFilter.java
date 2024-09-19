//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Interface for individual filters within a where clause.
 */
public interface WhereFilter extends Filter {

    static WhereFilter of(Filter filter) {
        return (filter instanceof WhereFilter) ? (WhereFilter) filter : WhereFilterAdapter.of(filter);
    }

    static WhereFilter[] from(Collection<? extends Filter> filters) {
        return filters.stream().map(WhereFilter::of).toArray(WhereFilter[]::new);
    }

    static WhereFilter[] copyFrom(WhereFilter[] filters) {
        return Arrays.stream(filters).map(WhereFilter::copy).toArray(WhereFilter[]::new);
    }

    @InternalUseOnly
    static WhereFilter[] fromInternal(Filter filter) {
        return from(FilterToListImpl.of(filter));
    }

    /**
     * Users of WhereFilter may implement this interface if they must react to the filter fundamentally changing.
     *
     * @see DynamicWhereFilter
     */
    interface RecomputeListener {
        /**
         * Notify the listener that its result must be recomputed.
         */
        void requestRecompute();

        /**
         * Notify that something about the filters has changed such that all unmatched rows of the source table should
         * be re-evaluated.
         */
        void requestRecomputeUnmatched();

        /**
         * Notify that something about the filters has changed such that all matched rows of the source table should be
         * re-evaluated.
         */
        void requestRecomputeMatched();

        /**
         * Notify that something about the filters has changed such that the following rows of the source table should
         * be re-evaluated. The rowSet ownership is not taken by requestRecompute.
         */
        void requestRecompute(RowSet rowSet);

        /**
         * Get the table underlying this listener.
         *
         * @return the underlying table
         */
        @NotNull
        QueryTable getTable();

        /**
         * Set the filter and the table refreshing or not.
         */
        void setIsRefreshing(boolean refreshing);
    }

    WhereFilter[] ZERO_LENGTH_WHERE_FILTER_ARRAY = new WhereFilter[0];

    /**
     * Get the columns required by this select filter.
     * <p>
     * This filter must already be initialized before calling this method.
     *
     * @return the columns used as input by this select filter.
     */
    List<String> getColumns();

    /**
     * Get the array columns required by this select filter.
     * <p>
     * This filter must already be initialized before calling this method.
     *
     * @return the columns used as array input by this select filter.
     */
    List<String> getColumnArrays();

    /**
     * Initialize this filter given the table definition. If this filter has already been initialized, this should be a
     * no-op, or optionally validate that the table definition is compatible with previous initialization.
     *
     * @param tableDefinition the definition of the table that will be filtered
     * @apiNote Any {@link io.deephaven.engine.context.QueryLibrary}, {@link io.deephaven.engine.context.QueryScope}, or
     *          {@link QueryCompiler} usage needs to be resolved within init. Implementations must be idempotent.
     */
    void init(@NotNull TableDefinition tableDefinition);

    /**
     * Initialize this select filter given the table definition
     *
     * @param tableDefinition the definition of the table that will be filtered
     * @param compilationProcessor the processor to use for compilation
     * @apiNote Any {@link io.deephaven.engine.context.QueryLibrary}, {@link io.deephaven.engine.context.QueryScope}, or
     *          {@link QueryCompiler} usage needs to be resolved within init. Implementations must be idempotent.
     */
    @SuppressWarnings("unused")
    default void init(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
        init(tableDefinition);
    }

    /**
     * Perform any operation-level initialization necessary using the {@link Table} that will be filtered with this
     * WhereFilter, e.g. gathering {@link DataIndex data indexes}. This method will always be called exactly once,
     * before gathering any dependencies or filtering data.
     *
     * @param sourceTable The {@link Table} that will be filtered with this WhereFilter
     * @return A {@link SafeCloseable} that will be {@link SafeCloseable#close() closed} when the operation is complete,
     *         whether successful or not
     */
    default SafeCloseable beginOperation(@NotNull final Table sourceTable) {
        return () -> {
        };
    }

    /**
     * Validate that this {@code WhereFilter} is safe to use in the context of the provided sourceTable.
     *
     * @param sourceTable the source table
     */
    default void validateSafeForRefresh(final BaseTable<?> sourceTable) {
        // nothing to validate by default
    }

    /**
     * Filter selection to only matching rows.
     *
     * @param selection the indices that should be filtered. The selection must be a subset of fullSet, and may include
     *        rows that the engine determines need not be evaluated to produce the result. Implementations <em>may
     *        not</em> mutate or {@link RowSet#close() close} {@code selection}.
     * @param fullSet the complete RowSet of the table to filter. The fullSet is used for calculating variables like "i"
     *        or "ii". Implementations <em>may not</em> mutate or {@link RowSet#close() close} {@code fullSet}.
     * @param table the table to filter
     * @param usePrev true if previous values should be used. Implementing previous value filtering is optional, and a
     *        {@link PreviousFilteringNotSupported} exception may be thrown. If a PreviousFiltering exception is thrown,
     *        then the caller must acquire the PeriodicUpdateGraph lock.
     *
     * @return The subset of selection accepted by this filter; ownership passes to the caller
     */
    @NotNull
    WritableRowSet filter(
            @NotNull RowSet selection,
            @NotNull RowSet fullSet,
            @NotNull Table table,
            boolean usePrev);

    /**
     * Filter selection to only non-matching rows.
     *
     * <p>
     * Defaults to
     *
     * <pre>
     * {@code
     * try (final WritableRowSet regular = filter(selection, fullSet, table, usePrev)) {
     *     return selection.minus(regular);
     * }
     * }
     * </pre>
     *
     * <p>
     * Implementations are encouraged to override this when they can provide more efficient implementations.
     *
     * @param selection the indices that should be filtered. The selection must be a subset of fullSet, and may include
     *        rows that the engine determines need not be evaluated to produce the result. Implementations <em>may
     *        not</em> mutate or {@link RowSet#close() close} {@code selection}.
     * @param fullSet the complete RowSet of the table to filter. The fullSet is used for calculating variables like "i"
     *        or "ii". Implementations <em>may not</em> mutate or {@link RowSet#close() close} {@code fullSet}.
     * @param table the table to filter
     * @param usePrev true if previous values should be used. Implementing previous value filtering is optional, and a
     *        {@link PreviousFilteringNotSupported} exception may be thrown. If a PreviousFiltering exception is thrown,
     *        then the caller must acquire the PeriodicUpdateGraph lock.
     *
     * @return The subset of selection not accepted by this filter; ownership passes to the caller
     */
    @NotNull
    default WritableRowSet filterInverse(
            @NotNull RowSet selection,
            @NotNull RowSet fullSet,
            @NotNull Table table,
            boolean usePrev) {
        try (final WritableRowSet regular = filter(selection, fullSet, table, usePrev)) {
            return selection.minus(regular);
        }
    }

    /**
     * Delegates to {@link #filter(RowSet, RowSet, Table, boolean)} when {@code invert == false} and
     * {@link #filterInverse(RowSet, RowSet, Table, boolean)} when {@code invert == true}.
     *
     * @param selection the indices that should be filtered. The selection must be a subset of fullSet, and may include
     *        rows that the engine determines need not be evaluated to produce the result. Implementations <em>may
     *        not</em> mutate or {@link RowSet#close() close} {@code selection}.
     * @param fullSet the complete RowSet of the table to filter. The fullSet is used for calculating variables like "i"
     *        or "ii". Implementations <em>may not</em> mutate or {@link RowSet#close() close} {@code fullSet}.
     * @param table the table to filter
     * @param usePrev true if previous values should be used. Implementing previous value filtering is optional, and a
     *        {@link PreviousFilteringNotSupported} exception may be thrown. If a PreviousFiltering exception is thrown,
     *        then the caller must acquire the PeriodicUpdateGraph lock.
     * @param invert if the filter should be inverted
     * @return The subset of selection; ownership passes to the caller
     */
    @FinalDefault
    default WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev, boolean invert) {
        return invert
                ? filterInverse(selection, fullSet, table, usePrev)
                : filter(selection, fullSet, table, usePrev);
    }

    /**
     * @return true if this is a filter that does not require any code execution, but rather is handled entirely within
     *         the database engine.
     */
    boolean isSimpleFilter();

    /**
     * Is this filter refreshing?
     *
     * @return if this filter is refreshing
     */
    default boolean isRefreshing() {
        return false;
    }

    /**
     * @return if this filter can be applied in parallel
     */
    default boolean permitParallelization() {
        return true;
    }

    /**
     * Set the {@link RecomputeListener} that should be notified if results based on this WhereFilter must be
     * recomputed.
     *
     * @param result The {@link RecomputeListener} to notify
     */
    void setRecomputeListener(RecomputeListener result);

    /**
     * The database system may automatically generate a filter, for example, when applying an ACL to a table. There are
     * certain operations which may bypass these filters.
     * <p>
     * This function returns whether this filter is automated.
     *
     * @return true if this filter was automatically applied by the database system. False otherwise.
     */
    boolean isAutomatedFilter();

    /**
     * The database system may automatically generate a filter, for example, when applying an ACL to a table. There are
     * certain operations which may bypass these filters.
     * <p>
     * This function indicates that this filter is automated.
     *
     * @param value true if this filter was automatically applied by the database system. False otherwise.
     */
    void setAutomatedFilter(boolean value);

    /**
     * Can this filter operation be memoized?
     *
     * @return if this filter can be memoized
     */
    default boolean canMemoize() {
        return false;
    }

    /**
     * Create a copy of this WhereFilter.
     *
     * @return an independent copy of this WhereFilter.
     */
    WhereFilter copy();

    /**
     * This exception is thrown when a where() filter is incapable of handling previous values, and thus needs to be
     * executed while under the UGP lock.
     */
    class PreviousFilteringNotSupported extends ConstructSnapshot.NoSnapshotAllowedException {
        public PreviousFilteringNotSupported() {
            super();
        }

        public PreviousFilteringNotSupported(String message) {
            super(message);
        }
    }

    // region Filter impl

    @Override
    default Filter invert() {
        throw new UnsupportedOperationException("WhereFilters do not implement invert");
    }

    @Override
    default <T> T walk(Expression.Visitor<T> visitor) {
        throw new UnsupportedOperationException("WhereFilters do not implement walk");
    }

    @Override
    default <T> T walk(Filter.Visitor<T> visitor) {
        throw new UnsupportedOperationException("WhereFilters do not implement walk");
    }

    // endregion Filter impl
}
