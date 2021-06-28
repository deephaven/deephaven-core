/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.api.ColumnMatch;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.select.SelectFilterFactory;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.api.ColumnName;
import io.deephaven.api.Filter;
import io.deephaven.api.RawString;
import java.util.Collection;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Interface for individual filters within a where clause.
 */
public interface SelectFilter {

    static SelectFilter of(Filter filter) {
        return filter.walk(new Adapter()).getOut();
    }

    static SelectFilter[] from(Collection<Filter> filters) {
        return filters.stream().map(SelectFilter::of).toArray(SelectFilter[]::new);
    }

    /**
     * Users of SelectFilter may implement this interface if they must react to the filter fundamentally changing.
     *
     * @see DynamicWhereFilter
     */
    interface RecomputeListener {
        /**
         * Notify the listener that its result must be recomputed.
         */
        void requestRecompute();

        /**
         * Notify the something about the filters has changed such that all unmatched rows of the source table should be re-evaluated.
         */
        void requestRecomputeUnmatched();

        /**
         * Notify the something about the filters has changed such that all matched rows of the source table should be re-evaluated.
         */
        void requestRecomputeMatched();

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

    SelectFilter [] ZERO_LENGTH_SELECT_FILTER_ARRAY = new SelectFilter[0];

    /**
     * Get the columns required by this select filter.
     *
     * @return the columns used as input by this select filter.
     */
    List<String> getColumns();

    /**
     * Get the array columns required by this select filter.
     *
     * @return the columns used as array input by this select filter.
     */
    List<String> getColumnArrays();

    /**
     * Initialize this select filter given the table definition
     * @param tableDefinition the definition of the table that will be filtered
     */
    void init(TableDefinition tableDefinition);

    /**
     * Filter selection to only matching rows.
     *
     * @param selection the indices that should be filtered.  The selection must be a subset of fullSet; and may not
     *                  include rows that the engine determines need not be evaluated to produce the result.
     * @param fullSet the complete Index of the table to filter.  The fullSet is used for calculating variables like
     *                "i" or "ii".
     * @param table the table to filter
     * @param usePrev true if previous values should be used.  Implementing previous value filtering is optional, and
     *                a {@link PreviousFilteringNotSupported} exception may be thrown.  If a PreviousFiltering exception
     *                is thrown, then the caller must acquire the LiveTableMonitor lock.
     *
     * @return the subset of selection accepted by this filter
     */
    Index filter(Index selection, Index fullSet, Table table, boolean usePrev);

    /**
      * @return true if this is a filter that does not require any code execution, but rather is handled entirely
     *  within the database engine.
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
     * Set the Listener that should be notified if results based on this filter must be recomputed.
     *
     * @param result the listener to notify.
     */
    void setRecomputeListener(RecomputeListener result);

    /**
     * The database system may automatically generate a filter, for example, when applying an ACL to a table.  There
     * are certain operations which may bypass these filters.
     *
     * This function returns whether or not this filter is automated.
     *
     * @return true if this filter was automatically applied by the database system.  False otherwise.
     */
    boolean isAutomatedFilter();

    /**
     * The database system may automatically generate a filter, for example, when applying an ACL to a table.  There
     * are certain operations which may bypass these filters.
     *
     * This function indicates that this filter is automated.
     *
     * @param value true if this filter was automatically applied by the database system.  False otherwise.
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
     * Create a copy of this SelectFilter.
     *
     * @return an independent copy of this SelectFilter.
     */
    SelectFilter copy();

    /**
     * This exception is thrown when a where() filter is incapable of handling previous values, and thus needs to be
     * executed while under the LTM lock.
     */
    class PreviousFilteringNotSupported extends ConstructSnapshot.NoSnapshotAllowedException {
        public PreviousFilteringNotSupported() {
            super();
        }

        public PreviousFilteringNotSupported(String message) {
            super(message);
        }
    }

    class Adapter implements Filter.Visitor {
        private SelectFilter out;

        private Adapter() {}

        public SelectFilter getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName name) {
            // todo: improve
            out = SelectFilterFactory.getExpression(name.name());
        }

        @Override
        public void visit(ColumnMatch match) {
            // todo: improve
            out = SelectFilterFactory.getExpression(String.format("%s==%s", match.left().name(), match.right().name()));
        }

        @Override
        public void visit(RawString rawString) {
            out = SelectFilterFactory.getExpression(rawString.value());
        }
    }
}
