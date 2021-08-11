/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterCondition;
import io.deephaven.api.filter.FilterIsNotNull;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.value.Value;
import io.deephaven.api.value.Value.Visitor;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.select.SelectFilterFactory;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.select.MatchFilter.MatchType;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Interface for individual filters within a where clause.
 */
public interface SelectFilter {

    static SelectFilter of(Filter filter) {
        return filter.walk(new Adapter(false)).getOut();
    }

    static SelectFilter ofInverted(Filter filter) {
        return filter.walk(new Adapter(true)).getOut();
    }

    static SelectFilter[] from(Collection<? extends Filter> filters) {
        return filters.stream().map(SelectFilter::of).toArray(SelectFilter[]::new);
    }

    static SelectFilter[] fromInverted(Collection<? extends Filter> filters) {
        return filters.stream().map(SelectFilter::ofInverted).toArray(SelectFilter[]::new);
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
        private final boolean inverted;
        private SelectFilter out;

        private Adapter(boolean inverted) {
            this.inverted = inverted;
        }

        public SelectFilter getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(FilterCondition condition) {
            out = FilterConditionAdapter.of(inverted ? condition.invert() : condition);
        }

        @Override
        public void visit(FilterNot not) {
            out = not.filter().walk(new Adapter(!inverted)).getOut();
        }

        @Override
        public void visit(FilterIsNull isNull) {
            if (inverted) {
                out = isNotNull(isNull.column());
            } else {
                out = isNull(isNull.column());
            }
        }

        @Override
        public void visit(FilterIsNotNull isNotNull) {
            if (inverted) {
                out = isNull(isNotNull.column());
            } else {
                out = isNotNull(isNotNull.column());
            }
        }

        @Override
        public void visit(FilterOr ors) {
            if (inverted) {
                // !A && !B && ... && !Z
                out = ConjunctiveFilter.makeConjunctiveFilter(fromInverted(ors.filters()));
            } else {
                // A || B || ... || Z
                out = DisjunctiveFilter.makeDisjunctiveFilter(from(ors.filters()));
            }
        }

        @Override
        public void visit(FilterAnd ands) {
            if (inverted) {
                // !A || !B || ... || !Z
                out = DisjunctiveFilter.makeDisjunctiveFilter(fromInverted(ands.filters()));
            } else {
                // A && B && ... && Z
                out = ConjunctiveFilter.makeConjunctiveFilter(from(ands.filters()));
            }
        }

        @Override
        public void visit(RawString rawString) {
            if (inverted) {
                out = SelectFilterFactory.getExpression(String.format("!(%s)", rawString.value()));
            } else {
                out = SelectFilterFactory.getExpression(rawString.value());
            }
        }

        private static MatchFilter isNull(ColumnName columnName) {
            return new MatchFilter(columnName.name(), new Object[] { null });
        }

        private static MatchFilter isNotNull(ColumnName columnName) {
            return new MatchFilter(MatchType.Inverted, columnName.name(), new Object[] { null });
        }

        private static class FilterConditionAdapter implements Value.Visitor {

            public static SelectFilter of(FilterCondition condition) {
                FilterCondition preferred = condition.maybeTranspose();
                return preferred.lhs().walk(new FilterConditionAdapter(condition, preferred)).getOut();
            }

            private final FilterCondition original;
            private final FilterCondition preferred;

            private SelectFilter out;

            private FilterConditionAdapter(FilterCondition original, FilterCondition preferred) {
                this.original = Objects.requireNonNull(original);
                this.preferred = Objects.requireNonNull(preferred);
            }

            public SelectFilter getOut() {
                return Objects.requireNonNull(out);
            }

            @Override
            public void visit(ColumnName lhs) {
                preferred.rhs().walk(new Visitor() {
                    @Override
                    public void visit(ColumnName rhs) {
                        out = SelectFilterFactory.getExpression(Strings.of(original));
                    }

                    @Override
                    public void visit(long rhs) {
                        switch (preferred.operator()) {
                            case LESS_THAN:
                                out = new LongRangeFilter(lhs.name(), Long.MIN_VALUE, rhs, true, false);
                                break;
                            case LESS_THAN_OR_EQUAL:
                                out = new LongRangeFilter(lhs.name(), Long.MIN_VALUE, rhs, true, true);
                                break;
                            case GREATER_THAN:
                                out = new LongRangeFilter(lhs.name(), rhs, Long.MAX_VALUE, false, true);
                                break;
                            case GREATER_THAN_OR_EQUAL:
                                out = new LongRangeFilter(lhs.name(), rhs, Long.MAX_VALUE, true, true);
                                break;
                            case EQUALS:
                                out = new MatchFilter(lhs.name(), rhs);
                                break;
                            case NOT_EQUALS:
                                out = new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                                break;
                            default:
                                throw new IllegalStateException("Unexpected operator " + original.operator());
                        }
                    }
                });
            }

            // Note for all remaining cases: since we are walking the preferred object, we know we don't have to handle
            // the case where rhs is column name.

            @Override
            public void visit(long lhs) {
                out = SelectFilterFactory.getExpression(Strings.of(original));
            }
        }
    }
}
