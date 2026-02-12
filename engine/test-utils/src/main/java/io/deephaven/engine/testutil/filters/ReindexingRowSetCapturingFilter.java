//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.filters;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.select.ReindexingFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public class ReindexingRowSetCapturingFilter extends RowSetCapturingFilter implements ReindexingFilter {
    private final ReindexingFilter innerReindexingFilter;

    public ReindexingRowSetCapturingFilter(Filter filter) {
        super(filter);
        this.innerReindexingFilter = check(filter);
    }

    public ReindexingRowSetCapturingFilter(WhereFilter filter, List<RowSet> rowSets) {
        super(filter, rowSets);
        this.innerReindexingFilter = check(filter);
    }

    static ReindexingFilter check(Filter filter) {
        if (filter instanceof ReindexingFilter) {
            return (ReindexingFilter) filter;
        }
        throw new IllegalArgumentException("Wrapped Filter is not a ReindexingFilter");
    }

    @Override
    public boolean requiresSorting() {
        return innerReindexingFilter.requiresSorting();
    }

    @Override
    public @Nullable String[] getSortColumns() {
        return innerReindexingFilter.getSortColumns();
    }

    @Override
    public void sortingDone() {
        innerReindexingFilter.sortingDone();
    }

    @Override
    public WhereFilter copy() {
        if (innerFilter != null) {
            final WhereFilter newInner = innerFilter.copy();
            if (newInner != innerFilter) {
                // note we share the rowset collection
                return new ReindexingRowSetCapturingFilter(newInner, rowSets);
            }
        }
        return this;
    }
}
