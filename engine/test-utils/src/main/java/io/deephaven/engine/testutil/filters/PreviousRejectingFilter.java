//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.filters;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegatingBase;
import org.jetbrains.annotations.NotNull;

/**
 * A WhereFilter that wraps another filter, but cannot filter previous values. A where using it during an update cycle,
 * before its source has been notified, cannot snapshot concurrently, and so falls back to the update graph lock.
 * <p>
 * This filter is memoizable when the wrapped filter is. It does not unwrap, so that a where filters through it.
 */
public class PreviousRejectingFilter extends WhereFilterDelegatingBase {

    /**
     * Creates a PreviousRejectingFilter that wraps the provided filter.
     *
     * @param filter the filter to wrap
     */
    public PreviousRejectingFilter(@NotNull final WhereFilter filter) {
        super(filter);
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }
        return super.filter(selection, fullSet, table, usePrev);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }
        return super.filterInverse(selection, fullSet, table, usePrev);
    }

    @Override
    public WhereFilter maybeUnwrapFilter() {
        return this;
    }

    @Override
    public WhereFilter copy() {
        return new PreviousRejectingFilter(filter.copy());
    }
}
