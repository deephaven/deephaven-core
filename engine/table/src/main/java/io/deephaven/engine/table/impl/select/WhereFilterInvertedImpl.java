//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

public class WhereFilterInvertedImpl extends WhereFilterDelegatingBase {

    public static WhereFilter of(WhereFilter filter) {
        return new WhereFilterInvertedImpl(filter);
    }

    private WhereFilterInvertedImpl(WhereFilter filter) {
        super(filter);
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return filter.filterInverse(selection, fullSet, table, usePrev);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return filter.filter(selection, fullSet, table, usePrev);
    }

    @Override
    public WhereFilter copy() {
        return new WhereFilterInvertedImpl(filter.copy());
    }

    @Override
    public String toString() {
        return "not(" + filter + ")";
    }

    @VisibleForTesting
    WhereFilter filter() {
        return filter;
    }
}
