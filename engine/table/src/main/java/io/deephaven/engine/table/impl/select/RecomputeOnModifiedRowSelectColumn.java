//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import org.jetbrains.annotations.NotNull;


/**
 * {@link SelectColumn} implementation that wraps another {@link SelectColumn} and makes it report that values should be
 * recomputed when the row is modified via {@link #recomputeOnModifiedRow()}.
 */
class RecomputeOnModifiedRowSelectColumn extends WrappedSelectColumn {

    RecomputeOnModifiedRowSelectColumn(@NotNull final SelectColumn inner) {
        super(inner);
    }

    @Override
    public SelectColumn copy() {
        return new RecomputeOnModifiedRowSelectColumn(inner.copy());
    }

    @Override
    public boolean recomputeOnModifiedRow() {
        return true;
    }

    @Override
    public SelectColumn withRecomputeOnModifiedRow() {
        return copy();
    }
}
