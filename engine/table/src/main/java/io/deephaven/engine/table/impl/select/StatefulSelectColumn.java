//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import org.jetbrains.annotations.NotNull;

/**
 * {@link SelectColumn} implementation that wraps another {@link SelectColumn} and makes it report to be stateful (i.e.
 * {@link #isStateless()} returns false).
 */
class StatefulSelectColumn extends WrappedSelectColumn {

    StatefulSelectColumn(@NotNull final SelectColumn inner) {
        super(inner);
    }

    @Override
    public boolean isStateless() {
        return false;
    }

    @Override
    public SelectColumn copy() {
        return new StatefulSelectColumn(inner.copy());
    }
}
