//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import org.jetbrains.annotations.NotNull;


/**
 * {@link SelectColumn} implementation that wraps another {@link SelectColumn} and makes it report to be
 * {@link #isStateless() stateless}.
 */
class StatelessSelectColumn extends WrappedSelectColumn {

    StatelessSelectColumn(@NotNull final SelectColumn inner) {
        super(inner);
    }

    @Override
    public boolean isStateless() {
        return true;
    }

    @Override
    public SelectColumn copy() {
        return new StatelessSelectColumn(inner.copy());
    }
}
