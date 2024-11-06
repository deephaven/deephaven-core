//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

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
