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
