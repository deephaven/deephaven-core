/**
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * A {@link WhereFilter} that wraps another {@code WhereFilter}, and is used to indicate that the wrapped filter has
 * side effects that prevent parallelization. (e.g. a filter that depends on processing rows in order, or would suffer
 * from lock contention if parallelized)
 */
@Value.Immutable
@SimpleStyle
public abstract class StatefulWhereFilter implements WhereFilter {

    public static StatefulWhereFilter of(@NotNull final WhereFilter inner) {
        return ImmutableStatefulWhereFilter.of(inner);
    }

    @Value.Parameter
    public abstract WhereFilter innerFilter();

    @Override
    public List<String> getColumns() {
        return innerFilter().getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return innerFilter().getColumnArrays();
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        innerFilter().init(tableDefinition);
    }

    @Override
    public @NotNull WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            boolean usePrev) {
        return innerFilter().filter(selection, fullSet, table, usePrev);
    }

    @Override
    public boolean isSimpleFilter() {
        return innerFilter().isSimpleFilter();
    }

    @Override
    public void setRecomputeListener(RecomputeListener result) {
        innerFilter().setRecomputeListener(result);
    }

    @Override
    public boolean isAutomatedFilter() {
        return innerFilter().isAutomatedFilter();
    }

    @Override
    public void setAutomatedFilter(boolean value) {
        innerFilter().setAutomatedFilter(value);
    }

    @Override
    public WhereFilter copy() {
        return StatefulWhereFilter.of(innerFilter().copy());
    }

    @Override
    public boolean isStateless() {
        return false;
    }
}
