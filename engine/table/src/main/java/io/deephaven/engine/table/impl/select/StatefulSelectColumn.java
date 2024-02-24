/**
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.MatchPair;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

/**
 * A {@link SelectColumn} that wraps another {@code SelectColumn}, and is used to indicate that the wrapped column has
 * side effects that prevent parallelization. (e.g. a column that depends on processing rows in order, or would suffer
 * from lock contention if parallelized)
 */
@Value.Immutable
@SimpleStyle
public abstract class StatefulSelectColumn implements SelectColumn {

    public static StatefulSelectColumn of(@NotNull final SelectColumn innerSelectColumn) {
        return ImmutableStatefulSelectColumn.of(innerSelectColumn);
    }

    @Value.Parameter
    public abstract SelectColumn innerSelectColumn();

    @Override
    public List<String> initInputs(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        return innerSelectColumn().initInputs(rowSet, columnsOfInterest);
    }

    @Override
    public List<String> initDef(
            @NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        return innerSelectColumn().initDef(columnDefinitionMap);
    }

    @Override
    public Class<?> getReturnedType() {
        return innerSelectColumn().getReturnedType();
    }

    @Override
    public List<String> getColumns() {
        return innerSelectColumn().getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return innerSelectColumn().getColumnArrays();
    }

    @Override
    public @NotNull ColumnSource<?> getDataView() {
        return innerSelectColumn().getDataView();
    }

    @Override
    public @NotNull ColumnSource<?> getLazyView() {
        return innerSelectColumn().getLazyView();
    }

    @Override
    public String getName() {
        return innerSelectColumn().getName();
    }

    @Override
    public MatchPair getMatchPair() {
        return innerSelectColumn().getMatchPair();
    }

    @Override
    public WritableColumnSource<?> newDestInstance(long size) {
        return innerSelectColumn().newDestInstance(size);
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(long size) {
        return innerSelectColumn().newFlatDestInstance(size);
    }

    @Override
    public boolean isRetain() {
        return innerSelectColumn().isRetain();
    }

    @Override
    public SelectColumn copy() {
        return StatefulSelectColumn.of(innerSelectColumn().copy());
    }

    @Override
    public boolean isStateless() {
        return false;
    }
}
