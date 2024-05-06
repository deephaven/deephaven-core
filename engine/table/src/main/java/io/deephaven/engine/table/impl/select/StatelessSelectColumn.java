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
class StatelessSelectColumn implements SelectColumn {

    private final SelectColumn inner;

    StatelessSelectColumn(@NotNull final SelectColumn inner) {
        this.inner = inner;
    }

    @Override
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        return inner.initInputs(rowSet, columnsOfInterest);
    }

    @Override
    public List<String> initDef(@NotNull Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        return inner.initDef(columnDefinitionMap);
    }

    @Override
    public List<String> initDef(@NotNull Map<String, ColumnDefinition<?>> columnDefinitionMap,
            @NotNull QueryCompilerRequestProcessor compilationRequestProcessor) {
        return inner.initDef(columnDefinitionMap, compilationRequestProcessor);
    }

    @Override
    public Class<?> getReturnedType() {
        return inner.getReturnedType();
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return inner.getReturnedComponentType();
    }

    @Override
    public List<String> getColumns() {
        return inner.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return inner.getColumnArrays();
    }

    @Override
    public @NotNull ColumnSource<?> getDataView() {
        return inner.getDataView();
    }

    @Override
    public @NotNull ColumnSource<?> getLazyView() {
        return inner.getLazyView();
    }

    @Override
    public String getName() {
        return inner.getName();
    }

    @Override
    public MatchPair getMatchPair() {
        return inner.getMatchPair();
    }

    @Override
    public WritableColumnSource<?> newDestInstance(long size) {
        return inner.newDestInstance(size);
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(long size) {
        return inner.newFlatDestInstance(size);
    }

    @Override
    public boolean isRetain() {
        return inner.isRetain();
    }

    @Override
    public void validateSafeForRefresh(BaseTable<?> sourceTable) {
        inner.validateSafeForRefresh(sourceTable);
    }

    @Override
    public boolean isStateless() {
        return true;
    }

    @Override
    public SelectColumn copy() {
        return new StatelessSelectColumn(inner.copy());
    }

    @Override
    public ColumnName newColumn() {
        return inner.newColumn();
    }

    @Override
    public Expression expression() {
        return inner.expression();
    }
}
