//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
import java.util.Optional;

/**
 * {@link SelectColumn} implementation that wraps another {@link SelectColumn}.
 */
abstract class WrappedSelectColumn implements SelectColumn {

    /**
     * The select column that is being wrapped.
     */
    protected final SelectColumn inner;

    WrappedSelectColumn(@NotNull final SelectColumn inner) {
        this.inner = inner;
    }

    @Override
    public List<String> initInputs(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        return inner.initInputs(rowSet, columnsOfInterest);
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        return inner.initDef(columnDefinitionMap);
    }

    @Override
    public List<String> initDef(
            @NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            @NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
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
    @NotNull
    public ColumnSource<?> getDataView() {
        return inner.getDataView();
    }

    @Override
    @NotNull
    public ColumnSource<?> getLazyView() {
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
    public WritableColumnSource<?> newDestInstance(final long size) {
        return inner.newDestInstance(size);
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(final long size) {
        return inner.newFlatDestInstance(size);
    }

    @Override
    public boolean isRetain() {
        return inner.isRetain();
    }

    @Override
    public void validateSafeForRefresh(@NotNull final BaseTable<?> sourceTable) {
        inner.validateSafeForRefresh(sourceTable);
    }

    @Override
    public boolean isStateless() {
        return inner.isStateless();
    }

    @Override
    public boolean isParallelizable() {
        return inner.isParallelizable();
    }

    @Override
    public boolean hasVirtualRowVariables() {
        return inner.hasVirtualRowVariables();
    }

    @Override
    public boolean recomputeOnModifiedRow() {
        return inner.recomputeOnModifiedRow();
    }

    @Override
    public ColumnName newColumn() {
        return inner.newColumn();
    }

    @Override
    public Expression expression() {
        return inner.expression();
    }

    @Override
    public boolean hasConstantArrayAccess() {
        return inner.hasConstantArrayAccess();
    }

    @Override
    public boolean hasConstantValue() {
        return inner.hasConstantValue();
    }

    @Override
    public Optional<SourceColumn> maybeGetSourceColumn() {
        return inner.maybeGetSourceColumn();
    }

    @Override
    public Optional<FormulaColumn> maybeGetFormulaColumn() {
        return inner.maybeGetFormulaColumn();
    }

    @Override
    public Object[] respectedBarriers() {
        return inner.respectedBarriers();
    }

    @Override
    public Object[] declaredBarriers() {
        return inner.declaredBarriers();
    }
}
