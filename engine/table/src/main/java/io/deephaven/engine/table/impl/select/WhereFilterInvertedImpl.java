package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;

class WhereFilterInvertedImpl implements WhereFilter {

    static WhereFilter of(WhereFilter filter) {
        return new WhereFilterInvertedImpl(filter);
    }

    private final WhereFilter filter;

    private WhereFilterInvertedImpl(WhereFilter filter) {
        this.filter = Objects.requireNonNull(filter);
    }

    @Override
    public List<String> getColumns() {
        return filter.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return filter.getColumnArrays();
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        filter.init(tableDefinition);
    }

    @Override
    public void validateSafeForRefresh(BaseTable<?> sourceTable) {
        filter.validateSafeForRefresh(sourceTable);
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
    public boolean isSimpleFilter() {
        return filter.isSimpleFilter();
    }

    @Override
    public boolean isRefreshing() {
        return filter.isRefreshing();
    }

    @Override
    public void setRecomputeListener(RecomputeListener result) {
        filter.setRecomputeListener(result);
    }

    @Override
    public boolean isAutomatedFilter() {
        return filter.isAutomatedFilter();
    }

    @Override
    public void setAutomatedFilter(boolean value) {
        filter.setAutomatedFilter(value);
    }

    @Override
    public boolean canMemoize() {
        return filter.canMemoize();
    }

    @Override
    public WhereFilter copy() {
        return new WhereFilterInvertedImpl(filter.copy());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WhereFilterInvertedImpl that = (WhereFilterInvertedImpl) o;
        return filter.equals(that.filter);
    }

    @Override
    public int hashCode() {
        // Use class hashcode to improve hashcode to account for if a filter and its inverse are both in the same
        // HashMap
        return WhereFilterInvertedImpl.class.hashCode() ^ filter.hashCode();
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
