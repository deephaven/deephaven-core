package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;

import java.util.List;
import java.util.Objects;

class WhereFilterInvertedImpl implements WhereFilter {
    private final WhereFilter impl;

    public WhereFilterInvertedImpl(WhereFilter impl) {
        this.impl = Objects.requireNonNull(impl);
    }

    @Override
    public List<String> getColumns() {
        return impl.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return impl.getColumnArrays();
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        impl.init(tableDefinition);
    }

    @Override
    public void validateSafeForRefresh(BaseTable<?> sourceTable) {
        impl.validateSafeForRefresh(sourceTable);
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        return impl.filterInverse(selection, fullSet, table, usePrev);
    }

    @Override
    public WritableRowSet filterInverse(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        return impl.filter(selection, fullSet, table, usePrev);
    }

    @Override
    public boolean isSimpleFilter() {
        return impl.isSimpleFilter();
    }

    @Override
    public boolean isRefreshing() {
        return impl.isRefreshing();
    }

    @Override
    public void setRecomputeListener(RecomputeListener result) {
        impl.setRecomputeListener(result);
    }

    @Override
    public boolean isAutomatedFilter() {
        return impl.isAutomatedFilter();
    }

    @Override
    public void setAutomatedFilter(boolean value) {
        impl.setAutomatedFilter(value);
    }

    @Override
    public boolean canMemoize() {
        return impl.canMemoize();
    }

    @Override
    public WhereFilter copy() {
        return new WhereFilterInvertedImpl(impl.copy());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WhereFilterInvertedImpl that = (WhereFilterInvertedImpl) o;
        return impl.equals(that.impl);
    }

    @Override
    public int hashCode() {
        return impl.hashCode();
    }

    @Override
    public String toString() {
        return "WhereFilterInvertedImpl{" +
                "impl=" + impl +
                '}';
    }
}
