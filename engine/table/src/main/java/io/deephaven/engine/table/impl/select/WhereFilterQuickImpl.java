package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.FilterQuick;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;

import java.util.List;
import java.util.Objects;

class WhereFilterQuickImpl extends WhereFilterImpl {

    static WhereFilterQuickImpl of(FilterQuick quick, boolean inverted) {
        if (!(quick.expression() instanceof ColumnName)) {
            throw new IllegalArgumentException("WhereFilterQuickImpl only supports filtering against a column name");
        }
        if (inverted) {
            throw new UnsupportedOperationException("WhereFilterQuickImpl does not support inverted");
        }
        return new WhereFilterQuickImpl(quick);
    }

    private final FilterQuick quick;
    private transient WhereFilter impl;

    private WhereFilterQuickImpl(FilterQuick quick) {
        this.quick = Objects.requireNonNull(quick);
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        impl = WhereFilterFactory.quickFilter(quick, tableDefinition);
        impl.init(tableDefinition);
    }

    @Override
    public boolean canMemoize() {
        return true;
    }

    @Override
    public List<String> getColumns() {
        return List.of(((ColumnName) quick.expression()).name());
    }

    @Override
    public List<String> getColumnArrays() {
        return List.of();
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        return impl.filter(selection, fullSet, table, usePrev);
    }

    @Override
    public boolean isSimpleFilter() {
        return impl.isSimpleFilter();
    }

    @Override
    public void setRecomputeListener(RecomputeListener result) {
        impl.setRecomputeListener(result);
    }

    @Override
    public void validateSafeForRefresh(BaseTable<?> sourceTable) {
        impl.validateSafeForRefresh(sourceTable);
    }

    @Override
    public boolean isRefreshing() {
        return impl.isRefreshing();
    }

    @Override
    public WhereFilter copy() {
        return new WhereFilterQuickImpl(quick);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WhereFilterQuickImpl that = (WhereFilterQuickImpl) o;
        return quick.equals(that.quick);
    }

    @Override
    public int hashCode() {
        return quick.hashCode();
    }

    @Override
    public String toString() {
        return quick.toString();
    }
}
