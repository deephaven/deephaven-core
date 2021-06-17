package io.deephaven.api;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class SortColumn {

    public enum Order {
        ASCENDING, DESCENDING
    }

    public static SortColumn asc(ColumnName columnName) {
        return ImmutableSortColumn.of(columnName, Order.ASCENDING);
    }

    public static SortColumn desc(ColumnName columnName) {
        return ImmutableSortColumn.of(columnName, Order.DESCENDING);
    }

    @Parameter
    public abstract ColumnName column();

    @Parameter
    public abstract Order order();
}
