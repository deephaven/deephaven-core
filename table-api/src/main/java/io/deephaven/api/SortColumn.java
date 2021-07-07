package io.deephaven.api;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.Serializable;

/**
 * Represents a {@link #column() column} and {@link #order() order} pair.
 */
@Immutable
@SimpleStyle
public abstract class SortColumn implements Serializable {

    public enum Order {
        ASCENDING, DESCENDING;
    }

    public static SortColumn asc(ColumnName columnName) {
        return ImmutableSortColumn.of(columnName, Order.ASCENDING);
    }

    public static SortColumn desc(ColumnName columnName) {
        return ImmutableSortColumn.of(columnName, Order.DESCENDING);
    }

    /**
     * The column name.
     *
     * @return the column name
     */
    @Parameter
    public abstract ColumnName column();

    /**
     * The order.
     *
     * @return the order
     */
    @Parameter
    public abstract Order order();
}
