package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
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
        ASCENDING, DESCENDING
    }

    /**
     * Create an {@link Order#ASCENDING} sort column.
     *
     * @param columnName the column name
     * @return the ascending sort column
     */
    public static SortColumn asc(ColumnName columnName) {
        return ImmutableSortColumn.of(columnName, Order.ASCENDING);
    }

    /**
     * Create a {@link Order#DESCENDING} sort column.
     *
     * @param columnName the column name
     * @return the descending sort column
     */
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
