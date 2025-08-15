//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import org.immutables.value.Value.Parameter;

/**
 * Represents a {@link #column() column} and {@link #order() order} pair.
 */
public interface SortColumn {

    enum Order {
        ASCENDING, DESCENDING
    }

    /**
     * Create an {@link Order#ASCENDING} sort column.
     *
     * @param columnName the column name
     * @return the ascending sort column
     */
    static SortColumn asc(ColumnName columnName) {
        return ImmutableSortColumnImpl.of(columnName, Order.ASCENDING);
    }

    /**
     * Create a {@link Order#DESCENDING} sort column.
     *
     * @param columnName the column name
     * @return the descending sort column
     */
    static SortColumn desc(ColumnName columnName) {
        return ImmutableSortColumnImpl.of(columnName, Order.DESCENDING);
    }

    /**
     * The column name.
     *
     * @return the column name
     */
    ColumnName column();

    /**
     * The order.
     *
     * @return the order
     */
    Order order();

    default boolean isAscending() {
        return order() == Order.ASCENDING;
    }
}
