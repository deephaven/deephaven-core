//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

public interface EngineSortSpec {
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
    SortColumn.Order order();

    default boolean isAscending() {
        return order() == SortColumn.Order.ASCENDING;
    }
}
