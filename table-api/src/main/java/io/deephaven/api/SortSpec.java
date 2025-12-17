//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

/**
 * A specification for sorting.
 *
 * <p>
 * Note, that the engine is responsible for interpreting the SortSpec. The SortSpec is differentiated from
 * {@link SortColumn}, because SortColumn is used in clients and many operations. The engine's sort operation may also
 * take other implementations of SortSpec that provide additional details. Naive code that expects SortColumn may not
 * correctly preserve the details.
 * </p>
 */
public interface SortSpec {
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
    SortSpec.Order order();

    default boolean isAscending() {
        return order() == Order.ASCENDING;
    }

    enum Order {
        ASCENDING, DESCENDING;

        public boolean isAscending() {
            return this == ASCENDING;
        }
    }
}
