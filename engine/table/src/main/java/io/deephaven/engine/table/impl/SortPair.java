package io.deephaven.engine.table.impl;

import io.deephaven.api.SortColumn;
import io.deephaven.api.SortColumn.Order;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A pair representing a column to sort by and its direction.
 */
public class SortPair {
    public static SortPair[] ZERO_LENGTH_SORT_PAIR_ARRAY = new SortPair[0];

    public static SortPair[] from(Collection<SortColumn> sortColumns) {
        return sortColumns.stream().map(SortPair::of).toArray(SortPair[]::new);
    }

    public static SortPair of(SortColumn sortColumn) {
        return new SortPair(
                sortColumn.column().name(),
                sortColumn.order() == Order.ASCENDING ? SortingOrder.Ascending : SortingOrder.Descending);
    }

    private final String column;
    private final SortingOrder order;

    /**
     * Create an ascending SortPair for name.
     * 
     * @param name the column name to sort by
     * @return an ascending SortPair for name
     */
    public static SortPair ascending(String name) {
        return new SortPair(name, SortingOrder.Ascending);
    }

    /**
     * Create an array of ascending SortPair for names.
     * 
     * @param names the column names to sort by
     * @return an ascending SortPair array for names
     */
    public static SortPair[] ascendingPairs(String... names) {
        return Arrays.stream(names).map(name -> new SortPair(name, SortingOrder.Ascending)).toArray(SortPair[]::new);
    }


    /**
     * Create an descending SortPair for name.
     * 
     * @param name the column name to sort by
     * @return an descending SortPair for name
     */
    public static SortPair descending(String name) {
        return new SortPair(name, SortingOrder.Descending);
    }

    /**
     * Create an array of descending SortPair for names.
     * 
     * @param names the column names to sort by
     * @return an descending SortPair array for names
     */
    public static SortPair[] descendingPairs(String... names) {
        return Arrays.stream(names).map(name -> new SortPair(name, SortingOrder.Descending)).toArray(SortPair[]::new);
    }

    private SortPair(String column, SortingOrder order) {
        this.column = column;
        this.order = order;
    }

    /**
     * @return the column name for this SortPair.
     */
    public String getColumn() {
        return column;
    }

    /**
     * @return the SortingOrder for this pair.
     */
    public SortingOrder getOrder() {
        return order;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final SortPair sortPair = (SortPair) o;
        return order == sortPair.order && Objects.equals(column, sortPair.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, order.direction);
    }

    @Override
    public String toString() {
        return order + "(" + column + ")";
    }
}
