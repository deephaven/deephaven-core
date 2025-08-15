//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.Comparator;

/**
 * <p>
 * An implementation of SortColumn that additionally includes a Comparator.
 * </p>
 *
 * <p>
 * This separates the API-layer which does not include the Comparator Java object from the engine layer, which requires
 * Comparators for more extensible sorting of objects.
 * </p>
 */
@Immutable
@SimpleStyle
abstract class ComparatorSortColumn implements SortColumn {
    /**
     * The column name.
     *
     * @return the column name
     */
    @Value.Parameter
    @Override
    public abstract ColumnName column();

    /**
     * The order.
     *
     * @return the order
     */
    @Value.Parameter
    @Override
    public abstract Order order();

    /**
     * Does the comparator for this column respect equality?
     *
     * <p>
     * More formally, if Comparator.compare(a, b) == 0, then Objects.equals(a, b) must be true. Certain comparators do
     * not meet this requirement. For example, {@link String#CASE_INSENSITIVE_ORDER} produces zero for "apple" and
     * "Apple", even though those values are not equal. For equality respecting comparators, the engine may use more
     * optimizations than for those comparators that do not. If respects equality is incorrectly specified as true, the
     * results of the sort are undefined.
     * </p>
     *
     * @return true if this comparator respects equality
     */
    @Value.Parameter
    public abstract boolean respectsEquality();

    @Value.Parameter
    public abstract Comparator getComparator();

    /**
     * Create a ComparatorSortColumn for the provided column and Comparator. The comparator is assumed to not respect
     * equality.
     *
     * @param sortColumn a sort column, which has a name and order
     * @param comparator the comparator
     * @return a new ComparatorSortColumn
     */
    public static ComparatorSortColumn of(SortColumn sortColumn, Comparator comparator) {
        return ImmutableComparatorSortColumn.of(sortColumn.column(), sortColumn.order(), false, comparator);
    }

    /**
     * Create a ComparatorSortColumn for the provided column and Comparator.
     *
     * @param sortColumn a sort column, which has a name and order
     * @param comparator the comparator
     * @param respectsEquality if two values compareTo zero, then they must be equal. If two distinct values compare to
     *        zero and respectsEquality is true, then results are undefined.
     * @return a new ComparatorSortColumn
     */
    public static ComparatorSortColumn of(SortColumn sortColumn, Comparator comparator, boolean respectsEquality) {
        return ImmutableComparatorSortColumn.of(sortColumn.column(), sortColumn.order(), respectsEquality, comparator);
    }

    public static ComparatorSortColumn asc(String name, Comparator comparator) {
        return asc(name, comparator, false);
    }

    public static ComparatorSortColumn desc(String name, Comparator comparator) {
        return desc(name, comparator, false);
    }

    public static ComparatorSortColumn asc(String name, Comparator comparator, boolean respectsEquality) {
        return ImmutableComparatorSortColumn.of(ColumnName.of(name), Order.ASCENDING, respectsEquality, comparator);
    }

    public static ComparatorSortColumn desc(String name, Comparator comparator, boolean respectsEquality) {
        return ImmutableComparatorSortColumn.of(ColumnName.of(name), Order.ASCENDING, respectsEquality, comparator);
    }
}
