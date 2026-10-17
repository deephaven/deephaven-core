//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.SortOrder;

/**
 * A specification for providing {@link org.apache.iceberg.SortOrder} while writing to an iceberg table.
 * <p>
 * Deephaven sorts new data by the provided sort order and records only the sort's leading column, in its own metadata
 * in each data file. It does not declare the sort order in the Iceberg metadata, since it sorts with its own
 * comparisons, which differ from the Iceberg specification's for strings and floating point values.
 */
public interface SortOrderProvider {

    // Static factory methods for creating SortOrderProvider instances

    /**
     * Do not sort the data while writing new data to the iceberg table.
     */
    static SortOrderProvider unsorted() {
        return SortOrderProviderInternal.DisableSorting.DISABLE_SORTING;
    }

    /**
     * Use the default {@link org.apache.iceberg.Table#sortOrder()} of the table while writing new data. If no sort
     * order is set on the table, no sorting will be done.
     */
    static SortOrderProvider useTableDefault() {
        return new SortOrderProviderInternal.TableDefaultSortOrderProvider();
    }

    /**
     * Use the sort order with the given ID to sort new data while writing to the iceberg table.
     */
    static SortOrderProvider fromSortId(final int id) {
        return new SortOrderProviderInternal.IdSortOrderProvider(id);
    }

    /**
     * Use the given sort order directly to sort new data while writing to the iceberg table. Note that the provided
     * sort order must either have a valid {@link SortOrder#orderId()}, else this provider should be chained with an
     * {@link #withId(int)} call to set a valid order ID.
     */
    static SortOrderProvider fromSortOrder(final SortOrder sortOrder) {
        return new SortOrderProviderInternal.DirectSortOrderProvider(sortOrder);
    }

    /**
     * Returns a sort order provider that uses the current provider to determine the columns to sort on, but with a
     * different sort order ID. Note that the sort order returned by the caller must
     * {@link SortOrder#satisfies(SortOrder) satisfy} the sort order corresponding to the provided sort order ID.
     * <p>
     * For example, this provider might sort by columns {A, B, C}, but the ID corresponds to a sort order with columns
     * {A, B}. Since the sort order is not declared in the Iceberg metadata, the ID is checked but not written.
     *
     * @param sortOrderId the sort order ID
     */
    SortOrderProvider withId(final int sortOrderId);

    /**
     * Returns a sort order provider configured to fail (or not) if the sort order cannot be applied to the tables being
     * written. By default, all providers fail if the sort order cannot be applied.
     *
     * @param failOnUnmapped whether to fail if the sort order cannot be applied to the tables being written. If
     *        {@code false} and the sort order cannot be applied, the tables will be written without sorting.
     */
    SortOrderProvider withFailOnUnmapped(boolean failOnUnmapped);
}
