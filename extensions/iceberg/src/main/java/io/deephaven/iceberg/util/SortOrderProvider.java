//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.SortOrder;

/**
 * A specification for providing {@link org.apache.iceberg.SortOrder} while writing to an iceberg table.
 */
public interface SortOrderProvider {

    // Static factory methods for creating SortOrderProvider instances

    /**
     * Do not sort the data while writing new data to the iceberg table.
     */
    static SortOrderProvider unsorted() {
        return SortOrderProviderInternal.DisableSorting.INSTANCE;
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
     * Return a sort order provider that delegates to this provider for computing the columns to sort on, but writes a
     * different sort order ID to the iceberg table. Note that the sort order returned by the caller must
     * {@link SortOrder#satisfies(SortOrder) satisfy} the sort order corresponding to the provided sort order ID.
     * <p>
     * For example, this provider might return fields {A, B, C} to sort on, but the ID written to iceberg corresponds to
     * sort order with fields {A, B}.
     *
     * @param sortOrderId the sort order ID to write to the iceberg table
     */
    default SortOrderProvider as(final int sortOrderId) {
        return new SortOrderProviderInternal.DelegatingSortOrderProvider(this, sortOrderId);
    }

    /**
     * Returns a sort order provider which will fail, if for any reason, the sort order cannot be applied to the tables
     * being written. By default, the provider will not fail if the sort order cannot be applied.
     *
     * @param failOnUnmapped whether to fail if the sort order cannot be applied to the tables being written
     */
    SortOrderProvider withFailOnUnmapped(boolean failOnUnmapped);
}
