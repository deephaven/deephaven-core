//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

/**
 * A specification for providing {@link org.apache.iceberg.SortOrder} while writing to an iceberg table.
 */
public interface SortOrderProvider {

    // Static factory methods for creating SortOrderProvider instances

    /**
     * Do not sort the data while writing new data to the iceberg table.
     */
    static SortOrderProvider disableSorting() {
        return SortOrderProviderInternal.DisableSorting.getInstance();
    }

    /**
     * Use the default {@link org.apache.iceberg.Table#sortOrder()} of the table while writing new data.
     */
    static SortOrderProvider useTableDefault() {
        return SortOrderProviderInternal.TableDefaultSortOrderProvider.getInstance();
    }

    /**
     * Use the sort order with the given ID to sort new data while writing to the iceberg table.
     */
    static SortOrderProvider fromSortId(final int id) {
        return new SortOrderProviderInternal.IdSortOrderProvider(id);
    }
}
