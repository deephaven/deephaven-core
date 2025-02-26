//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;

/**
 * Internal class containing the implementations of {@link SortOrderProvider}.
 */
class SortOrderProviderInternal {

    interface SortOrderProviderImpl {
        /**
         * Returns the {@link SortOrder} to use when writing to the given table based on this {@link SortOrderProvider}.
         */
        SortOrder getSortOrder(Table table);
    }

    // Implementations of SortOrderProvider
    static class DisableSorting implements SortOrderProvider, SortOrderProviderImpl {

        private static final DisableSorting INSTANCE = new DisableSorting();

        private DisableSorting() {}

        static DisableSorting getInstance() {
            return INSTANCE;
        }

        @Override
        public SortOrder getSortOrder(final Table table) {
            return SortOrder.unsorted();
        }
    }

    static class TableDefaultSortOrderProvider implements SortOrderProvider, SortOrderProviderImpl {

        private static final TableDefaultSortOrderProvider INSTANCE = new TableDefaultSortOrderProvider();

        private TableDefaultSortOrderProvider() {}

        static TableDefaultSortOrderProvider getInstance() {
            return INSTANCE;
        }

        @Override
        public SortOrder getSortOrder(final Table table) {
            return table.sortOrder();
        }
    }

    static class IdSortOrderProvider implements SortOrderProvider, SortOrderProviderImpl {
        private final int sortOrderId;

        IdSortOrderProvider(final int sortOrderId) {
            this.sortOrderId = sortOrderId;
        }

        @Override
        public SortOrder getSortOrder(final Table table) {
            if (!table.sortOrders().containsKey(sortOrderId)) {
                throw new IllegalArgumentException("Sort order with ID " + sortOrderId + " not found for table " +
                        table);
            }
            return table.sortOrders().get(sortOrderId);
        }
    }
}
