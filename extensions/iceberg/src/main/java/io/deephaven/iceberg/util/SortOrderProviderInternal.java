//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.jetbrains.annotations.NotNull;

/**
 * Internal class containing the implementations of {@link SortOrderProvider}.
 */
class SortOrderProviderInternal {

    interface SortOrderProviderImpl {
        /**
         * Returns the {@link SortOrder} to use when writing to the given table based on this {@link SortOrderProvider}.
         */
        @NotNull
        SortOrder getSortOrder(Table table);
    }

    // Implementations of SortOrderProvider
    enum DisableSorting implements SortOrderProvider, SortOrderProviderImpl {
        INSTANCE;

        @Override
        @NotNull
        public SortOrder getSortOrder(final Table table) {
            return SortOrder.unsorted();
        }
    }

    enum TableDefaultSortOrderProvider implements SortOrderProvider, SortOrderProviderImpl {
        INSTANCE;

        @Override
        @NotNull
        public SortOrder getSortOrder(final Table table) {
            final SortOrder sortOrder = table.sortOrder();
            return sortOrder != null ? sortOrder : SortOrder.unsorted();
        }
    }

    static class IdSortOrderProvider implements SortOrderProvider, SortOrderProviderImpl {
        private final int sortOrderId;

        IdSortOrderProvider(final int sortOrderId) {
            this.sortOrderId = sortOrderId;
        }

        @Override
        @NotNull
        public SortOrder getSortOrder(final Table table) {
            if (!table.sortOrders().containsKey(sortOrderId)) {
                throw new IllegalArgumentException("Sort order with ID " + sortOrderId + " not found for table " +
                        table);
            }
            return table.sortOrders().get(sortOrderId);
        }
    }
}
