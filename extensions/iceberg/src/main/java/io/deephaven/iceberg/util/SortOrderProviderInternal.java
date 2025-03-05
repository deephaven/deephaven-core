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
         * Returns the {@link SortOrder} to use for sorting the data when writing to the provided iceberg table.
         */
        @NotNull
        SortOrder getSortOrderToUse(Table table);

        /**
         * Returns the sort order to write down to the iceberg table when appending new data. This may be different from
         * the {@code getSortOrderToUse(table)} as this sort order may be customized. But
         * {@link #getSortOrderToUse(Table)} should always {@link SortOrder#satisfies(SortOrder) satisfy} this sort
         * order.
         */
        @NotNull
        default SortOrder getSortOrderToWrite(final Table table) {
            return getSortOrderToUse(table);
        }

        /**
         * Whether to fail if the sort order cannot be applied to the tables being written.
         */
        boolean failOnUnmapped();
    }

    // Implementations of SortOrderProvider
    enum DisableSorting implements SortOrderProviderImpl, SortOrderProvider {
        INSTANCE;

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(final Table table) {
            return SortOrder.unsorted();
        }

        @Override
        @NotNull
        public SortOrderProvider withFailOnUnmapped(final boolean failOnUnmapped) {
            throw new UnsupportedOperationException("Cannot set failOnUnmapped for unsorted sort order provider");
        }

        @Override
        public boolean failOnUnmapped() {
            return true; // Should never fail as we are unsorted
        }
    }

    static class TableDefaultSortOrderProvider implements SortOrderProvider, SortOrderProviderImpl {
        private boolean failOnUnmapped;

        TableDefaultSortOrderProvider() {
            failOnUnmapped = false;
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(final Table table) {
            final SortOrder sortOrder = table.sortOrder();
            return sortOrder != null ? sortOrder : SortOrder.unsorted();
        }

        @Override
        public SortOrderProvider withFailOnUnmapped(final boolean failOnUnmapped) {
            this.failOnUnmapped = failOnUnmapped;
            return this;
        }

        @Override
        public boolean failOnUnmapped() {
            return failOnUnmapped;
        }
    }

    static class IdSortOrderProvider implements SortOrderProvider, SortOrderProviderImpl {
        private boolean failOnUnmapped;
        private final int sortOrderId;

        IdSortOrderProvider(final int sortOrderId) {
            super();
            this.sortOrderId = sortOrderId;
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(final Table table) {
            return getSortOrderForId(table, sortOrderId);
        }

        @Override
        public SortOrderProvider withFailOnUnmapped(final boolean failOnUnmapped) {
            this.failOnUnmapped = failOnUnmapped;
            return this;
        }

        @Override
        public boolean failOnUnmapped() {
            return failOnUnmapped;
        }
    }

    /**
     * A {@link SortOrderProvider} that delegates to another {@link SortOrderProvider} for computing the sort order,
     * while providing a custom sort order ID.
     */
    static class DelegatingSortOrderProvider implements SortOrderProvider, SortOrderProviderImpl {
        private SortOrderProvider sortOrderProvider;
        private final int sortOrderId;

        DelegatingSortOrderProvider(final SortOrderProvider sortOrderProvider, final int sortOrderId) {
            this.sortOrderProvider = sortOrderProvider;
            this.sortOrderId = sortOrderId;
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(final Table table) {
            return ((SortOrderProviderImpl) sortOrderProvider).getSortOrderToUse(table);
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToWrite(final Table table) {
            final SortOrder sortOrderFromDelegate = getSortOrderToUse(table);
            final SortOrder sortOrderForId = getSortOrderForId(table, sortOrderId);
            if (!sortOrderFromDelegate.satisfies(sortOrderForId)) {
                throw new IllegalArgumentException("Sort order with ID " + sortOrderId + " does not satisfy the " +
                        "table's sort order: " + sortOrderFromDelegate);
            }
            return sortOrderForId;
        }

        @Override
        @NotNull
        public SortOrderProvider as(final int sortOrderId) {
            return new DelegatingSortOrderProvider(sortOrderProvider, sortOrderId);
        }

        @Override
        public SortOrderProvider withFailOnUnmapped(final boolean failOnUnmapped) {
            sortOrderProvider = sortOrderProvider.withFailOnUnmapped(failOnUnmapped);
            return this;
        }

        @Override
        public boolean failOnUnmapped() {
            return ((SortOrderProviderImpl) sortOrderProvider).failOnUnmapped();
        }
    }

    // --------------------------------------------------------------------------------------------------

    // Methods for extracting the sort order from the table
    private static SortOrder getSortOrderForId(final Table table, final int sortOrderId) {
        if (!table.sortOrders().containsKey(sortOrderId)) {
            throw new IllegalArgumentException("Sort order with ID " + sortOrderId + " not found for table " +
                    table);
        }
        return table.sortOrders().get(sortOrderId);
    }
}
