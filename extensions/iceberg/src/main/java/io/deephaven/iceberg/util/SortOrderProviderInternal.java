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

    interface SortOrderProviderImpl extends SortOrderProvider {
        /**
         * Returns the {@link SortOrder} to use for sorting the data when writing to the provided iceberg table.
         */
        @NotNull
        SortOrder getSortOrderToUse(@NotNull Table table);

        /**
         * Returns the sort order to write down to the iceberg table when appending new data. This may be different from
         * the {@code getSortOrderToUse(table)} as this sort order may be customized. But
         * {@link #getSortOrderToUse(Table)} should always {@link SortOrder#satisfies(SortOrder) satisfy} this sort
         * order.
         */
        @NotNull
        default SortOrder getSortOrderToWrite(@NotNull final Table table) {
            return getSortOrderToUse(table);
        }

        /**
         * Whether to fail if the sort order cannot be applied to the tables being written.
         */
        boolean failOnUnmapped();

        @NotNull
        default SortOrderProvider withId(final int sortOrderId) {
            return new SortOrderProviderInternal.DelegatingSortOrderProvider(this, sortOrderId);
        }
    }

    // Implementations of SortOrderProvider
    enum DisableSorting implements SortOrderProviderImpl {
        DISABLE_SORTING;

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(@NotNull final Table table) {
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

    static class TableDefaultSortOrderProvider extends CanFailOnUnmapped implements SortOrderProviderImpl {

        TableDefaultSortOrderProvider() {}

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(@NotNull final Table table) {
            final SortOrder sortOrder = table.sortOrder();
            return sortOrder != null ? sortOrder : SortOrder.unsorted();
        }
    }

    static class IdSortOrderProvider extends CanFailOnUnmapped implements SortOrderProviderImpl {
        private final int sortOrderId;

        IdSortOrderProvider(final int sortOrderId) {
            this.sortOrderId = sortOrderId;
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(@NotNull final Table table) {
            return getSortOrderForId(table, sortOrderId);
        }
    }

    static class DirectSortOrderProvider extends CanFailOnUnmapped implements SortOrderProviderImpl {
        private final SortOrder sortOrder;

        DirectSortOrderProvider(@NotNull final SortOrder sortOrder) {
            this.sortOrder = sortOrder;
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(@NotNull final Table table) {
            return sortOrder;
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToWrite(@NotNull final Table table) {
            // Check if provided sort order is included in the table's sort orders
            if (!sortOrder.equals(getSortOrderForId(table, sortOrder.orderId()))) {
                throw new IllegalArgumentException("Provided sort order with id " + sortOrder.orderId() + " is not " +
                        "included in the table's sort orders");
            }
            return sortOrder;
        }
    }

    /**
     * A {@link SortOrderProvider} that delegates to another {@link SortOrderProvider} for computing the sort order,
     * while providing a custom sort order ID.
     */
    private static class DelegatingSortOrderProvider implements SortOrderProviderImpl {
        private SortOrderProvider delegateProvider;
        private final int sortOrderId;

        DelegatingSortOrderProvider(final SortOrderProvider sortOrderProvider, final int sortOrderId) {
            this.delegateProvider = sortOrderProvider;
            this.sortOrderId = sortOrderId;
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToUse(final @NotNull Table table) {
            return ((SortOrderProviderImpl) delegateProvider).getSortOrderToUse(table);
        }

        @Override
        @NotNull
        public SortOrder getSortOrderToWrite(final @NotNull Table table) {
            final SortOrder sortOrderFromDelegate = getSortOrderToUse(table);
            final SortOrder sortOrderForId = getSortOrderForId(table, sortOrderId);
            if (!sortOrderFromDelegate.satisfies(sortOrderForId)) {
                throw new IllegalArgumentException(
                        "Provided sort order " + sortOrderFromDelegate + " does not satisfy the table's sort order " +
                                "with id " + sortOrderId + ": " + sortOrderForId);
            }
            return sortOrderForId;
        }

        @Override
        @NotNull
        public SortOrderProvider withId(final int sortOrderId) {
            return new DelegatingSortOrderProvider(delegateProvider, sortOrderId);
        }

        @Override
        public SortOrderProvider withFailOnUnmapped(final boolean failOnUnmapped) {
            delegateProvider = delegateProvider.withFailOnUnmapped(failOnUnmapped);
            return this;
        }

        @Override
        public boolean failOnUnmapped() {
            return ((SortOrderProviderImpl) delegateProvider).failOnUnmapped();
        }
    }

    // --------------------------------------------------------------------------------------------------

    private static SortOrder getSortOrderForId(final Table table, final int sortOrderId) {
        if (!table.sortOrders().containsKey(sortOrderId)) {
            throw new IllegalArgumentException("Sort order with ID " + sortOrderId + " not found for table " +
                    table);
        }
        return table.sortOrders().get(sortOrderId);
    }

    private static abstract class CanFailOnUnmapped implements SortOrderProviderImpl {
        private boolean failOnUnmapped;

        CanFailOnUnmapped() {
            this.failOnUnmapped = true;
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
}
