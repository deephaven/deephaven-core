//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.reference.WeakReferenceWrapper;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@link TableDataService} implementation with support to filter the provided {@link TableLocation}s.
 */
public class FilteredTableDataService extends AbstractTableDataService {

    private static final String IMPLEMENTATION_NAME = FilteredTableDataService.class.getSimpleName();

    private final TableDataService serviceToFilter;
    private final LocationKeyFilterProvider locationKeyFilterProvider;

    /**
     * A filter that has been bound to a table, and so can decide that table's locations.
     */
    @FunctionalInterface
    public interface LocationKeyFilter {

        /** Accepts every location of the table. */
        LocationKeyFilter ALL = locationKey -> true;

        /**
         * Accepts no location of the table. A provider that returns this is saying the table is entirely excluded,
         * which lets the caller skip the filtered service for it.
         */
        LocationKeyFilter NONE = locationKey -> false;

        /**
         * Determine whether one location of the bound table should be visible via this service.
         *
         * @param locationKey The location key, whose partition values are meaningful only within the bound table
         * @return True if the location key should be visible, false otherwise
         */
        boolean accept(@NotNull TableLocationKey locationKey);
    }

    /**
     * Supplies the {@link LocationKeyFilter} for a table.
     * <p>
     * A filter that does not discriminate on the table is a provider that ignores its argument.
     */
    @FunctionalInterface
    public interface LocationKeyFilterProvider {

        /**
         * Produce the filter that decides the locations of {@code tableKey}.
         * <p>
         * An implementation that accepts no location of {@code tableKey} <em>must</em> return the
         * {@link LocationKeyFilter#NONE} instance itself, and one that accepts every location <em>should</em> return
         * {@link LocationKeyFilter#ALL}. The sentinels are recognized by reference identity, so an equivalent lambda is
         * not a substitute.
         * <p>
         * This may be called more than once for the same table, so it must be a deterministic function of its argument:
         * equal table keys must yield filters that accept the same locations.
         *
         * @param tableKey The table to filter the locations of
         * @return The filter for locations of {@code tableKey}; {@link LocationKeyFilter#NONE} if no location of that
         *         table can be accepted, which lets the caller avoid consulting the filtered service at all
         */
        @NotNull
        LocationKeyFilter forTable(@NotNull TableKey tableKey);
    }

    /**
     * @param serviceToFilter The service that's being filtered
     * @param locationKeyFilterProvider Supplies the filter for each table's locations
     */
    public FilteredTableDataService(@NotNull final TableDataService serviceToFilter,
            @NotNull final LocationKeyFilterProvider locationKeyFilterProvider) {
        super("Filtered-" + Require.neqNull(serviceToFilter, "serviceToFilter").getName());
        this.serviceToFilter = Require.neqNull(serviceToFilter, "serviceToFilter");
        this.locationKeyFilterProvider =
                Require.neqNull(locationKeyFilterProvider, "locationKeyFilterProvider");
    }

    @Override
    @Nullable
    public TableLocationProvider getRawTableLocationProvider(@NotNull final TableKey tableKey,
            @NotNull final TableLocationKey tableLocationKey) {
        if (!locationKeyFilterProvider.forTable(tableKey).accept(tableLocationKey)) {
            return null;
        }

        return serviceToFilter.getRawTableLocationProvider(tableKey, tableLocationKey);
    }

    @Override
    public void reset() {
        super.reset();
        serviceToFilter.reset();
    }

    @Override
    public void reset(@NotNull final TableKey key) {
        super.reset(key);
        serviceToFilter.reset(key);
    }

    @Override
    @NotNull
    protected TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
        final LocationKeyFilter filterForTable = locationKeyFilterProvider.forTable(tableKey);
        if (filterForTable == LocationKeyFilter.NONE) {
            // No location of this table can be accepted, so don't consult the filtered service at all. That service is
            // frequently remote, and consulting it would open a subscription whose every result would be discarded.
            return new NullTableLocationProvider(tableKey);
        }
        return new TableLocationProviderImpl(serviceToFilter.getTableLocationProvider(tableKey), filterForTable);
    }

    private class TableLocationProviderImpl implements TableLocationProvider {

        private final TableLocationProvider inputProvider;

        /** The service's filter, bound to this provider's table. */
        private final LocationKeyFilter filterForTable;

        private final String implementationName;
        private final Map<Listener, FilteringListener> listeners = new WeakHashMap<>();

        private TableLocationProviderImpl(@NotNull final TableLocationProvider inputProvider,
                @NotNull final LocationKeyFilter filterForTable) {
            this.inputProvider = inputProvider;
            this.filterForTable = filterForTable;
            implementationName = "Filtered-" + inputProvider.getImplementationName();
        }

        @Override
        public String getImplementationName() {
            return implementationName;
        }

        @Override
        public ImmutableTableKey getKey() {
            return inputProvider.getKey();
        }

        @Override
        public boolean supportsSubscriptions() {
            return inputProvider.supportsSubscriptions();
        }

        @Override
        public void subscribe(@NotNull final Listener listener) {
            final FilteringListener filteringListener = new FilteringListener(filterForTable, listener);
            synchronized (listeners) {
                listeners.put(listener, filteringListener);
            }
            inputProvider.subscribe(filteringListener);
        }

        @Override
        public void unsubscribe(@NotNull final Listener listener) {
            final FilteringListener filteringListener;
            synchronized (listeners) {
                filteringListener = listeners.remove(listener);
            }
            if (filteringListener != null) {
                inputProvider.unsubscribe(filteringListener);
            }
        }

        @Override
        public void refresh() {
            inputProvider.refresh();
        }

        @Override
        public TableLocationProvider ensureInitialized() {
            inputProvider.ensureInitialized();
            return this;
        }

        @Override
        public void getTableLocationKeys(
                final Consumer<LiveSupplier<ImmutableTableLocationKey>> consumer,
                final Predicate<ImmutableTableLocationKey> filter) {
            // Apply this table's bound filter alongside the caller's, so that enumeration exposes the same
            // set as hasTableLocationKey, getTableLocationIfPresent, and subscription delivery.
            inputProvider.getTableLocationKeys(consumer, filter.and(filterForTable::accept));
        }

        @Override
        public boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
            return filterForTable.accept(tableLocationKey) && inputProvider.hasTableLocationKey(tableLocationKey);
        }

        @Nullable
        @Override
        public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
            if (!filterForTable.accept(tableLocationKey)) {
                return null;
            }
            return inputProvider.getTableLocationIfPresent(tableLocationKey);
        }

        @Override
        public String getName() {
            return FilteredTableDataService.this.getName();
        }

        @Override
        @NotNull
        public TableUpdateMode getUpdateMode() {
            return inputProvider.getUpdateMode();
        }

        @Override
        @NotNull
        public TableUpdateMode getLocationUpdateMode() {
            return inputProvider.getLocationUpdateMode();
        }
    }

    private class FilteringListener extends WeakReferenceWrapper<TableLocationProvider.Listener>
            implements TableLocationProvider.Listener {

        /** The service's filter, bound to the table of the provider this listener was subscribed to. */
        private final LocationKeyFilter filterForTable;

        private FilteringListener(@NotNull final LocationKeyFilter filterForTable,
                @NotNull final TableLocationProvider.Listener outputListener) {
            super(outputListener);
            this.filterForTable = filterForTable;
        }

        @Override
        public void handleTableLocationKeyAdded(
                @NotNull final LiveSupplier<ImmutableTableLocationKey> tableLocationKey) {
            final TableLocationProvider.Listener outputListener = getWrapped();
            // We can't try to clean up null listeners here, the underlying implementation may not allow concurrent
            // unsubscribe operations.
            if (outputListener != null && filterForTable.accept(tableLocationKey.get())) {
                outputListener.handleTableLocationKeyAdded(tableLocationKey);
            }
        }

        @Override
        public void handleTableLocationKeyRemoved(
                @NotNull final LiveSupplier<ImmutableTableLocationKey> tableLocationKey) {
            final TableLocationProvider.Listener outputListener = getWrapped();
            if (outputListener != null && filterForTable.accept(tableLocationKey.get())) {
                outputListener.handleTableLocationKeyRemoved(tableLocationKey);
            }
        }

        @Override
        public void handleTableLocationKeysUpdate(
                @NotNull final Collection<LiveSupplier<ImmutableTableLocationKey>> addedKeys,
                @NotNull final Collection<LiveSupplier<ImmutableTableLocationKey>> removedKeys) {
            // NOTE: We are filtering the added and removed keys for every listener. We should consider refactoring to
            // filter once and then notify all listeners with the filtered lists (similar to SubscriptionAggregator).
            final TableLocationProvider.Listener outputListener = getWrapped();
            if (outputListener != null) {
                // Produce filtered lists of added and removed keys.
                final Collection<LiveSupplier<ImmutableTableLocationKey>> filteredAddedKeys = addedKeys.stream()
                        .filter(key -> filterForTable.accept(key.get())).collect(Collectors.toList());
                final Collection<LiveSupplier<ImmutableTableLocationKey>> filteredRemovedKeys = removedKeys.stream()
                        .filter(key -> filterForTable.accept(key.get())).collect(Collectors.toList());

                if (filteredAddedKeys.isEmpty() && filteredRemovedKeys.isEmpty()) {
                    return;
                }
                outputListener.handleTableLocationKeysUpdate(filteredAddedKeys, filteredRemovedKeys);
            }
        }

        @Override
        public void handleException(@NotNull final TableDataException exception) {
            final TableLocationProvider.Listener outputListener = getWrapped();
            // See note in handleTableLocationKey.
            if (outputListener != null) {
                outputListener.handleException(exception);
            }
        }

        @Override
        public String toString() {
            return "FilteringListener{" + FilteredTableDataService.this + "}";
        }
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public String toString() {
        return getImplementationName() + '{' +
                (getName() != null ? "name=" + getName() + ", " : "") +
                "locationKeyFilterProvider=" + locationKeyFilterProvider +
                ", serviceToFilter=" + serviceToFilter +
                '}';
    }

    @Override
    public String describe() {
        return getImplementationName() + '{' +
                (getName() != null ? "name=" + getName() + ", " : "") +
                "locationKeyFilterProvider=" + locationKeyFilterProvider +
                ", serviceToFilter=" + serviceToFilter.describe() +
                '}';
    }
}
