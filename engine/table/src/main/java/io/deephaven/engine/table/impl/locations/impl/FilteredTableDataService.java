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
    private final LocationKeyFilter locationKeyFilter;

    @FunctionalInterface
    public interface LocationKeyFilter {

        /**
         * Accepts every location of every table.
         */
        LocationKeyFilter ALL = locationKey -> true;

        /**
         * Accepts no location of any table. Returning this from {@link #forTable(TableKey)} lets the caller skip the
         * filtered service entirely for that table.
         */
        LocationKeyFilter NONE = locationKey -> false;

        /**
         * Determine whether a {@link TableLocationKey} should be visible via this service.
         *
         * <p>
         * A {@link TableLocationKey} holds partition values and no table identity, so it identifies a location only
         * relative to its table. This method must therefore be asked only of a filter that has already been bound to a
         * table with {@link #forTable(TableKey)}. A table-blind filter is its own binding, so for such a filter the
         * distinction does not arise.
         *
         * @param locationKey The location key
         * @return True if the location key should be visible, false otherwise
         */
        boolean accept(@NotNull TableLocationKey locationKey);

        /**
         * Bind this filter to a table, returning the filter that applies to that table's locations.
         *
         * <p>
         * {@link FilteredTableDataService} binds once, when it builds the {@link TableLocationProvider} for a table,
         * and asks only the bound filter about locations. A filter whose decision depends on both the table and the
         * location - one that does not factor into an independent table test and location test - overrides this to
         * capture the table. A filter that does not discriminate on the table inherits the default and binds to itself.
         *
         * @param tableKey The table to bind to
         * @return The filter for locations of {@code tableKey}; {@link #NONE} if no location of that table can be
         *         accepted, which lets the caller avoid consulting the filtered service at all
         */
        @NotNull
        default LocationKeyFilter forTable(@NotNull TableKey tableKey) {
            return this;
        }
    }

    /**
     * @param serviceToFilter The service that's being filtered
     * @param locationKeyFilter The filter function
     */
    public FilteredTableDataService(@NotNull final TableDataService serviceToFilter,
            @NotNull final LocationKeyFilter locationKeyFilter) {
        super("Filtered-" + Require.neqNull(serviceToFilter, "serviceToFilter").getName());
        this.serviceToFilter = Require.neqNull(serviceToFilter, "serviceToFilter");
        this.locationKeyFilter = Require.neqNull(locationKeyFilter, "locationKeyFilter");
    }

    @Override
    @Nullable
    public TableLocationProvider getRawTableLocationProvider(@NotNull final TableKey tableKey,
            @NotNull final TableLocationKey tableLocationKey) {
        if (!locationKeyFilter.forTable(tableKey).accept(tableLocationKey)) {
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
        final LocationKeyFilter filterForTable = locationKeyFilter.forTable(tableKey);
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
            // Apply this service's locationKeyFilter alongside the caller's, so that enumeration exposes the same
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
                "locationKeyFilter=" + locationKeyFilter +
                ", serviceToFilter=" + serviceToFilter +
                '}';
    }

    @Override
    public String describe() {
        return getImplementationName() + '{' +
                (getName() != null ? "name=" + getName() + ", " : "") +
                "locationKeyFilter=" + locationKeyFilter +
                ", serviceToFilter=" + serviceToFilter.describe() +
                '}';
    }
}
