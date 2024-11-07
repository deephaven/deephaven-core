//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
         * Determine whether a {@link TableLocationKey} should be visible via this service.
         *
         * @param locationKey The location key
         * @return True if the location key should be visible, false otherwise
         */
        boolean accept(@NotNull TableLocationKey locationKey);
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
        if (!locationKeyFilter.accept(tableLocationKey)) {
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
        return new TableLocationProviderImpl(serviceToFilter.getTableLocationProvider(tableKey));
    }

    private class TableLocationProviderImpl implements TableLocationProvider {

        private final TableLocationProvider inputProvider;

        private final String implementationName;
        private final Map<Listener, FilteringListener> listeners = new WeakHashMap<>();

        private TableLocationProviderImpl(@NotNull final TableLocationProvider inputProvider) {
            this.inputProvider = inputProvider;
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
            final FilteringListener filteringListener = new FilteringListener(listener);
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
            inputProvider.getTableLocationKeys(consumer, filter);
        }

        @Override
        public boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
            return locationKeyFilter.accept(tableLocationKey) && inputProvider.hasTableLocationKey(tableLocationKey);
        }

        @Nullable
        @Override
        public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
            if (!locationKeyFilter.accept(tableLocationKey)) {
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

        private FilteringListener(@NotNull final TableLocationProvider.Listener outputListener) {
            super(outputListener);
        }

        @Override
        public void handleTableLocationKeyAdded(
                @NotNull final LiveSupplier<ImmutableTableLocationKey> tableLocationKey) {
            final TableLocationProvider.Listener outputListener = getWrapped();
            // We can't try to clean up null listeners here, the underlying implementation may not allow concurrent
            // unsubscribe operations.
            if (outputListener != null && locationKeyFilter.accept(tableLocationKey.get())) {
                outputListener.handleTableLocationKeyAdded(tableLocationKey);
            }
        }

        @Override
        public void handleTableLocationKeyRemoved(
                @NotNull final LiveSupplier<ImmutableTableLocationKey> tableLocationKey) {
            final TableLocationProvider.Listener outputListener = getWrapped();
            if (outputListener != null && locationKeyFilter.accept(tableLocationKey.get())) {
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
                        .filter(key -> locationKeyFilter.accept(key.get())).collect(Collectors.toList());
                final Collection<LiveSupplier<ImmutableTableLocationKey>> filteredRemovedKeys = removedKeys.stream()
                        .filter(key -> locationKeyFilter.accept(key.get())).collect(Collectors.toList());

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
