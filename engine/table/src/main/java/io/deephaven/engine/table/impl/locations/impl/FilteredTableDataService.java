//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.reference.WeakReferenceWrapper;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.table.impl.locations.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

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
        public UPDATE_TYPE getUpdateMode() {
            // Assuming a dynamic filter, locations could be added or removed at any time from the perspective of
            // this filtered provider.
            return UPDATE_TYPE.REFRESHING;
        }
    }

    private class FilteringListener extends WeakReferenceWrapper<TableLocationProvider.Listener>
            implements TableLocationProvider.Listener {

        private FilteringListener(@NotNull final TableLocationProvider.Listener outputListener) {
            super(outputListener);
        }

        @Override
        public void beginTransaction(@NotNull final Object token) {
            // Delegate to the wrapped listener.
            final TableLocationProvider.Listener outputListener = getWrapped();
            if (outputListener != null) {
                outputListener.beginTransaction(token);
            }
        }

        @Override
        public void endTransaction(@NotNull final Object token) {
            // Delegate to the wrapped listener.
            final TableLocationProvider.Listener outputListener = getWrapped();
            if (outputListener != null) {
                outputListener.endTransaction(token);
            }
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
                @NotNull Collection<LiveSupplier<ImmutableTableLocationKey>> addedKeys,
                @NotNull Collection<LiveSupplier<ImmutableTableLocationKey>> removedKeys) {
            final TableLocationProvider.Listener outputListener = getWrapped();
            if (outputListener != null) {
                outputListener.handleTableLocationKeysUpdate(addedKeys, removedKeys);
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
