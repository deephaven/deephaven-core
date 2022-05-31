/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.reference.WeakReferenceWrapper;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;
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
        public @NotNull Collection<ImmutableTableLocationKey> getTableLocationKeys() {
            return inputProvider.getTableLocationKeys().stream().filter(locationKeyFilter::accept)
                    .collect(Collectors.toList());
        }

        @Override
        public boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
            return locationKeyFilter.accept(tableLocationKey) && inputProvider.hasTableLocationKey(tableLocationKey);
        }

        @Nullable
        @Override
        public TableLocation getTableLocationIfPresent(@NotNull TableLocationKey tableLocationKey) {
            if (!locationKeyFilter.accept(tableLocationKey)) {
                return null;
            }
            return inputProvider.getTableLocationIfPresent(tableLocationKey);
        }

        @Override
        public String getName() {
            return FilteredTableDataService.this.getName();
        }
    }

    private class FilteringListener extends WeakReferenceWrapper<TableLocationProvider.Listener>
            implements TableLocationProvider.Listener {

        private FilteringListener(@NotNull final TableLocationProvider.Listener outputListener) {
            super(outputListener);
        }

        @Override
        public void handleTableLocationKey(@NotNull final ImmutableTableLocationKey tableLocationKey) {
            final TableLocationProvider.Listener outputListener = getWrapped();
            // We can't try to clean up null listeners here, the underlying implementation may not allow concurrent
            // unsubscribe operations.
            if (outputListener != null && locationKeyFilter.accept(tableLocationKey)) {
                outputListener.handleTableLocationKey(tableLocationKey);
            }
        }

        @Override
        public void handleException(@NotNull TableDataException exception) {
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
