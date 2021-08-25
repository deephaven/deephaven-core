/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.db.util.Formatter;
import io.deephaven.db.v2.locations.*;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Routing {@link TableDataService} that applies a selector function to pick service(s) for each
 * request. It is assumed that each service will provide access to a non-overlapping set of table
 * locations for any table key.
 */
public class CompositeTableDataService extends AbstractTableDataService {

    private static final String IMPLEMENTATION_NAME =
        CompositeTableDataService.class.getSimpleName();

    private final ServiceSelector serviceSelector;

    public interface ServiceSelector {

        TableDataService[] call(@NotNull TableKey tableKey);

        void resetServices();

        void resetServices(@NotNull TableKey key);

        /**
         * Get a detailed description string.
         *
         * @return A description string
         * @implNote Defaults to {@link #toString()}
         */
        default String describe() {
            return toString();
        }
    }

    /**
     * @param name optional name for this service
     * @param serviceSelector Function to map a table key to a set of services that should be
     *        queried.
     */
    public CompositeTableDataService(@NotNull String name,
        @NotNull final ServiceSelector serviceSelector) {
        super(name);
        this.serviceSelector = Require.neqNull(serviceSelector, "serviceSelector");
    }

    @Override
    public void reset() {
        super.reset();
        serviceSelector.resetServices();
    }

    @Override
    public void reset(@NotNull final TableKey key) {
        super.reset(key);
        serviceSelector.resetServices(key);
    }

    @Override
    @NotNull
    protected TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
        final TableDataService[] services = serviceSelector.call(tableKey);
        if (services == null || services.length == 0) {
            throw new TableDataException(
                "No services found for " + tableKey + " in " + serviceSelector);
        }
        if (services.length == 1) {
            return services[0].getTableLocationProvider(tableKey);
        }
        return new TableLocationProviderImpl(services, tableKey);
    }

    private class TableLocationProviderImpl implements TableLocationProvider {

        private final ImmutableTableKey tableKey;

        private final List<TableLocationProvider> inputProviders;
        private final String implementationName;

        private TableLocationProviderImpl(@NotNull final TableDataService[] inputServices,
            @NotNull final TableKey tableKey) {
            this.tableKey = tableKey.makeImmutable();
            inputProviders = Arrays.stream(inputServices)
                .map(s -> s.getTableLocationProvider(this.tableKey)).collect(Collectors.toList());
            implementationName = "Composite-" + inputProviders;
        }

        @Override
        public String getImplementationName() {
            return implementationName;
        }

        @Override
        public ImmutableTableKey getKey() {
            return tableKey;
        }

        @Override
        public boolean supportsSubscriptions() {
            return inputProviders.stream().anyMatch(TableLocationProvider::supportsSubscriptions);
        }

        @Override
        public void subscribe(@NotNull final Listener listener) {
            inputProviders.forEach(p -> {
                if (p.supportsSubscriptions()) {
                    p.subscribe(listener);
                } else {
                    p.refresh();
                    p.getTableLocationKeys().forEach(listener::handleTableLocationKey);
                }
            });
        }

        @Override
        public void unsubscribe(@NotNull final Listener listener) {
            inputProviders.forEach(p -> {
                if (p.supportsSubscriptions()) {
                    p.unsubscribe(listener);
                }
            });
        }

        @Override
        public void refresh() {
            inputProviders.forEach(TableLocationProvider::refresh);
        }

        @Override
        public TableLocationProvider ensureInitialized() {
            inputProviders.forEach(TableLocationProvider::ensureInitialized);
            return this;
        }

        @Override
        @NotNull
        public Collection<ImmutableTableLocationKey> getTableLocationKeys() {
            final Set<ImmutableTableLocationKey> locationKeys =
                new KeyedObjectHashSet<>(KeyKeyDefinition.INSTANCE);
            try (final SafeCloseable ignored =
                CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
                inputProviders.stream()
                    .map(TableLocationProvider::getTableLocationKeys)
                    .flatMap(Collection::stream)
                    .filter(x -> !locationKeys.add(x))
                    .findFirst()
                    .ifPresent(duplicateLocationKey -> {
                        final String overlappingProviders = inputProviders.stream()
                            .filter(inputProvider -> inputProvider
                                .hasTableLocationKey(duplicateLocationKey))
                            .map(TableLocationProvider::getName)
                            .collect(Collectors.joining(","));
                        throw new TableDataException(
                            "Data Routing Configuration error: TableDataService elements overlap at location "
                                +
                                duplicateLocationKey +
                                " in providers " + overlappingProviders +
                                ". Full TableDataService configuration:\n" +
                                Formatter.formatTableDataService(
                                    CompositeTableDataService.this.toString()));
                    });
                return Collections.unmodifiableCollection(locationKeys);
            }
        }

        @Override
        public boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
            return inputProviders.stream()
                .anyMatch(inputProvider -> inputProvider.hasTableLocationKey(tableLocationKey));
        }

        @Override
        @Nullable
        public TableLocation getTableLocationIfPresent(
            @NotNull final TableLocationKey tableLocationKey) {
            // hang onto the first location and provider, so we can report well on any duplicates
            TableLocation location = null;
            TableLocationProvider provider = null;

            try (final SafeCloseable ignored =
                CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
                for (final TableLocationProvider tlp : inputProviders) {
                    final TableLocation candidateLocation =
                        tlp.getTableLocationIfPresent(tableLocationKey);
                    if (candidateLocation != null) {
                        if (location != null) {
                            throw new TableDataException(
                                "TableDataService elements " + provider.getName() +
                                    " and " + tlp.getName() + " overlap at location "
                                    + location.toGenericString() +
                                    ". Full TableDataService configuration:\n" +
                                    Formatter.formatTableDataService(
                                        CompositeTableDataService.this.toString()));
                        }
                        location = candidateLocation;
                        provider = tlp;
                    }
                }
            }
            return location;
        }
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public String toString() {
        return getImplementationName() + '{' +
            (getName() == null ? "" : "name=" + getName() + ", ") +
            "serviceSelector=" + serviceSelector +
            '}';
    }

    @Override
    public String describe() {
        return getImplementationName() + '{' +
            (getName() == null ? "" : "name=" + getName() + ", ") +
            "serviceSelector=" + serviceSelector.describe() +
            '}';
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Location key definition implementation
    // ------------------------------------------------------------------------------------------------------------------

    private static final class KeyKeyDefinition
        extends KeyedObjectKey.Basic<ImmutableTableLocationKey, ImmutableTableLocationKey> {

        private static final KeyedObjectKey<ImmutableTableLocationKey, ImmutableTableLocationKey> INSTANCE =
            new KeyKeyDefinition();

        private KeyKeyDefinition() {}

        @Override
        public ImmutableTableLocationKey getKey(
            @NotNull final ImmutableTableLocationKey tableLocationKey) {
            return tableLocationKey;
        }
    }
}
