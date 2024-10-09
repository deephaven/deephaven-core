//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.util.Formatter;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Routing {@link TableDataService} that applies a selector function to pick service(s) for each request. It is assumed
 * that each service will provide access to a non-overlapping set of table locations for any table key.
 */
public class CompositeTableDataService extends AbstractTableDataService {

    private static final String IMPLEMENTATION_NAME = CompositeTableDataService.class.getSimpleName();

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
     * @param serviceSelector Function to map a table key to a set of services that should be queried.
     */
    public CompositeTableDataService(@NotNull String name, @NotNull final ServiceSelector serviceSelector) {
        super(name);
        this.serviceSelector = Require.neqNull(serviceSelector, "serviceSelector");
    }

    @Override
    @Nullable
    public TableLocationProvider getRawTableLocationProvider(@NotNull final TableKey tableKey,
            @NotNull final TableLocationKey tableLocationKey) {
        final TableDataService[] services = serviceSelector.call(tableKey);

        if (services == null || services.length == 0) {
            return null;
        }

        TableLocationProvider tlp = null;
        for (final TableDataService service : services) {
            final TableLocationProvider tlpCandidate = service.getRawTableLocationProvider(tableKey, tableLocationKey);
            if (tlpCandidate == null) {
                continue;
            }

            if (tlp != null) {
                throw new TableDataException(
                        "TableDataService elements " + tlpCandidate.getName() + " and " + tlp.getName()
                                + " both contain " + tableLocationKey + ". Full TableDataService configuration:\n"
                                + Formatter.formatTableDataService(CompositeTableDataService.this.toString()));
            }

            tlp = tlpCandidate;
        }

        return tlp;
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
            throw new TableDataException("No services found for " + tableKey + " in " + serviceSelector);
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

        // What guarantees about added/removed locations can be made?
        private final TableUpdateMode updateMode;
        // What guarantees about added/removed rows within locations can be made?
        private final TableUpdateMode locationUpdateMode;

        private TableLocationProviderImpl(@NotNull final TableDataService[] inputServices,
                @NotNull final TableKey tableKey) {
            this.tableKey = tableKey.makeImmutable();
            inputProviders = Arrays.stream(inputServices).map(s -> s.getTableLocationProvider(this.tableKey))
                    .collect(Collectors.toList());
            implementationName = "Composite-" + inputProviders;

            // Analyze the update modes of the input providers to determine the update mode of the composite provider.
            // The resultant mode is the most permissive mode of the input providers, with the exception that we will
            // never return APPEND_ONLY.
            final TableUpdateMode tmpUpdateMode = TableUpdateMode.mostPermissiveMode(
                    inputProviders.stream().map(TableLocationProvider::getUpdateMode));
            updateMode = tmpUpdateMode == TableUpdateMode.APPEND_ONLY ? TableUpdateMode.ADD_ONLY : tmpUpdateMode;

            // Analyze the location update modes of the input providers to determine the location update mode
            // of the composite provider. The resultant mode is the most permissive mode of the input provider
            // locations.
            locationUpdateMode = TableUpdateMode.mostPermissiveMode(
                    inputProviders.stream().map(TableLocationProvider::getLocationUpdateMode));
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
                    p.getTableLocationKeys(listener::handleTableLocationKeyAdded);
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
        public void getTableLocationKeys(
                final Consumer<LiveSupplier<ImmutableTableLocationKey>> consumer,
                final Predicate<ImmutableTableLocationKey> filter) {
            final Set<LiveSupplier<ImmutableTableLocationKey>> locationKeys =
                    new KeyedObjectHashSet<>(KeyKeyDefinition.INSTANCE);
            try (final SafeCloseable ignored = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
                // Add all the location keys from the providers to the set, throw an exception if there are duplicates
                inputProviders.forEach(p -> p.getTableLocationKeys(tlk -> {
                    if (locationKeys.add(tlk)) {
                        // Consume the key immediately (while the key is still managed by the input provider)
                        consumer.accept(tlk);
                        return;
                    }
                    // We have a duplicate key, throw a detailed exception
                    final String overlappingProviders = inputProviders.stream()
                            .filter(inputProvider -> inputProvider.hasTableLocationKey(tlk.get()))
                            .map(TableLocationProvider::getName)
                            .collect(Collectors.joining(","));
                    throw new TableDataException(
                            "Data Routing Configuration error: TableDataService elements overlap at location " +
                                    tlk +
                                    " in providers " + overlappingProviders +
                                    ". Full TableDataService configuration:\n" +
                                    Formatter
                                            .formatTableDataService(CompositeTableDataService.this.toString()));

                }, filter));
            }
        }

        @Override
        public boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
            return inputProviders.stream()
                    .anyMatch(inputProvider -> inputProvider.hasTableLocationKey(tableLocationKey));
        }

        @Override
        @Nullable
        public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
            // hang onto the first location and provider, so we can report well on any duplicates
            TableLocation location = null;
            TableLocationProvider provider = null;

            try (final SafeCloseable ignored = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
                for (final TableLocationProvider tlp : inputProviders) {
                    final TableLocation candidateLocation = tlp.getTableLocationIfPresent(tableLocationKey);
                    if (candidateLocation != null) {
                        if (location != null) {
                            throw new TableDataException("TableDataService elements " + provider.getName() +
                                    " and " + tlp.getName() + " overlap at location " + location.toGenericString() +
                                    ". Full TableDataService configuration:\n" +
                                    Formatter.formatTableDataService(CompositeTableDataService.this.toString()));
                        }
                        location = candidateLocation;
                        provider = tlp;
                    }
                }
            }
            return location;
        }

        @Override
        @NotNull
        public TableUpdateMode getUpdateMode() {
            return updateMode;
        }

        @Override
        @NotNull
        public TableUpdateMode getLocationUpdateMode() {
            return locationUpdateMode;
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
            extends KeyedObjectKey.Basic<ImmutableTableLocationKey, LiveSupplier<ImmutableTableLocationKey>> {

        private static final KeyedObjectKey<ImmutableTableLocationKey, LiveSupplier<ImmutableTableLocationKey>> INSTANCE =
                new KeyKeyDefinition();

        private KeyKeyDefinition() {}

        @Override
        public ImmutableTableLocationKey getKey(
                @NotNull final LiveSupplier<ImmutableTableLocationKey> tableLocationKey) {
            return tableLocationKey.get();
        }
    }
}
