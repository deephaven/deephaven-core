//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.table.impl.DummyTableLocation;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableDataService;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestTableDataService {
    @Test
    public void testGetRawTableDataService() {
        final Map<String, Comparable<?>> partitions1 = new HashMap<>();
        partitions1.put("Part", "A");
        final Map<String, Comparable<?>> partitions2 = new HashMap<>();
        partitions2.put("Part", "B");
        final Map<String, Comparable<?>> partitions3 = new HashMap<>();
        partitions3.put("Part", "C");

        TableLocationKey tlk1 = new SimpleTableLocationKey(partitions1);
        TableLocationKey tlk2 = new SimpleTableLocationKey(partitions2);
        TableLocationKey tlk3 = new SimpleTableLocationKey(partitions3);

        // No table location overlap
        final CompositeTableDataService ctds1 =
                new CompositeTableDataService("ctds1", new DummyServiceSelector(tlk1, tlk2));
        Assert.assertNotNull(ctds1.getRawTableLocationProvider(StandaloneTableKey.getInstance(), tlk1));
        Assert.assertNull(ctds1.getRawTableLocationProvider(StandaloneTableKey.getInstance(), tlk3));

        // Table location overlap
        final CompositeTableDataService ctds2 =
                new CompositeTableDataService("ctds2", new DummyServiceSelector(tlk1, tlk1));
        Assert.assertThrows(TableDataException.class,
                () -> ctds2.getRawTableLocationProvider(StandaloneTableKey.getInstance(), tlk1));
        Assert.assertNull(ctds2.getRawTableLocationProvider(StandaloneTableKey.getInstance(), tlk3));
    }

    /**
     * Verify that {@link TableDataService#shutdown()} delivers a terminal exception to an existing provider subscriber
     * and clears the cached provider.
     */
    @Test
    public void testShutdownNotifiesSubscribers() {
        final SubscribableTableDataService tds = new SubscribableTableDataService();
        final TableLocationProvider provider = tds.getTableLocationProvider(StandaloneTableKey.getInstance());
        final RecordingListener listener = new RecordingListener();
        provider.subscribe(listener);
        Assert.assertNull(listener.exception);

        tds.shutdown();

        Assert.assertNotNull("subscriber notified of error on shutdown", listener.exception);
        // The cached provider is cleared: a subsequent request yields a fresh instance.
        Assert.assertNotSame(provider, tds.getTableLocationProvider(StandaloneTableKey.getInstance()));
    }

    /**
     * A {@link TableLocationProvider.Listener} that records the exception delivered to it.
     */
    private static final class RecordingListener implements TableLocationProvider.Listener {

        private TableDataException exception;

        /** Ignore added keys. */
        @Override
        public void handleTableLocationKeyAdded(
                @NotNull final LiveSupplier<ImmutableTableLocationKey> tableLocationKey) {}

        /** Ignore removed keys. */
        @Override
        public void handleTableLocationKeyRemoved(
                @NotNull final LiveSupplier<ImmutableTableLocationKey> tableLocationKey) {}

        /** Record the delivered exception. */
        @Override
        public void handleException(@NotNull final TableDataException exception) {
            this.exception = exception;
        }
    }

    /**
     * A subscription-supporting provider that activates synchronously, so a subscribe call completes without a backing
     * data source.
     */
    private static final class SubscribableProvider extends AbstractTableLocationProvider {

        private SubscribableProvider() {
            super(StandaloneTableKey.getInstance(), true, TableUpdateMode.ADD_REMOVE, TableUpdateMode.ADD_REMOVE);
        }

        /** Mark activation successful immediately so {@code subscribe} does not block. */
        @Override
        protected void activateUnderlyingDataSource() {
            activationSuccessful(this);
        }

        /** No underlying data source to deactivate. */
        @Override
        protected void deactivateUnderlyingDataSource() {}

        /** No locations to discover. */
        @Override
        public void refresh() {}

        /** This provider has a single implicit subscription, keyed by itself. */
        @Override
        protected <T> boolean matchSubscriptionToken(final T token) {
            return token == this;
        }

        /** This provider serves no locations. */
        @Override
        @NotNull
        protected TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
            throw new UnsupportedOperationException("test provider has no locations");
        }
    }

    /**
     * A minimal {@link AbstractTableDataService} whose providers support subscriptions.
     */
    private static final class SubscribableTableDataService extends AbstractTableDataService {

        private SubscribableTableDataService() {
            super("subscribableTds");
        }

        /** Create a subscription-supporting provider. */
        @Override
        @NotNull
        protected TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
            return new SubscribableProvider();
        }
    }

    private static class DummyTableDataService extends AbstractTableDataService {
        final TableLocation tableLocation;

        private DummyTableDataService(@NotNull final String name, @NotNull final TableLocation tableLocation) {
            super(name);
            this.tableLocation = tableLocation;
        }

        @Override
        @NotNull
        protected TableLocationProvider makeTableLocationProvider(@NotNull TableKey tableKey) {
            return new SingleTableLocationProvider(tableLocation, TableUpdateMode.ADD_REMOVE);
        }
    }

    private static class DummyServiceSelector implements CompositeTableDataService.ServiceSelector {
        final TableDataService[] tableDataServices;

        private DummyServiceSelector(final TableLocationKey tlk1, final TableLocationKey tlk2) {
            final FilteredTableDataService.LocationKeyFilterProvider dummyFilter =
                    tableKey -> FilteredTableDataService.LocationKeyFilter.ALL;
            tableDataServices = new TableDataService[] {
                    new FilteredTableDataService(new DummyTableDataService("dummyTds1",
                            new DummyTableLocation(StandaloneTableKey.getInstance(), tlk1)), dummyFilter),
                    new FilteredTableDataService(new DummyTableDataService("dummyTds2",
                            new DummyTableLocation(StandaloneTableKey.getInstance(), tlk2)), dummyFilter)
            };
            // Init table locations
            tableDataServices[0].getTableLocationProvider(StandaloneTableKey.getInstance())
                    .getTableLocationIfPresent(tlk1);
            tableDataServices[1].getTableLocationProvider(StandaloneTableKey.getInstance())
                    .getTableLocationIfPresent(tlk2);
        }

        @Override
        public TableDataService[] call(@NotNull TableKey tableKey) {
            return tableDataServices;
        }

        @Override
        public void resetServices() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resetServices(@NotNull TableKey key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdownServices() {
            throw new UnsupportedOperationException();
        }
    }
}
