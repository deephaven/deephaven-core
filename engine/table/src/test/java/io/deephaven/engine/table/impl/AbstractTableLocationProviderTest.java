//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.SortColumn;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.StandaloneLivenessManager;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.local.URITableLocationKey;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.test.types.OutOfBandTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

@Category(OutOfBandTest.class)
public class AbstractTableLocationProviderTest extends RefreshingTableTestCase {
    private static class TestTableLocationProvider extends AbstractTableLocationProvider {

        public TestTableLocationProvider(final boolean supportsSubscriptions) {
            super(supportsSubscriptions, TableUpdateMode.ADD_REMOVE, TableUpdateMode.ADD_REMOVE);
        }

        @Override
        protected @NotNull TableLocation makeTableLocation(@NotNull TableLocationKey locationKey) {
            return new TestTableLocation(getKey(), locationKey, false);
        }

        @Override
        public void refresh() {
            Assert.statementNeverExecuted();
        }

        public void addKey(@NotNull TableLocationKey locationKey) {
            handleTableLocationKeyAdded(locationKey, null);
        }

        public void removeKey(@NotNull TableLocationKey locationKey) {
            handleTableLocationKeyRemoved(locationKey, null);
        }


        public void beginTransaction(@NotNull Object token) {
            super.beginTransaction(token);
        }

        public void endTransaction(@NotNull Object token) {
            super.endTransaction(token);
        }

        public void addKey(@NotNull TableLocationKey locationKey, @NotNull Object token) {
            handleTableLocationKeyAdded(locationKey, token);
        }

        public void removeKey(@NotNull TableLocationKey locationKey, @NotNull Object token) {
            handleTableLocationKeyRemoved(locationKey, token);
        }
    }

    private static class TestTableLocation extends AbstractTableLocation {
        private boolean destroyed = false;

        protected TestTableLocation(@NotNull TableKey tableKey, @NotNull TableLocationKey tableLocationKey,
                boolean supportsSubscriptions) {
            super(tableKey, tableLocationKey, supportsSubscriptions);
        }

        public boolean isDestroyed() {
            return destroyed;
        }

        @Override
        protected void destroy() {
            super.destroy();
            destroyed = true;
        }

        @Override
        protected @NotNull ColumnLocation makeColumnLocation(@NotNull String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable BasicDataIndex loadDataIndex(@NotNull String... columns) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void refresh() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull List<SortColumn> getSortedColumns() {
            return List.of();
        }

        @Override
        public @NotNull List<String[]> getDataIndexColumns() {
            return List.of();
        }

        @Override
        public boolean hasDataIndex(@NotNull String... columns) {
            return false;
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setExpectError(false);
    }

    private List<TableLocationKey> createKeys(final int count) {
        final List<TableLocationKey> keys = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            keys.add(new URITableLocationKey(URI.create("file:///tmp/" + i), i, null));
        }
        return keys;
    }

    /**
     * Test the management and release of the liveness of table location keys is correct.
     */
    @Test
    public void testTableLocationKeyManagement() {

        // Create a test table location provider
        final TestTableLocationProvider provider = new TestTableLocationProvider(false);

        // Create a set of table location keys
        final List<TableLocationKey> initialKeys = createKeys(5);

        // Add the keys to the table location provider
        for (final TableLocationKey locationKey : initialKeys) {
            provider.addKey(locationKey);
        }

        // Verify all the initial keys are present for new listeners
        Set<TableLocationKey> keys = new HashSet<>(provider.getTableLocationKeys());
        for (final TableLocationKey tlk : initialKeys) {
            Assert.eqTrue(keys.contains(tlk), "keys.contains(tlk)");
        }

        // Create a local liveness manager
        final StandaloneLivenessManager manager = new StandaloneLivenessManager(false);

        // Get a list of the LiveSupplier<> keys
        final List<LiveSupplier<ImmutableTableLocationKey>> initialTrackedKeys = new ArrayList<>();
        provider.getTableLocationKeys(lstlk -> {
            // Externally manage all keys (simulate a TLSB)
            manager.manage(lstlk);
            initialTrackedKeys.add(lstlk);
        });

        // Resolve and create all the table locations
        final List<TestTableLocation> tableLocations = new ArrayList<>();
        for (final TableLocationKey key : initialKeys) {
            final TestTableLocation tl = (TestTableLocation) provider.getTableLocation(key);
            tableLocations.add(tl);
        }

        // Drop the first 4 keys from the provider
        for (int i = 0; i < 4; i++) {
            final TableLocationKey removedKey = initialKeys.get(i);
            provider.removeKey(removedKey);
        }

        // Simulate delivering the initial keys to the RCSM
        final List<TableLocation> includedLocations = new ArrayList<>();
        includedLocations.add(provider.getTableLocation(initialKeys.get(2)));
        includedLocations.forEach(manager::manage);
        initialTrackedKeys.forEach(manager::unmanage);

        // Verify only the last key is present for new listeners
        keys = new HashSet<>(provider.getTableLocationKeys());
        Assert.eqTrue(keys.contains(initialKeys.get(4)), "keys.contains(initialKeys.get(4))");
        Assert.eq(keys.size(), "keys.size()", 1);

        // Verify that we CAN'T retrieve the unmanaged locations from the provider (they were dropped)
        IntStream.range(0, 4).forEach(
                i -> {
                    Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(i)),
                            "provider.hasTableLocationKey(initialKeys.get(" + i + "))");
                });

        // Verify the tableLocations for only the unmanaged keys are destroyed
        Assert.eqTrue(tableLocations.get(0).isDestroyed(), "tableLocations.get(0).isDestroyed()");
        Assert.eqTrue(tableLocations.get(1).isDestroyed(), "tableLocations.get(1).isDestroyed()");
        Assert.eqTrue(tableLocations.get(3).isDestroyed(), "tableLocations.get(3).isDestroyed()");
        Assert.eqFalse(tableLocations.get(2).isDestroyed(), "tableLocations.get(2).isDestroyed()");

        // Verify the tableLocations for the previously included keys are destroyed
        includedLocations.forEach(manager::unmanage);
        Assert.eqTrue(tableLocations.get(2).isDestroyed(), "tableLocations.get(2).isDestroyed()");

        // Verify that we CAN retrieve the last key from the provider and the location is not destroyed
        Assert.eqTrue(provider.hasTableLocationKey(initialKeys.get(4)),
                "provider.hasTableLocationKey(initialKeys.get(4))");
        Assert.eqFalse(tableLocations.get(4).isDestroyed(), "tableLocations.get(4).isDestroyed()");

        // Drop the final key from the provider
        provider.removeKey(initialKeys.get(4));

        // Verify that we CAN retrieve the last key from the provider and the location is not destroyed
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(4)),
                "provider.hasTableLocationKey(initialKeys.get(4))");
        Assert.eqTrue(tableLocations.get(4).isDestroyed(), "tableLocations.get(4).isDestroyed()");
    }


    /**
     * Test the management and release of the liveness of table location keys is correct when using transactions.
     */
    @Test
    public void testTableLocationKeyTransactionManagement() {

        // Create a test table location provider
        final TestTableLocationProvider provider = new TestTableLocationProvider(false);

        // Create a set of table location keys
        final List<TableLocationKey> initialKeys = createKeys(5);

        // Add the keys to the table location provider in a single tranasction
        provider.beginTransaction(provider);
        for (final TableLocationKey locationKey : initialKeys) {
            provider.addKey(locationKey, provider);
        }
        provider.endTransaction(provider);

        // Verify all the initial keys are present for new listeners
        Set<TableLocationKey> keys = new HashSet<>(provider.getTableLocationKeys());
        for (final TableLocationKey tlk : initialKeys) {
            Assert.eqTrue(keys.contains(tlk), "keys.contains(tlk)");
        }

        // Create a local liveness manager
        final StandaloneLivenessManager manager = new StandaloneLivenessManager(false);

        // Get a list of the LiveSupplier<> keys
        final List<LiveSupplier<ImmutableTableLocationKey>> initialTrackedKeys = new ArrayList<>();
        provider.getTableLocationKeys(lstlk -> {
            // Externally manage all keys (simulate a TLSB)
            manager.manage(lstlk);
            initialTrackedKeys.add(lstlk);
        });

        // Resolve and create all the table locations
        final List<TestTableLocation> tableLocations = new ArrayList<>();
        for (final TableLocationKey key : initialKeys) {
            final TestTableLocation tl = (TestTableLocation) provider.getTableLocation(key);
            tableLocations.add(tl);
        }

        // Drop the first 4 keys from the provider in a single tranasction
        provider.beginTransaction(provider);
        for (int i = 0; i < 4; i++) {
            final TableLocationKey removedKey = initialKeys.get(i);
            provider.removeKey(removedKey, provider);
        }
        provider.endTransaction(provider);

        // Simulate delivering the initial keys to the RCSM
        final List<TableLocation> includedLocations = new ArrayList<>();
        includedLocations.add(provider.getTableLocation(initialKeys.get(2)));
        includedLocations.forEach(manager::manage);
        initialTrackedKeys.forEach(manager::unmanage);

        // Verify only the last key is present for new listeners
        keys = new HashSet<>(provider.getTableLocationKeys());
        Assert.eqTrue(keys.contains(initialKeys.get(4)), "keys.contains(initialKeys.get(4))");
        Assert.eq(keys.size(), "keys.size()", 1);

        // Verify that we CAN'T retrieve the unmanaged locations from the provider (they were dropped)
        IntStream.range(0, 4).forEach(
                i -> {
                    Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(i)),
                            "provider.hasTableLocationKey(initialKeys.get(" + i + "))");
                });

        // Verify the tableLocations for only the unmanaged keys are destroyed
        Assert.eqTrue(tableLocations.get(0).isDestroyed(), "tableLocations.get(0).isDestroyed()");
        Assert.eqTrue(tableLocations.get(1).isDestroyed(), "tableLocations.get(1).isDestroyed()");
        Assert.eqTrue(tableLocations.get(3).isDestroyed(), "tableLocations.get(3).isDestroyed()");
        Assert.eqFalse(tableLocations.get(2).isDestroyed(), "tableLocations.get(2).isDestroyed()");

        // Verify the tableLocations for the previously included keys are destroyed
        includedLocations.forEach(manager::unmanage);
        Assert.eqTrue(tableLocations.get(2).isDestroyed(), "tableLocations.get(2).isDestroyed()");

        // Verify that we CAN retrieve the last key from the provider and the location is not destroyed
        Assert.eqTrue(provider.hasTableLocationKey(initialKeys.get(4)),
                "provider.hasTableLocationKey(initialKeys.get(4))");
        Assert.eqFalse(tableLocations.get(4).isDestroyed(), "tableLocations.get(4).isDestroyed()");

        // Drop the final key from the provider
        provider.removeKey(initialKeys.get(4));

        // Verify that we CAN retrieve the last key from the provider and the location is not destroyed
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(4)),
                "provider.hasTableLocationKey(initialKeys.get(4))");
        Assert.eqTrue(tableLocations.get(4).isDestroyed(), "tableLocations.get(4).isDestroyed()");
    }
}
