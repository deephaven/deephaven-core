//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.SortColumn;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.StandaloneLivenessManager;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.local.URITableLocationKey;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Category(OutOfBandTest.class)
public class AbstractTableLocationTest extends RefreshingTableTestCase {
    private static class TestTableLocationProvider extends AbstractTableLocationProvider {

        public TestTableLocationProvider(final boolean supportsSubscriptions) {
            super(supportsSubscriptions);
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

        @Override
        @NotNull
        public UPDATE_TYPE getUpdateMode() {
            return UPDATE_TYPE.REFRESHING;
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
            return null;
        }

        @Override
        public @Nullable BasicDataIndex loadDataIndex(@NotNull String... columns) {
            return null;
        }

        @Override
        public void refresh() {
            Assert.statementNeverExecuted();
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
        if (null == ProcessEnvironment.tryGet()) {
            ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                    "SourcePartitionedTableTest", new StreamLoggerImpl());
        }
        super.setUp();
        setExpectError(false);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
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
        provider.getTableLocationKeys(initialTrackedKeys::add);

        // Resolve and create all the table locations
        final List<TestTableLocation> tableLocations = new ArrayList<>();
        for (final TableLocationKey key : initialKeys) {
            final TestTableLocation tl = (TestTableLocation) provider.getTableLocation(key);
            tableLocations.add(tl);
        }

        // Externally manage keys 2 & 3 (simulate a TLSB)
        manager.manage(initialTrackedKeys.get(2));
        manager.manage(initialTrackedKeys.get(3));

        // Also manage the table locations for key 3 (simulate a filtered RCSM)
        manager.manage(tableLocations.get(3));

        // Drop the first 4 keys from the provider
        for (int i = 0; i < 4; i++) {
            final TableLocationKey removedKey = initialKeys.get(i);
            provider.removeKey(removedKey);
        }

        // Verify only the last key is present for new listeners
        keys = new HashSet<>(provider.getTableLocationKeys());
        Assert.eqTrue(keys.contains(initialKeys.get(4)), "keys.contains(initialKeys.get(4))");
        Assert.eq(keys.size(), "keys.size()", 1);

        // Verify that we CAN'T retrieve the unmanaged locations from the provider (they were dropped)
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(0)),
                "provider.hasTableLocationKey(initialKeys.get(0))");
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(1)),
                "provider.hasTableLocationKey(initialKeys.get(1))");

        // Verify the tableLocations for the unmanaged keys are destroyed
        Assert.eqTrue(tableLocations.get(0).isDestroyed(), "tableLocations.get(0).isDestroyed()");
        Assert.eqTrue(tableLocations.get(1).isDestroyed(), "tableLocations.get(1).isDestroyed()");

        // Verify that we CAN retrieve the managed locations from the provider (they are still live)
        Assert.eqTrue(provider.hasTableLocationKey(initialKeys.get(2)),
                "provider.hasTableLocationKey(initialKeys.get(2))");
        Assert.eqTrue(provider.hasTableLocationKey(initialKeys.get(3)),
                "provider.hasTableLocationKey(initialKeys.get(3))");

        // Verify the tableLocations for the managed keys are NOT destroyed
        Assert.eqFalse(tableLocations.get(2).isDestroyed(), "tableLocations.get(2).isDestroyed()");
        Assert.eqFalse(tableLocations.get(3).isDestroyed(), "tableLocations.get(3).isDestroyed()");

        // Un-manage the two keys
        manager.unmanage(initialTrackedKeys.get(2));
        manager.unmanage(initialTrackedKeys.get(3));

        // location #2 should be destroyed, location #3 should not (because an RCSM is managing it)
        Assert.eqTrue(tableLocations.get(2).isDestroyed(), "tableLocations.get(2).isDestroyed()");
        Assert.eqFalse(tableLocations.get(3).isDestroyed(), "tableLocations.get(3).isDestroyed()");

        // Verify that we CAN'T retrieve the (now) unmanaged locations from the provider (they were dropped)
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(2)),
                "provider.hasTableLocationKey(initialKeys.get(2))");
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(3)),
                "provider.hasTableLocationKey(initialKeys.get(3))");

        // Release the table location being held by the RCSM
        manager.unmanage(tableLocations.get(3));
        Assert.eqTrue(tableLocations.get(3).isDestroyed(), "tableLocations.get(3).isDestroyed()");
    }

    /**
     * Test the management and release of the liveness of table location keys is correct when using transactions.
     */
    @Test
    public void testTableLocationKeyTransactionManagement() {

        // Create a test table location provider
        final TestTableLocationProvider provider = new TestTableLocationProvider(true);

        // Create a set of table location keys
        final List<TableLocationKey> initialKeys = createKeys(5);

        // Add the keys to the table location provider
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
        provider.getTableLocationKeys(initialTrackedKeys::add);

        // Resolve and create all the table locations
        final List<TestTableLocation> tableLocations = new ArrayList<>();
        for (final TableLocationKey key : initialKeys) {
            final TestTableLocation tl = (TestTableLocation) provider.getTableLocation(key);
            tableLocations.add(tl);
        }

        // Externally manage keys 2 & 3 (simulate a TLSB)
        manager.manage(initialTrackedKeys.get(2));
        manager.manage(initialTrackedKeys.get(3));

        // Also manage the table locations for key 3 (simulate a filtered RCSM)
        manager.manage(tableLocations.get(3));

        // Drop the first 4 keys from the provider
        provider.beginTransaction(provider);
        for (int i = 0; i < 4; i++) {
            final TableLocationKey removedKey = initialKeys.get(i);
            provider.removeKey(removedKey, provider);
        }
        provider.endTransaction(provider);

        // Verify only the last key is present for new listeners
        keys = new HashSet<>(provider.getTableLocationKeys());
        Assert.eqTrue(keys.contains(initialKeys.get(4)), "keys.contains(initialKeys.get(4))");
        Assert.eq(keys.size(), "keys.size()", 1);

        // Verify that we CAN'T retrieve the unmanaged locations from the provider (they were dropped)
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(0)),
                "provider.hasTableLocationKey(initialKeys.get(0))");
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(1)),
                "provider.hasTableLocationKey(initialKeys.get(1))");

        // Verify the tableLocations for the unmanaged keys are destroyed
        Assert.eqTrue(tableLocations.get(0).isDestroyed(), "tableLocations.get(0).isDestroyed()");
        Assert.eqTrue(tableLocations.get(1).isDestroyed(), "tableLocations.get(1).isDestroyed()");

        // Verify that we CAN retrieve the managed locations from the provider (they are still live)
        Assert.eqTrue(provider.hasTableLocationKey(initialKeys.get(2)),
                "provider.hasTableLocationKey(initialKeys.get(2))");
        Assert.eqTrue(provider.hasTableLocationKey(initialKeys.get(3)),
                "provider.hasTableLocationKey(initialKeys.get(3))");

        // Verify the tableLocations for the managed keys are NOT destroyed
        Assert.eqFalse(tableLocations.get(2).isDestroyed(), "tableLocations.get(2).isDestroyed()");
        Assert.eqFalse(tableLocations.get(3).isDestroyed(), "tableLocations.get(3).isDestroyed()");

        // Un-manage the two keys
        manager.unmanage(initialTrackedKeys.get(2));
        manager.unmanage(initialTrackedKeys.get(3));

        // location #2 should be destroyed, location #3 should not (because an RCSM is managing it)
        Assert.eqTrue(tableLocations.get(2).isDestroyed(), "tableLocations.get(2).isDestroyed()");
        Assert.eqFalse(tableLocations.get(3).isDestroyed(), "tableLocations.get(3).isDestroyed()");

        // Verify that we CAN'T retrieve the (now) unmanaged locations from the provider (they were dropped)
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(2)),
                "provider.hasTableLocationKey(initialKeys.get(2))");
        Assert.eqFalse(provider.hasTableLocationKey(initialKeys.get(3)),
                "provider.hasTableLocationKey(initialKeys.get(3))");

        // Release the table location being held by the RCSM and verify its destruction.
        manager.unmanage(tableLocations.get(3));
        Assert.eqTrue(tableLocations.get(3).isDestroyed(), "tableLocations.get(3).isDestroyed()");
    }
}
