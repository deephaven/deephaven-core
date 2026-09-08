//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.ImmutableTableKey;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.FilteredTableDataService.LocationKeyFilter;
import io.deephaven.engine.table.impl.locations.impl.FilteredTableDataService.LocationKeyFilterProvider;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Verifies that {@link FilteredTableDataService} applies its {@code locationKeyFilter} consistently across every way a
 * caller can discover a location.
 */
public class TestFilteredTableDataService extends RefreshingTableTestCase {

    /** The table whose locations are filtered throughout. */
    private static final TableKey TABLE = new NamedTableKey("Market.Quotes");

    /**
     * Enumerating locations through the filtered provider hides the keys the {@code locationKeyFilter} rejects, rather
     * than passing the underlying provider's unfiltered set through.
     */
    @Test
    public void testGetTableLocationKeysAppliesFilter() {
        final TableLocationKey a = keyFor("A");
        final TableLocationKey b = keyFor("B");
        final TableLocationKey c = keyFor("C");

        final PopulatedProvider underlying = new PopulatedProvider(TABLE);
        underlying.addKey(a);
        underlying.addKey(b);
        underlying.addKey(c);

        final FilteredTableDataService filtered =
                new FilteredTableDataService(new FixedProviderService(underlying), tableKey -> key -> !key.equals(c));

        final Set<ImmutableTableLocationKey> visible =
                new HashSet<>(filtered.getTableLocationProvider(TABLE).getTableLocationKeys());

        Assert.assertEquals(Set.of(a, b), visible);
    }

    /**
     * Enumeration and {@code hasTableLocationKey} report the same set. These are the two discovery paths a caller can
     * take, and a location visible through one but not the other is the defect this fixes.
     */
    @Test
    public void testEnumerationAgreesWithHasTableLocationKey() {
        final TableLocationKey a = keyFor("A");
        final TableLocationKey b = keyFor("B");
        final TableLocationKey c = keyFor("C");

        final PopulatedProvider underlying = new PopulatedProvider(TABLE);
        underlying.addKey(a);
        underlying.addKey(b);
        underlying.addKey(c);

        final FilteredTableDataService filtered =
                new FilteredTableDataService(new FixedProviderService(underlying), tableKey -> key -> !key.equals(c));
        final TableLocationProvider provider = filtered.getTableLocationProvider(TABLE);

        final Set<ImmutableTableLocationKey> enumerated = new HashSet<>(provider.getTableLocationKeys());
        for (final TableLocationKey key : new TableLocationKey[] {a, b, c}) {
            Assert.assertEquals("agreement for " + key,
                    provider.hasTableLocationKey(key), enumerated.contains(key));
        }
    }

    /**
     * The caller's own predicate still applies, and is combined with the service's filter rather than replacing it.
     */
    @Test
    public void testCallerPredicateIsCombinedWithTheServiceFilter() {
        final TableLocationKey a = keyFor("A");
        final TableLocationKey b = keyFor("B");
        final TableLocationKey c = keyFor("C");

        final PopulatedProvider underlying = new PopulatedProvider(TABLE);
        underlying.addKey(a);
        underlying.addKey(b);
        underlying.addKey(c);

        final FilteredTableDataService filtered =
                new FilteredTableDataService(new FixedProviderService(underlying), tableKey -> key -> !key.equals(c));

        final Set<ImmutableTableLocationKey> visible = new HashSet<>();
        filtered.getTableLocationProvider(TABLE)
                .getTableLocationKeys(trackedKey -> visible.add(trackedKey.get()), key -> !key.equals(b));

        Assert.assertEquals(Set.of(a), visible);
    }

    /**
     * Building a table's provider binds the filter once, and neither a repeated provider lookup nor per-location
     * discovery through that provider binds it again.
     */
    @Test
    public void testFilterIsBoundOncePerTable() {
        final PopulatedProvider underlying = new PopulatedProvider(TABLE);
        underlying.addKey(keyFor("A"));
        underlying.addKey(keyFor("B"));
        underlying.addKey(keyFor("C"));

        final CountingProvider counting = new CountingProvider(LocationKeyFilter.ALL);
        final FilteredTableDataService filtered =
                new FilteredTableDataService(new FixedProviderService(underlying), counting);

        final TableLocationProvider provider = filtered.getTableLocationProvider(TABLE);
        Assert.assertEquals(3, provider.getTableLocationKeys().size());
        Assert.assertTrue(provider.hasTableLocationKey(keyFor("A")));
        Assert.assertEquals(3, filtered.getTableLocationProvider(TABLE).getTableLocationKeys().size());

        Assert.assertEquals("forTable calls", 1, counting.bindCount);
    }

    /**
     * The bound filter, not the unbound one, decides each location, so a filter that discriminates on the table takes
     * effect per table rather than per service.
     */
    @Test
    public void testBoundFilterDecidesLocations() {
        final TableLocationKey a = keyFor("A");
        final TableLocationKey b = keyFor("B");

        final PopulatedProvider underlying = new PopulatedProvider(TABLE);
        underlying.addKey(a);
        underlying.addKey(b);

        // Binding to TABLE yields a filter that keeps only A; any other table is excluded outright.
        final LocationKeyFilterProvider perTable =
                tableKey -> TABLE.equals(tableKey) ? key -> key.equals(a) : LocationKeyFilter.NONE;

        final TableLocationProvider provider =
                new FilteredTableDataService(new FixedProviderService(underlying), perTable)
                        .getTableLocationProvider(TABLE);

        Assert.assertEquals(Set.of(a), new HashSet<>(provider.getTableLocationKeys()));
        Assert.assertTrue(provider.hasTableLocationKey(a));
        Assert.assertFalse(provider.hasTableLocationKey(b));
    }

    /**
     * Binding to {@link LocationKeyFilter#NONE} yields an empty provider without the filtered service being consulted
     * at all. That service is frequently remote, so avoiding it is the point of the {@code NONE} answer.
     */
    @Test
    public void testNoneAvoidsTheFilteredService() {
        final PopulatedProvider underlying = new PopulatedProvider(TABLE);
        underlying.addKey(keyFor("A"));

        final RecordingProviderService service = new RecordingProviderService(underlying);
        final FilteredTableDataService filtered =
                new FilteredTableDataService(service, tableKey -> LocationKeyFilter.NONE);

        final TableLocationProvider provider = filtered.getTableLocationProvider(TABLE);

        Assert.assertTrue(provider.getTableLocationKeys().isEmpty());
        Assert.assertFalse(provider.hasTableLocationKey(keyFor("A")));
        Assert.assertEquals(TABLE, provider.getKey());
        Assert.assertFalse("the filtered service was consulted", service.consulted);
    }

    /**
     * The single-location raw lookup binds the filter to the table key it was handed, so one location key is accepted
     * for one table and rejected for another. This is the path that previously asked an unbound filter about a bare
     * location key and so could not tell the two tables apart.
     */
    @Test
    public void testRawLookupAppliesTheTableAwareFilter() {
        final TableLocationKey a = keyFor("A");
        final TableKey other = new NamedTableKey("Market.Trades");

        // The delegate holds the same location under BOTH tables, so it can answer for either. Without that, the
        // negative assertion below would pass even if the filter were never consulted - the delegate would return
        // null for an unknown table anyway, which is what made an earlier version of this test vacuous.
        final PopulatedProvider underlying = new PopulatedProvider(TABLE);
        underlying.addKey(a);
        final PopulatedProvider otherUnderlying = new PopulatedProvider(other);
        otherUnderlying.addKey(a);
        final PerTableProviderService delegate = new PerTableProviderService(Map.of(
                TABLE, underlying, other, otherUnderlying));
        delegate.getTableLocationProvider(TABLE);
        delegate.getTableLocationProvider(other);
        Assert.assertSame("the delegate must be able to answer for the excluded table, or this test proves nothing",
                otherUnderlying, delegate.getRawTableLocationProvider(other, a));

        // Accepts every location of TABLE and no location of any other table.
        final LocationKeyFilterProvider perTable =
                tableKey -> TABLE.equals(tableKey) ? LocationKeyFilter.ALL : LocationKeyFilter.NONE;
        final FilteredTableDataService filtered = new FilteredTableDataService(delegate, perTable);

        Assert.assertSame(underlying, filtered.getRawTableLocationProvider(TABLE, a));
        Assert.assertNull("the same location key must be rejected for a table the filter excludes",
                filtered.getRawTableLocationProvider(other, a));
    }

    /**
     * A provider that ignores its argument supplies the same filter for every table, which is how a filter that does
     * not discriminate on the table is expressed.
     */
    @Test
    public void testTableBlindProviderSuppliesOneFilter() {
        final LocationKeyFilter blind = key -> true;
        final LocationKeyFilterProvider provider = tableKey -> blind;
        Assert.assertSame(blind, provider.forTable(TABLE));
        Assert.assertSame(blind, provider.forTable(new NamedTableKey("Other.Table")));
    }

    /**
     * A {@link LocationKeyFilterProvider} that counts how often it is asked to bind, supplying a fixed filter.
     */
    private static final class CountingProvider implements LocationKeyFilterProvider {

        private final LocationKeyFilter delegate;
        private int bindCount;

        /**
         * Creates a counting provider that always supplies the given filter.
         *
         * @param delegate the filter that decides locations once bound
         */
        private CountingProvider(@NotNull final LocationKeyFilter delegate) {
            this.delegate = delegate;
        }

        /**
         * Records the binding and returns the delegate.
         *
         * @param tableKey the table being bound to
         * @return the delegate
         */
        @Override
        @NotNull
        public LocationKeyFilter forTable(@NotNull final TableKey tableKey) {
            ++bindCount;
            return delegate;
        }
    }

    /**
     * A named {@link TableKey}, distinct from every other key, so a test cannot pass by accident on a key the
     * implementation supplied by default.
     */
    private static final class NamedTableKey implements ImmutableTableKey {

        private final String name;

        /**
         * Creates a key with the given name.
         *
         * @param name the name, used for identity and display
         */
        private NamedTableKey(@NotNull final String name) {
            this.name = name;
        }

        /**
         * Returns the implementation name used in diagnostic output.
         *
         * @return the implementation name
         */
        @Override
        public String getImplementationName() {
            return "NamedTableKey";
        }

        /**
         * Appends this key's name to the given log output.
         *
         * @param logOutput the output to append to
         * @return the output, for chaining
         */
        @Override
        public LogOutput append(final LogOutput logOutput) {
            return logOutput.append(name);
        }

        /**
         * Returns this key's name.
         *
         * @return the name
         */
        @Override
        public String toString() {
            return name;
        }

        /**
         * Hashes on the name.
         *
         * @return the hash code
         */
        @Override
        public int hashCode() {
            return name.hashCode();
        }

        /**
         * Compares on the name.
         *
         * @param other the object to compare to
         * @return true if the other object is a NamedTableKey with the same name
         */
        @Override
        public boolean equals(final Object other) {
            return other instanceof NamedTableKey && name.equals(((NamedTableKey) other).name);
        }
    }

    /**
     * Creates a single-partition location key for the given value.
     *
     * @param value the value of the {@code Part} partition
     * @return the key
     */
    private static TableLocationKey keyFor(@NotNull final String value) {
        final Map<String, Comparable<?>> partitions = new HashMap<>();
        partitions.put("Part", value);
        return new SimpleTableLocationKey(partitions);
    }

    /**
     * A subscription-free provider populated with a fixed set of keys via {@link #addKey(TableLocationKey)}.
     */
    private static final class PopulatedProvider extends AbstractTableLocationProvider {

        /**
         * Creates an empty provider for the given table.
         *
         * @param tableKey the table this provider serves
         */
        private PopulatedProvider(@NotNull final TableKey tableKey) {
            super(tableKey, false, TableUpdateMode.ADD_REMOVE, TableUpdateMode.ADD_REMOVE);
        }

        /**
         * Adds a key to this provider's set.
         *
         * @param locationKey the key to add
         */
        private void addKey(@NotNull final TableLocationKey locationKey) {
            handleTableLocationKeyAdded(locationKey);
        }

        /**
         * Never called: this provider is enumerated, not read.
         *
         * @param locationKey the key that would be built
         * @return never returns
         */
        @Override
        @NotNull
        protected TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
            throw new UnsupportedOperationException("test provider is enumerated only");
        }

        /** Does nothing: the key set is fixed at construction. */
        @Override
        public void refresh() {}
    }

    /**
     * A minimal {@link AbstractTableDataService} that always hands back one fixed provider.
     */
    private static final class FixedProviderService extends AbstractTableDataService {

        private final TableLocationProvider provider;

        /**
         * Creates a service backed by a single provider.
         *
         * @param provider the provider to return for every table
         */
        private FixedProviderService(@NotNull final TableLocationProvider provider) {
            super("fixedProviderService");
            this.provider = provider;
        }

        /**
         * Returns the fixed provider, regardless of the requested table.
         *
         * @param tableKey ignored
         * @return the fixed provider
         */
        @Override
        @NotNull
        protected TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
            return provider;
        }
    }

    /**
     * A {@link FixedProviderService} that records whether it was ever asked for a provider.
     */
    private static final class RecordingProviderService extends AbstractTableDataService {

        private final TableLocationProvider provider;
        private boolean consulted;

        /**
         * Creates a service backed by a single provider.
         *
         * @param provider the provider to return for every table
         */
        private RecordingProviderService(@NotNull final TableLocationProvider provider) {
            super("recordingProviderService");
            this.provider = provider;
        }

        /**
         * Records the request and returns the fixed provider.
         *
         * @param tableKey ignored
         * @return the fixed provider
         */
        @Override
        @NotNull
        protected TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
            consulted = true;
            return provider;
        }
    }


    /**
     * Subscription delivery applies the table-bound filter, so a listener sees only the keys the filter accepts - both
     * the initial snapshot and keys pushed afterwards. This is the one discovery path a caller can take that
     * enumeration and {@code hasTableLocationKey} do not cover.
     */
    @Test
    public void testSubscriptionDeliveryAppliesTheFilter() {
        final TableLocationKey a = keyFor("A");
        final TableLocationKey b = keyFor("B");
        final TableLocationKey pushed = keyFor("pushedAccepted");
        final TableLocationKey pushedRejected = keyFor("C");

        final SubscribableProvider underlying = new SubscribableProvider(TABLE);
        underlying.addKey(a);
        underlying.addKey(b);

        // Reject exactly the keys named "C"; everything else is visible.
        final FilteredTableDataService filtered = new FilteredTableDataService(
                new FixedProviderService(underlying), tableKey -> key -> !key.equals(pushedRejected));
        final TableLocationProvider provider = filtered.getTableLocationProvider(TABLE);
        Assert.assertTrue("the provider must be subscribable for this test to mean anything",
                provider.supportsSubscriptions());

        final Set<ImmutableTableLocationKey> delivered = new HashSet<>();
        provider.subscribe(new TableLocationProvider.Listener() {
            @Override
            public void handleTableLocationKeyAdded(@NotNull final LiveSupplier<ImmutableTableLocationKey> key) {
                delivered.add(key.get());
            }

            @Override
            public void handleTableLocationKeyRemoved(@NotNull final LiveSupplier<ImmutableTableLocationKey> key) {
                delivered.remove(key.get());
            }

            @Override
            public void handleException(@NotNull final TableDataException exception) {
                throw exception;
            }
        });

        // The initial snapshot is filtered.
        Assert.assertEquals(Set.of(a, b), delivered);

        // ... and so is everything pushed afterwards.
        underlying.addKey(pushed);
        underlying.addKey(pushedRejected);
        Assert.assertEquals(Set.of(a, b, pushed), delivered);
    }

    /**
     * A {@link TableDataService} holding a distinct provider per table. The provider cache is keyed by each provider's
     * own table key, so a single provider instance cannot serve two different tables.
     */
    private static final class PerTableProviderService extends AbstractTableDataService {

        private final Map<TableKey, TableLocationProvider> providers;

        /**
         * Creates a service backed by one provider per table.
         *
         * @param providers the provider to return for each table
         */
        private PerTableProviderService(@NotNull final Map<TableKey, TableLocationProvider> providers) {
            super("perTableProviderService");
            this.providers = providers;
        }

        /**
         * Returns the provider registered for the requested table.
         *
         * @param tableKey the table being requested
         * @return that table's provider
         */
        @Override
        @NotNull
        protected TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
            final TableLocationProvider provider = providers.get(tableKey);
            Assert.assertNotNull("no provider registered for " + tableKey, provider);
            return provider;
        }
    }


    /**
     * A {@link PopulatedProvider} that supports subscriptions, so that listener delivery can be exercised. Keys added
     * after a subscriber attaches are pushed to it.
     */
    private static final class SubscribableProvider extends AbstractTableLocationProvider {

        /**
         * Creates an empty subscribable provider for the given table.
         *
         * @param tableKey the table this provider serves
         */
        private SubscribableProvider(@NotNull final TableKey tableKey) {
            super(tableKey, true, TableUpdateMode.ADD_REMOVE, TableUpdateMode.ADD_REMOVE);
        }

        /**
         * Adds a key, pushing it to any current subscriber.
         *
         * @param locationKey the key to add
         */
        private void addKey(@NotNull final TableLocationKey locationKey) {
            handleTableLocationKeyAdded(locationKey);
        }

        /** Activation is immediate: the key set is already in memory. */
        @Override
        protected void activateUnderlyingDataSource() {
            activationSuccessful(this);
        }

        /** Nothing to tear down. */
        @Override
        protected void deactivateUnderlyingDataSource() {}

        /**
         * Matches the token handed to {@link #activationSuccessful}.
         *
         * @param token the subscription token
         * @param <T> the token type
         * @return whether the token is this provider
         */
        @Override
        protected <T> boolean matchSubscriptionToken(final T token) {
            return token == this;
        }

        /**
         * Never called: this provider is enumerated, not read.
         *
         * @param locationKey the key that would be built
         * @return never returns
         */
        @Override
        @NotNull
        protected TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
            throw new UnsupportedOperationException("test provider is enumerated only");
        }

        /** Does nothing: keys arrive through {@link #addKey}. */
        @Override
        public void refresh() {}
    }

}
