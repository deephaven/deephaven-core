//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Verifies that {@link FilteredTableDataService}'s {@code getTableLocationKeys} enumeration applies the service's
 * {@code locationKeyFilter}, so it exposes the same filtered set as subscription delivery and the point lookups.
 */
public class TestFilteredTableDataService extends RefreshingTableTestCase {

    /**
     * Enumerating locations through the filtered provider hides keys the {@code locationKeyFilter} rejects, matching
     * {@code hasTableLocationKey} / {@code getTableLocationIfPresent} / subscription delivery rather than passing the
     * unfiltered underlying set through.
     */
    @Test
    public void testGetTableLocationKeysAppliesFilter() {
        final TableLocationKey a = keyFor("A");
        final TableLocationKey b = keyFor("B");
        final TableLocationKey c = keyFor("C");

        final PopulatedProvider underlying = new PopulatedProvider();
        underlying.addKey(a);
        underlying.addKey(b);
        underlying.addKey(c);

        // Accept everything except C.
        final FilteredTableDataService.LocationKeyFilter acceptsAllButC = key -> !key.equals(c);
        final FilteredTableDataService filtered =
                new FilteredTableDataService(new FixedProviderService(underlying), acceptsAllButC);

        final Set<ImmutableTableLocationKey> visible = new HashSet<>(
                filtered.getTableLocationProvider(StandaloneTableKey.getInstance()).getTableLocationKeys());

        Assert.assertEquals(Set.of(a, b), visible);
    }

    /**
     * A single-partition location key for the given value.
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

        private PopulatedProvider() {
            super(false, TableUpdateMode.ADD_REMOVE, TableUpdateMode.ADD_REMOVE);
        }

        /**
         * Add a key to this provider's set.
         *
         * @param locationKey the key to add
         */
        private void addKey(@NotNull final TableLocationKey locationKey) {
            handleTableLocationKeyAdded(locationKey);
        }

        /** This provider is enumerated, not read, so no location is ever built. */
        @Override
        @NotNull
        protected TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
            throw new UnsupportedOperationException("test provider is enumerated only");
        }

        /** The key set is fixed at construction; nothing to refresh. */
        @Override
        public void refresh() {}
    }

    /**
     * A minimal {@link AbstractTableDataService} that always hands back one fixed provider.
     */
    private static final class FixedProviderService extends AbstractTableDataService {

        private final TableLocationProvider provider;

        private FixedProviderService(@NotNull final TableLocationProvider provider) {
            super("fixedProviderService");
            this.provider = provider;
        }

        /** Return the fixed provider regardless of key. */
        @Override
        @NotNull
        protected TableLocationProvider makeTableLocationProvider(@NotNull final TableKey tableKey) {
            return provider;
        }
    }
}
