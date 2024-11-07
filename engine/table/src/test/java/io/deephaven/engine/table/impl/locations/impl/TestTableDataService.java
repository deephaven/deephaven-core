//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.DummyTableLocation;
import io.deephaven.engine.table.impl.TableUpdateMode;
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
            final FilteredTableDataService.LocationKeyFilter dummyFilter = (tlk) -> true;
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
    }
}
