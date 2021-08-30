package io.deephaven.db.util;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.ColumnSource;

import static io.deephaven.db.v2.TstUtils.c;
import static io.deephaven.db.v2.TstUtils.i;

public class TestToMapListener extends LiveTableTestCase {
    public void testToMap() {
        final QueryTable source = TstUtils.testRefreshingTable(
            i(2, 4, 6, 8),
            TstUtils.c("Sentinel", "A", "B", "C", "D"),
            TstUtils.c("Sentinel2", "H", "I", "J", "K"));
        io.deephaven.db.tables.utils.TableTools.show(source);

        final ColumnSource<String> sentinelSource = source.getColumnSource("Sentinel");
        final ColumnSource<String> sentinel2Source = source.getColumnSource("Sentinel2");

        final ToMapListener<String, String> tml = ToMapListener.make(source, sentinelSource::get,
            sentinelSource::getPrev, sentinel2Source::get, sentinel2Source::getPrev);
        source.listenForUpdates(tml);

        assertEquals("H", tml.get("A"));
        assertEquals("I", tml.get("B"));
        assertEquals("J", tml.get("C"));
        assertEquals("K", tml.get("D"));
        assertNull(tml.get("E"));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(10), TstUtils.c("Sentinel", "E"), c("Sentinel2", "L"));
            source.notifyListeners(i(10), i(), i());

            assertEquals("H", tml.get("A"));
            assertEquals("I", tml.get("B"));
            assertEquals("J", tml.get("C"));
            assertEquals("K", tml.get("D"));
            assertNull(tml.get("E"));
        });

        assertEquals("H", tml.get("A"));
        assertEquals("I", tml.get("B"));
        assertEquals("J", tml.get("C"));
        assertEquals("K", tml.get("D"));
        assertEquals("L", tml.get("E"));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(10), TstUtils.c("Sentinel", "E"), c("Sentinel2", "M"));
            TstUtils.removeRows(source, i(2));
            source.notifyListeners(i(), i(2), i(10));
        });

        assertNull(tml.get("A"));
        assertEquals("I", tml.get("B"));
        assertEquals("J", tml.get("C"));
        assertEquals("K", tml.get("D"));
        assertEquals("M", tml.get("E"));
    }
}
