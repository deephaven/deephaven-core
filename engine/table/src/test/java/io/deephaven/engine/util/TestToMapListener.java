/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;

public class TestToMapListener extends RefreshingTableTestCase {
    public void testToMap() {
        final QueryTable source = testRefreshingTable(
                i(2, 4, 6, 8).toTracking(),
                col("Sentinel", "A", "B", "C", "D"),
                col("Sentinel2", "H", "I", "J", "K"));
        TableTools.show(source);

        final ColumnSource<String> sentinelSource = source.getColumnSource("Sentinel");
        final ColumnSource<String> sentinel2Source = source.getColumnSource("Sentinel2");

        final ToMapListener<String, String> tml = ToMapListener.make(source, sentinelSource::get,
                sentinelSource::getPrev, sentinel2Source::get, sentinel2Source::getPrev);
        source.addUpdateListener(tml);

        assertEquals("H", tml.get("A"));
        assertEquals("I", tml.get("B"));
        assertEquals("J", tml.get("C"));
        assertEquals("K", tml.get("D"));
        assertNull(tml.get("E"));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(source, i(10), col("Sentinel", "E"), col("Sentinel2", "L"));
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

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(source, i(10), col("Sentinel", "E"), col("Sentinel2", "M"));
            removeRows(source, i(2));
            source.notifyListeners(i(), i(2), i(10));
        });

        assertNull(tml.get("A"));
        assertEquals("I", tml.get("B"));
        assertEquals("J", tml.get("C"));
        assertEquals("K", tml.get("D"));
        assertEquals("M", tml.get("E"));
    }
}
