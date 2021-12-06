package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.util.TableTools;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.table.impl.TstUtils.assertTableEquals;
import static io.deephaven.engine.table.impl.TstUtils.i;

public class TestStreamTableTools {
    @Before
    public void setUp() throws Exception {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testStreamToAppendOnlyTable() {
        final DateTime dt1 = DateTimeUtils.convertDateTime("2021-08-11T8:20:00 NY");
        final DateTime dt2 = DateTimeUtils.convertDateTime("2021-08-11T8:21:00 NY");
        final DateTime dt3 = DateTimeUtils.convertDateTime("2021-08-11T11:22:00 NY");

        final QueryTable streamTable = TstUtils.testRefreshingTable(i(1).toTracking(), intCol("I", 7),
                doubleCol("D", Double.NEGATIVE_INFINITY), dateTimeCol("DT", dt1), col("B", Boolean.TRUE));
        streamTable.setAttribute(Table.STREAM_TABLE_ATTRIBUTE, true);

        final Table appendOnly = StreamTableTools.streamToAppendOnlyTable(streamTable);

        assertTableEquals(streamTable, appendOnly);
        TestCase.assertEquals(true, appendOnly.getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE));
        TestCase.assertTrue(appendOnly.isFlat());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(streamTable, i(7), intCol("I", 1), doubleCol("D", Math.PI), dateTimeCol("DT", dt2),
                    col("B", true));
            streamTable.notifyListeners(i(7), i(), i());
        });

        assertTableEquals(TableTools.newTable(intCol("I", 7, 1), doubleCol("D", Double.NEGATIVE_INFINITY, Math.PI),
                dateTimeCol("DT", dt1, dt2), col("B", true, true)), appendOnly);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(streamTable, i(7), intCol("I", 2), doubleCol("D", Math.E), dateTimeCol("DT", dt3),
                    col("B", false));
            streamTable.notifyListeners(i(7), i(), i());
        });
        assertTableEquals(
                TableTools.newTable(intCol("I", 7, 1, 2), doubleCol("D", Double.NEGATIVE_INFINITY, Math.PI, Math.E),
                        dateTimeCol("DT", dt1, dt2, dt3), col("B", true, true, false)),
                appendOnly);

    }

}
