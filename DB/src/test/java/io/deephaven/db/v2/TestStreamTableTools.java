package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.assertTableEquals;
import static io.deephaven.db.v2.TstUtils.i;

public class TestStreamTableTools {
    @Before
    public void setUp() throws Exception {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testStreamToAppendOnlyTable() {
        final DBDateTime dt1 = DBTimeUtils.convertDateTime("2021-08-11T8:20:00 NY");
        final DBDateTime dt2 = DBTimeUtils.convertDateTime("2021-08-11T8:21:00 NY");
        final DBDateTime dt3 = DBTimeUtils.convertDateTime("2021-08-11T11:22:00 NY");

        final QueryTable streamTable = TstUtils.testRefreshingTable(i(1), intCol("I", 7),
                doubleCol("D", Double.NEGATIVE_INFINITY), dateTimeCol("DT", dt1), col("B", Boolean.TRUE));
        streamTable.setAttribute(Table.STREAM_TABLE_ATTRIBUTE, true);

        final Table appendOnly = StreamTableTools.streamToAppendOnlyTable(streamTable);

        assertTableEquals(streamTable, appendOnly);
        TestCase.assertEquals(true, appendOnly.getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE));
        TestCase.assertTrue(appendOnly.isFlat());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(streamTable, i(7), intCol("I", 1), doubleCol("D", Math.PI), dateTimeCol("DT", dt2),
                    col("B", true));
            streamTable.notifyListeners(i(7), i(), i());
        });

        assertTableEquals(TableTools.newTable(intCol("I", 7, 1), doubleCol("D", Double.NEGATIVE_INFINITY, Math.PI),
                dateTimeCol("DT", dt1, dt2), col("B", true, true)), appendOnly);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
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
