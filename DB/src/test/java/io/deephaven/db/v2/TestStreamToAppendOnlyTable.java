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

public class TestStreamToAppendOnlyTable {
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
    public void testSimple() {
        final QueryTable streamTable = TstUtils.testRefreshingTable(i(), intCol("I"), doubleCol("D"), dateTimeCol("DT"), col("B", new Boolean[0]));
        streamTable.setAttribute(Table.STREAM_TABLE_ATTRIBUTE, true);

        final Table appendOnly = StreamTableToAppendOnlyTable.streamToAppendOnlyTable(streamTable);

        assertTableEquals(streamTable, appendOnly);
        TestCase.assertEquals(true, appendOnly.getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE));
        TestCase.assertTrue(appendOnly.isFlat());

        final DBDateTime dt1 = DBTimeUtils.convertDateTime("2021-08-11T8:20:00 NY");
        final DBDateTime dt2 = DBTimeUtils.convertDateTime("2021-08-11T8:21:00 NY");

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(streamTable, i(7), intCol("I", 1), doubleCol("D", Math.PI), dateTimeCol("DT", dt1), col("B", true));
            streamTable.notifyListeners(i(7), i(), i());
        });

        assertTableEquals(TableTools.newTable(intCol("I", 1), doubleCol("D", Math.PI), dateTimeCol("DT", dt1), col("B", true)), appendOnly);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(streamTable, i(7), intCol("I", 2), doubleCol("D", Math.E), dateTimeCol("DT", dt2), col("B", false));
            streamTable.notifyListeners(i(7), i(), i());
        });
        assertTableEquals(TableTools.newTable(intCol("I", 1, 2), doubleCol("D", Math.PI, Math.E), dateTimeCol("DT", dt1, dt2), col("B", true, false)), appendOnly);

    }

}
