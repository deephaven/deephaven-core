package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.LiveTableTestCase;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.sources.DateTimeTreeMapSource;

public class TestTailInitializationFilter extends LiveTableTestCase {
    public void testSimple() {
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        builder.appendRange(0, 99);
        builder.appendRange(1000, 1099);
        final long[] data = new long[200];
        final DBDateTime baseTime = DBTimeUtils.convertDateTime("2020-08-20T07:00:00 NY");
        final DBDateTime baseTime2 = DBTimeUtils.convertDateTime("2020-08-20T06:00:00 NY");
        for (int ii = 0; ii < 100; ii++) {
            data[ii] = baseTime.getNanos() + (DBTimeUtils.secondsToNanos(60) * (ii / 2));
            data[100 + ii] = baseTime2.getNanos() + (DBTimeUtils.secondsToNanos(60) * (ii / 2));
        }
        final DBDateTime threshold1 = new DBDateTime(data[99] - DBTimeUtils.secondsToNanos(600));
        final DBDateTime threshold2 = new DBDateTime(data[199] - DBTimeUtils.secondsToNanos(600));

        final QueryTable input = TstUtils.testRefreshingTable(builder.getIndex(),
            ColumnHolder.getDateTimeColumnHolder("Timestamp", false, data));
        final Table filtered = TailInitializationFilter.mostRecent(input, "Timestamp", "00:10:00");
        TableTools.showWithIndex(filtered);
        assertEquals(44, filtered.size());

        final Table slice0_100_filtered =
            input.slice(0, 100).where("Timestamp >= '" + threshold1 + "'");
        final Table slice100_200_filtered =
            input.slice(100, 200).where("Timestamp >= '" + threshold2 + "'");
        final Table expected = LiveTableMonitor.DEFAULT.sharedLock()
            .computeLocked(() -> TableTools.merge(slice0_100_filtered, slice100_200_filtered));
        assertEquals("", TableTools.diff(filtered, expected, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final DBDateTime[] data2 = new DBDateTime[4];
            data2[0] = DBTimeUtils.convertDateTime("2020-08-20T06:00:00 NY");
            data2[1] = DBTimeUtils.convertDateTime("2020-08-20T06:30:00 NY");
            data2[0] = DBTimeUtils.convertDateTime("2020-08-20T07:00:00 NY");
            data2[1] = DBTimeUtils.convertDateTime("2020-08-20T08:30:00 NY");
            final Index newIndex = Index.FACTORY.getIndexByValues(100, 101, 1100, 1101);
            input.getIndex().insert(newIndex);
            ((DateTimeTreeMapSource) input.getColumnSource("Timestamp")).add(newIndex, data2);
            input.notifyListeners(newIndex, TstUtils.i(), TstUtils.i());
        });

        final Table slice100_102 = input.slice(100, 102);
        final Table slice102_202_filtered =
            input.slice(102, 202).where("Timestamp >= '" + threshold2 + "'");
        final Table slice202_204 = input.slice(202, 204);
        final Table expected2 = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> TableTools
            .merge(slice0_100_filtered, slice100_102, slice102_202_filtered, slice202_204));
        assertEquals("", TableTools.diff(filtered, expected2, 10));

    }
}
