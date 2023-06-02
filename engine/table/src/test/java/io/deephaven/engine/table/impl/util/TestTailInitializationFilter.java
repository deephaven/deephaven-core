/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.sources.InstantTestSource;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.TstUtils;

import java.time.Instant;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;

public class TestTailInitializationFilter extends RefreshingTableTestCase {
    public void testSimple() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 99);
        builder.appendRange(1000, 1099);
        final long[] data = new long[200];
        final Instant baseTime = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
        final Instant baseTime2 = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
        for (int ii = 0; ii < 100; ii++) {
            data[ii] = DateTimeUtils.epochNanos(baseTime) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
            data[100 + ii] = DateTimeUtils.epochNanos(baseTime2) + (DateTimeUtils.secondsToNanos(60) * (ii / 2));
        }
        final Instant threshold1 = DateTimeUtils.epochNanosToInstant(data[99] - DateTimeUtils.secondsToNanos(600));
        final Instant threshold2 = DateTimeUtils.epochNanosToInstant(data[199] - DateTimeUtils.secondsToNanos(600));

        final QueryTable input = TstUtils.testRefreshingTable(builder.build().toTracking(),
                ColumnHolder.getInstantColumnHolder("Timestamp", false, data));
        final Table filtered = TailInitializationFilter.mostRecent(input, "Timestamp", "PT00:10:00");
        TableTools.showWithRowSet(filtered);
        assertEquals(44, filtered.size());

        final Table slice0_100_filtered = input.slice(0, 100).where("Timestamp >= '" + threshold1 + "'");
        final Table slice100_200_filtered = input.slice(100, 200).where("Timestamp >= '" + threshold2 + "'");
        final Table expected = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> TableTools.merge(slice0_100_filtered, slice100_200_filtered));
        assertTableEquals(filtered, expected);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Instant[] data2 = new Instant[4];
            data2[0] = DateTimeUtils.parseInstant("2020-08-20T06:00:00 NY");
            data2[1] = DateTimeUtils.parseInstant("2020-08-20T06:30:00 NY");
            data2[0] = DateTimeUtils.parseInstant("2020-08-20T07:00:00 NY");
            data2[1] = DateTimeUtils.parseInstant("2020-08-20T08:30:00 NY");
            final RowSet newRowSet = RowSetFactory.fromKeys(100, 101, 1100, 1101);
            input.getRowSet().writableCast().insert(newRowSet);
            ((InstantTestSource) input.<Instant>getColumnSource("Timestamp")).add(newRowSet, data2);
            input.notifyListeners(newRowSet, TstUtils.i(), TstUtils.i());
        });

        final Table slice100_102 = input.slice(100, 102);
        final Table slice102_202_filtered = input.slice(102, 202).where("Timestamp >= '" + threshold2 + "'");
        final Table slice202_204 = input.slice(202, 204);
        final Table expected2 = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(
                () -> TableTools.merge(slice0_100_filtered, slice100_102, slice102_202_filtered, slice202_204));
        assertTableEquals(filtered, expected2);
    }
}
