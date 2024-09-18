//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.TestClock;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.DateGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.time.DateTimeUtils;

import java.lang.ref.WeakReference;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

public class TestTimeSeriesFilter extends RefreshingTableTestCase {
    public void testSimple() {
        Instant[] times = new Instant[10];

        final long startTime = System.currentTimeMillis() - (10 * times.length);
        for (int ii = 0; ii < times.length; ++ii) {
            times[ii] = DateTimeUtils.epochNanosToInstant((startTime + (ii * 1000)) * 1000000L);
        }

        Table source = TableTools.newTable(TableTools.col("Timestamp", times));
        TableTools.show(source);

        final TestClock testClock = new TestClock().setMillis(startTime);

        final TimeSeriesFilter timeSeriesFilter =
                new TimeSeriesFilter("Timestamp", DateTimeUtils.parseDurationNanos("PT00:00:05"), false, testClock);
        Table filtered = source.where(timeSeriesFilter);

        TableTools.show(filtered);
        assertEquals(10, filtered.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.addMillis(5000);
            timeSeriesFilter.runForUnitTests();
        });

        TableTools.show(filtered);
        assertEquals(10, filtered.size());

        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.addMillis(5000);
            timeSeriesFilter.runForUnitTests();
        });

        System.out.println(testClock);

        TableTools.show(filtered);
        assertEquals(5, filtered.size());

        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.addMillis(2000);
            timeSeriesFilter.runForUnitTests();
        });

        TableTools.show(filtered);
        assertEquals(3, filtered.size());
    }

    public void testIncremental() throws ParseException {
        Random random = new Random(0);

        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        ColumnInfo<?, ?>[] columnInfo;
        int size = 100;
        final Date startDate = format.parse("2015-03-23");
        Date endDate = format.parse("2015-03-24");
        final QueryTable table = getTable(size, random, columnInfo = initColumnInfos(new String[] {"Date", "C1"},
                new DateGenerator(startDate, endDate),
                new IntGenerator(1, 100)));

        final TestClock testClock = new TestClock().setMillis(startDate.getTime());

        final TimeSeriesFilter unitTestTimeSeriesFilter =
                new TimeSeriesFilter("Date", DateTimeUtils.parseDurationNanos("PT01:00:00"), false, testClock);
        final ArrayList<WeakReference<TimeSeriesFilter>> filtersToRefresh = new ArrayList<>();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> {
                    TimeSeriesFilter unitTestTimeSeriesFilter1 = unitTestTimeSeriesFilter.copy();
                    filtersToRefresh.add(new WeakReference<>(unitTestTimeSeriesFilter1));
                    return updateGraph.exclusiveLock().computeLocked(
                            () -> table.update("Date=DateTimeUtils.epochNanosToInstant(Date.getTime() * 1000000L)")
                                    .where(unitTestTimeSeriesFilter1));
                }),
        };


        int updatesPerTick = 3;
        for (int ii = 0; ii < 24 * (updatesPerTick + 1); ++ii) {
            if (ii % (updatesPerTick + 1) > 0) {
                simulateShiftAwareStep(size, random, table, columnInfo, en);
            } else {
                updateGraph.runWithinUnitTestCycle(() -> {
                    testClock.addMillis(3600 * 1000);

                    final ArrayList<WeakReference<TimeSeriesFilter>> collectedRefs = new ArrayList<>();
                    for (WeakReference<TimeSeriesFilter> ref : filtersToRefresh) {
                        final TimeSeriesFilter refreshFilter = ref.get();
                        if (refreshFilter == null) {
                            collectedRefs.add(ref);
                        } else {
                            refreshFilter.runForUnitTests();
                        }
                    }
                    filtersToRefresh.removeAll(collectedRefs);
                });
                TstUtils.validate("time update " + ii, en);
            }
        }
    }

    // TODO: test in a sequence of filters, with a dynamic where filter in front of us
    // TODO: test inverted filters
    // TODO: test actual modifications and additions to the table
    // TODO: test when nothing actually changes from the window check perspective
    // TODO: test that we are not causing more refiltering than necessary (with some counting filters before and after)
}
