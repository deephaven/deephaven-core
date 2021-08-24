/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.EvalNugget;
import io.deephaven.db.v2.LiveTableTestCase;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.select.TimeSeriesFilter;

import java.lang.ref.WeakReference;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.initColumnInfos;

public class TestTimeSeriesFilter extends LiveTableTestCase {
    public void testSimple() {
        DBDateTime[] times = new DBDateTime[10];

        final long startTime = System.currentTimeMillis() - (10 * times.length);
        for (int ii = 0; ii < times.length; ++ii) {
            times[ii] = new DBDateTime((startTime + (ii * 1000)) * 1000000L);
        }

        Table source = TableTools.newTable(TableTools.col("Timestamp", times));
        io.deephaven.db.tables.utils.TableTools.show(source);

        UnitTestTimeSeriesFilter timeSeriesFilter =
            new UnitTestTimeSeriesFilter(startTime, "Timestamp", "00:00:05");
        Table filtered = source.where(timeSeriesFilter);

        io.deephaven.db.tables.utils.TableTools.show(filtered);
        assertEquals(10, filtered.size());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            timeSeriesFilter.incrementNow(5000);
            timeSeriesFilter.refresh();
        });

        io.deephaven.db.tables.utils.TableTools.show(filtered);
        assertEquals(10, filtered.size());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            timeSeriesFilter.incrementNow(5000);
            timeSeriesFilter.refresh();
        });

        io.deephaven.db.tables.utils.TableTools.show(filtered);
        assertEquals(5, filtered.size());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            timeSeriesFilter.incrementNow(2000);
            timeSeriesFilter.refresh();
        });

        io.deephaven.db.tables.utils.TableTools.show(filtered);
        assertEquals(3, filtered.size());
    }

    public void testIncremental() throws ParseException {
        Random random = new Random(0);

        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        TstUtils.ColumnInfo[] columnInfo;
        int size = 100;
        final Date startDate = format.parse("2015-03-23");
        Date endDate = format.parse("2015-03-24");
        final QueryTable table = getTable(size, random,
            columnInfo = initColumnInfos(new String[] {"Date", "C1"},
                new TstUtils.DateGenerator(startDate, endDate),
                new TstUtils.IntGenerator(1, 100)));

        final UnitTestTimeSeriesFilter unitTestTimeSeriesFilter =
            new UnitTestTimeSeriesFilter(startDate.getTime(), "Date", "01:00:00");
        final ArrayList<WeakReference<UnitTestTimeSeriesFilter>> filtersToRefresh =
            new ArrayList<>();

        EvalNugget en[] = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        UnitTestTimeSeriesFilter unitTestTimeSeriesFilter1 =
                            new UnitTestTimeSeriesFilter(unitTestTimeSeriesFilter);
                        filtersToRefresh.add(new WeakReference<>(unitTestTimeSeriesFilter1));
                        return LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(
                            () -> table.update("Date=new DBDateTime(Date.getTime() * 1000000L)")
                                .where(unitTestTimeSeriesFilter1));
                    }
                },
        };


        int updatesPerTick = 3;
        for (int ii = 0; ii < 24 * (updatesPerTick + 1); ++ii) {
            if (ii % (updatesPerTick + 1) > 0) {
                simulateShiftAwareStep(size, random, table, columnInfo, en);
            } else {
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    unitTestTimeSeriesFilter.incrementNow(3600 * 1000);

                    final ArrayList<WeakReference<UnitTestTimeSeriesFilter>> collectedRefs =
                        new ArrayList<>();
                    for (WeakReference<UnitTestTimeSeriesFilter> ref : filtersToRefresh) {
                        final UnitTestTimeSeriesFilter refreshFilter = ref.get();
                        if (refreshFilter == null) {
                            collectedRefs.add(ref);
                        } else {
                            refreshFilter.setNow(unitTestTimeSeriesFilter.getNowLong());
                            refreshFilter.refresh();
                        }
                    }
                    filtersToRefresh.removeAll(collectedRefs);
                });
                TstUtils.validate("time update " + ii, en);
            }
        }
    }

    private static class UnitTestTimeSeriesFilter extends TimeSeriesFilter {
        long now;

        public UnitTestTimeSeriesFilter(long startTime, String timestamp, String period) {
            super(timestamp, period);
            now = startTime;
        }

        public UnitTestTimeSeriesFilter(long startTime, String timestamp, long period) {
            super(timestamp, period);
            now = startTime;
        }

        public UnitTestTimeSeriesFilter(UnitTestTimeSeriesFilter other) {
            this(other.now, other.columnName, other.nanos);
        }

        @Override
        protected DBDateTime getNow() {
            return new DBDateTime(now * 1000000L);
        }

        long getNowLong() {
            return now;
        }

        void incrementNow(long increment) {
            now += increment;
        }

        void setNow(long newValue) {
            now = newValue;
        }
    }

}
