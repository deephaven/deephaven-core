//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.DateGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.util.TestClock;
import io.deephaven.time.DateTimeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;

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
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT00:00:05").clock(testClock).build();
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

    public void testInverted() {
        Instant[] times = new Instant[10];

        final long startTime = System.currentTimeMillis() - (10 * times.length);
        for (int ii = 0; ii < times.length; ++ii) {
            times[ii] = DateTimeUtils.epochNanosToInstant((startTime + (ii * 1000)) * 1000000L);
        }

        Table source = TableTools.newTable(TableTools.col("Timestamp", times));
        TableTools.show(source);

        final TestClock testClock = new TestClock().setMillis(startTime);

        final TimeSeriesFilter timeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT00:00:05").clock(testClock).invert(true)
                        .build();
        Table filtered = source.where(timeSeriesFilter);

        TableTools.show(filtered);
        assertEquals(0, filtered.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.addMillis(5000);
            timeSeriesFilter.runForUnitTests();
        });

        TableTools.show(filtered);
        assertEquals(0, filtered.size());

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
        assertEquals(7, filtered.size());
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

        final TimeSeriesFilter inclusionFilter =
                TimeSeriesFilter.newBuilder().columnName("Date").period("PT01:00:00").clock(testClock).invert(false)
                        .build();
        final TimeSeriesFilter exclusionFilter =
                TimeSeriesFilter.newBuilder().columnName("Date").period("PT01:00:00").clock(testClock).invert(true)
                        .build();
        final ArrayList<WeakReference<TimeSeriesFilter>> filtersToRefresh = new ArrayList<>();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        EvalNugget[] en = makeNuggets(table, inclusionFilter, filtersToRefresh, updateGraph, exclusionFilter);


        int updatesPerTick = 3;
        for (int ii = 0; ii < 24 * (updatesPerTick + 1); ++ii) {
            if (ii % (updatesPerTick + 1) > 0) {
                simulateShiftAwareStep(size, random, table, columnInfo, en);
            } else {
                updateGraph.runWithinUnitTestCycle(() -> refreshFilters(testClock, filtersToRefresh, 3600 * 1000));
                TstUtils.validate("time update " + ii, en);
            }
        }
    }

    public void testIncremental2() throws ParseException {
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
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final TimeSeriesFilter inclusionFilter =
                TimeSeriesFilter.newBuilder().columnName("Date").period("PT01:00:00").clock(testClock).invert(false)
                        .build();
        final TimeSeriesFilter exclusionFilter =
                TimeSeriesFilter.newBuilder().columnName("Date").period("PT01:00:00").clock(testClock).invert(true)
                        .build();
        final ArrayList<WeakReference<TimeSeriesFilter>> filtersToRefresh = new ArrayList<>();

        EvalNugget[] en = makeNuggets(table, inclusionFilter, filtersToRefresh, updateGraph, exclusionFilter);

        final MutableInt advanced = new MutableInt(0);
        while (advanced.intValue() < 24 * 3600 * 1000) {
            updateGraph.runWithinUnitTestCycle(() -> {
                if (random.nextBoolean()) {
                    final int toAdvance = random.nextInt(1800 * 1000);
                    refreshFilters(testClock, filtersToRefresh, toAdvance);
                    advanced.add(toAdvance);
                }
                if (random.nextBoolean()) {
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, size,
                            random, table, columnInfo);
                }
            });
            TstUtils.validate("incremental2", en);
        }
    }

    private static EvalNugget @NotNull [] makeNuggets(QueryTable table, TimeSeriesFilter inclusionFilter,
            ArrayList<WeakReference<TimeSeriesFilter>> filtersToRefresh, ControlledUpdateGraph updateGraph,
            TimeSeriesFilter exclusionFilter) {
        final Table withInstant = table.update("Date=DateTimeUtils.epochNanosToInstant(Date.getTime() * 1000000L)");
        return new EvalNugget[] {
                EvalNugget.from(() -> {
                    final TimeSeriesFilter inclusionCopy = inclusionFilter.copy();
                    filtersToRefresh.add(new WeakReference<>(inclusionCopy));
                    return updateGraph.exclusiveLock().computeLocked(() -> withInstant.where(inclusionCopy));
                }),
                EvalNugget.from(() -> {
                    final TimeSeriesFilter exclusionCopy = exclusionFilter.copy();
                    filtersToRefresh.add(new WeakReference<>(exclusionCopy));
                    return updateGraph.exclusiveLock().computeLocked(() -> withInstant.where(exclusionCopy));
                }),
        };
    }

    private static void refreshFilters(final TestClock testClock,
            final List<WeakReference<TimeSeriesFilter>> filtersToRefresh, final int millisToAdvance) {
        testClock.addMillis(millisToAdvance);

        final List<WeakReference<TimeSeriesFilter>> collectedRefs = new ArrayList<>();
        for (WeakReference<TimeSeriesFilter> ref : filtersToRefresh) {
            final TimeSeriesFilter refreshFilter = ref.get();
            if (refreshFilter == null) {
                collectedRefs.add(ref);
            } else {
                refreshFilter.runForUnitTests();
            }
        }
        filtersToRefresh.removeAll(collectedRefs);
    }

    private static class CountingFilter extends WhereFilterImpl {
        final AtomicLong count;

        CountingFilter() {
            count = new AtomicLong(0);
        }

        CountingFilter(final AtomicLong count) {
            this.count = count;
        }

        @Override
        public List<String> getColumns() {
            return List.of("Sentinel");
        }

        @Override
        public List<String> getColumnArrays() {
            return List.of();
        }

        @Override
        public void init(@NotNull TableDefinition tableDefinition) {

        }

        @Override
        public @NotNull WritableRowSet filter(@NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table,
                boolean usePrev) {
            count.addAndGet(selection.size());
            return selection.copy();
        }

        @Override
        public boolean isSimpleFilter() {
            return true;
        }

        @Override
        public void setRecomputeListener(RecomputeListener result) {

        }

        @Override
        public WhereFilter copy() {
            return new CountingFilter(count);
        }
    }

    public void testFilterSequence() {
        final long start = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2024-09-18T21:29:00 NY"));
        final QueryTable source = testRefreshingTable(i().toTracking(), longCol("Timestamp"), intCol("Sentinel"));
        final TestClock testClock = new TestClock().setNanos(start);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final CountingFilter cfi1 = new CountingFilter();
        final CountingFilter cfi2 = new CountingFilter();
        final CountingFilter cfe1 = new CountingFilter();
        final CountingFilter cfe2 = new CountingFilter();

        final TimeSeriesFilter inclusionFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT01:00:00").clock(testClock)
                        .invert(false).build();
        final TimeSeriesFilter exclusionFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT01:00:00").clock(testClock).invert(true)
                        .build();

        final Table inclusion = source.where(Filter.and(cfi1, inclusionFilter, cfi2));
        final Table exclusion = source.where(Filter.and(cfe1, exclusionFilter, cfe2));

        assertEquals(0, cfi1.count.intValue());
        assertEquals(0, cfi2.count.intValue());
        assertEquals(0, cfe1.count.intValue());
        assertEquals(0, cfe2.count.intValue());

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(10), longCol("Timestamp", start), intCol("Sentinel", 10));
            source.notifyListeners(i(10), i(), i());
            inclusionFilter.runForUnitTests();
            exclusionFilter.runForUnitTests();
        });

        TableTools.show(inclusion);
        TableTools.show(exclusion);

        assertEquals(1, cfi1.count.intValue());
        assertEquals(1, cfi2.count.intValue());
        assertEquals(1, cfe1.count.intValue());
        assertEquals(0, cfe2.count.intValue());


        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(10, 20, 30),
                    longCol("Timestamp", start, start + 300_000_000_000L, start + 4200_000_000_000L),
                    intCol("Sentinel", 10, 20, 30));
            source.notifyListeners(i(20, 30), i(), i(10));
            inclusionFilter.runForUnitTests();
            exclusionFilter.runForUnitTests();
        });

        assertEquals(4, cfi1.count.intValue());
        assertEquals(4, cfi2.count.intValue());
        assertEquals(4, cfe1.count.intValue());
        assertEquals(0, cfe2.count.intValue());

        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.addMillis(3_700_000L);
            inclusionFilter.runForUnitTests();
            exclusionFilter.runForUnitTests();
        });

        assertEquals(5, cfi1.count.intValue());
        assertEquals(4, cfi2.count.intValue());
        assertEquals(5, cfe1.count.intValue());
        assertEquals(1, cfe2.count.intValue());

        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.addMillis(300_000L);
            inclusionFilter.runForUnitTests();
            exclusionFilter.runForUnitTests();
        });

        assertEquals(6, cfi1.count.intValue());
        assertEquals(4, cfi2.count.intValue());
        assertEquals(6, cfe1.count.intValue());
        assertEquals(2, cfe2.count.intValue());
    }
}
