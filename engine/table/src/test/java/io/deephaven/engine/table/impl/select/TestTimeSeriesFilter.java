//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WouldMatchPair;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.DateGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.util.TestClock;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;

public class TestTimeSeriesFilter extends RefreshingTableTestCase {

    private static final long TIMEOUT_SECONDS = 30;

    @Test
    public void testSimple() {
        Instant[] times = new Instant[10];

        final long startTime = System.currentTimeMillis() - (10 * times.length);
        for (int ii = 0; ii < times.length; ++ii) {
            times[ii] = DateTimeUtils.epochNanosToInstant((startTime + (ii * 1000)) * 1000000L);
        }

        Table source = TableTools.newTable(col("Timestamp", times));
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

    @Test
    public void testInverted() {
        Instant[] times = new Instant[10];

        final long startTime = System.currentTimeMillis() - (10 * times.length);
        for (int ii = 0; ii < times.length; ++ii) {
            times[ii] = DateTimeUtils.epochNanosToInstant((startTime + (ii * 1000)) * 1000000L);
        }

        Table source = TableTools.newTable(col("Timestamp", times));
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

    @Test
    public void testIncremental() throws ParseException {
        for (int seed = 0; seed < 5; ++seed) {
            testIncremental(seed, 100, 24);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void testIncremental(final int seed, final int size, final int maxSteps) throws ParseException {
        Random random = new Random(seed);

        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        ColumnInfo<?, ?>[] columnInfo;
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

        final int updatesPerTick = 3;
        for (int steps = 0; steps < maxSteps * (updatesPerTick + 1); ++steps) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("seed=" + seed + ", step=" + steps);
            }
            if (steps % (updatesPerTick + 1) > 0) {
                simulateShiftAwareStep(size, random, table, columnInfo, en);
            } else {
                updateGraph.runWithinUnitTestCycle(() -> refreshFilters(testClock, filtersToRefresh, 3600 * 1000));
                TstUtils.validate("time update " + steps, en);
            }
        }
    }

    @Test
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
                if (refreshFilter.tryRetainReference()) {
                    refreshFilter.runForUnitTests();
                    refreshFilter.dropReference();
                } else {
                    collectedRefs.add(ref);
                }
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

    @Test
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

    @Test
    public void testStatic() {
        final long start = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2025-07-08T21:29:00 NY"));

        final int size = 60;
        final Instant[] times = new Instant[size];
        final int[] sentinel = new int[size];

        for (int ii = 0; ii < times.length; ++ii) {
            sentinel[ii] = ii;
            times[ii] = DateTimeUtils.epochNanosToInstant(start + (ii * DateTimeUtils.MINUTE));
        }

        final QueryTable source =
                testTable(RowSetFactory.flat(size).toTracking(), col("Timestamp", times), intCol("Sentinel", sentinel));
        final TestClock testClock = new TestClock().setNanos(start + 30 * DateTimeUtils.MINUTE);

        final TimeSeriesFilter timeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).invert(true)
                        .build();
        final Table filtered = source.where(timeSeriesFilter);
        assertTrue(filtered.isRefreshing());
        assertEquals(25, filtered.size());

        testClock.setNanos(start + 120 * DateTimeUtils.MINUTE);
        final TimeSeriesFilter timeSeriesFilter2 =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).invert(true)
                        .build();
        final Table filtered2 = source.where(timeSeriesFilter2);
        assertFalse(filtered2.isRefreshing());
        assertEquals(size, filtered2.size());
    }

    @Test
    public void testComposed() {
        final long start = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2026-03-10T09:00:00 NY"));

        final TestClock testClock = new TestClock().setNanos(start + 30 * DateTimeUtils.MINUTE);

        final TimeSeriesFilter timeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).invert(true)
                        .build();

        final Filter disjunctiveOne = DisjunctiveFilter.of(timeSeriesFilter);
        assertTrue(disjunctiveOne instanceof TimeSeriesFilter);

        final Filter disjunctiveTwo =
                DisjunctiveFilter.of(timeSeriesFilter, WhereFilterFactory.getExpression("A in `Apple`"));
        assertTrue(disjunctiveTwo instanceof DisjunctiveFilter);

        final Filter conjunctiveOne = ConjunctiveFilter.of(timeSeriesFilter);
        assertTrue(conjunctiveOne instanceof TimeSeriesFilter);

        final Filter conjunctiveTwo =
                ConjunctiveFilter.of(timeSeriesFilter, WhereFilterFactory.getExpression("A in `Apple`"));
        assertTrue(conjunctiveTwo instanceof ConjunctiveFilter);

        final WhereFilter inverted = WhereFilterInvertedImpl.of(timeSeriesFilter);
        assertTrue(inverted instanceof WhereFilterInvertedImpl);
    }

    /**
     * A filter that asks for specific rows to be re-evaluated, rather than for all of them, also re-evaluates the match
     * column. A time series filter is the only filter that makes that request.
     */
    @Test
    public void testMatchWithRowSetRecomputeRequests() {
        final Instant[] times = new Instant[10];
        final long startMillis = System.currentTimeMillis() - (10 * times.length);
        for (int ii = 0; ii < times.length; ++ii) {
            times[ii] = DateTimeUtils.epochNanosToInstant((startMillis + (ii * 1000)) * 1000000L);
        }
        final Table source = TableTools.newTable(col("Timestamp", times));
        final TestClock clock = new TestClock().setMillis(startMillis);
        final TimeSeriesFilter filter = TimeSeriesFilter.newBuilder()
                .columnName("Timestamp").period("PT00:00:05").clock(clock).build();

        final Table result = source.wouldMatch(new WouldMatchPair("M", filter));
        assertEquals(10, result.where("M").size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            clock.addMillis(10_000);
            filter.runForUnitTests();
        });
        assertEquals(5, result.where("M").size());
    }

    /**
     * A caller may ask whether a filter is refreshing before the filter has seen its source table, for example to
     * decide how to apply it. The time series filter cannot know yet, so it answers conservatively, including through a
     * composed filter.
     */
    @Test
    public void testIsRefreshingBeforeBeginOperation() {
        // Rows are one minute apart. At 30 minutes, rows 0 through 24 are older than the five minute window; at 40
        // minutes, rows 0 through 34 are. The composed filter keeps only even rows.
        checkIsRefreshingBeforeBeginOperation(false, 17, 12, 0);
        checkIsRefreshingBeforeBeginOperation(true, 13, 18, 30);
    }

    private void checkIsRefreshingBeforeBeginOperation(
            final boolean invert,
            final int expectedSize,
            final int expectedSizeAfterTick,
            final int expectedStaticSize) {
        final long start = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2026-03-10T09:00:00 NY"));

        final int size = 60;
        final Instant[] times = new Instant[size];
        final int[] sentinel = new int[size];

        for (int ii = 0; ii < times.length; ++ii) {
            sentinel[ii] = ii;
            times[ii] = DateTimeUtils.epochNanosToInstant(start + (ii * DateTimeUtils.MINUTE));
        }

        final QueryTable source =
                testTable(RowSetFactory.flat(size).toTracking(), col("Timestamp", times), intCol("Sentinel", sentinel));
        final TestClock testClock = new TestClock().setNanos(start + 30 * DateTimeUtils.MINUTE);

        final TimeSeriesFilter timeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).invert(invert)
                        .build();
        assertTrue(timeSeriesFilter.isRefreshing());

        final WhereFilter composed =
                ConjunctiveFilter.of(timeSeriesFilter, WhereFilterFactory.getExpression("Sentinel % 2 == 0"));
        assertTrue(composed instanceof ConjunctiveFilter);
        assertTrue(composed.isRefreshing());

        final Table filtered = source.where(composed);
        assertTrue(filtered.isRefreshing());
        assertEquals(expectedSize, filtered.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.setNanos(start + 40 * DateTimeUtils.MINUTE);
            timeSeriesFilter.runForUnitTests();
        });
        assertEquals(expectedSizeAfterTick, filtered.size());

        // Every row is already outside the window, and the source is static, so once the filter has seen the source
        // it knows nothing will ever change.
        final TestClock lateClock = new TestClock().setNanos(start + 120 * DateTimeUtils.MINUTE);
        final TimeSeriesFilter staticTimeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(lateClock).invert(invert)
                        .build();
        final WhereFilter staticComposed =
                ConjunctiveFilter.of(staticTimeSeriesFilter, WhereFilterFactory.getExpression("Sentinel % 2 == 0"));
        assertTrue(staticTimeSeriesFilter.isRefreshing());
        assertTrue(staticComposed.isRefreshing());

        final Table staticFiltered = source.where(staticComposed);
        assertFalse(staticFiltered.isRefreshing());
        assertEquals(expectedStaticSize, staticFiltered.size());
        assertFalse(staticTimeSeriesFilter.isRefreshing());
        assertFalse(staticComposed.isRefreshing());
    }

    /**
     * A caller that may not initiate serial table operations, such as a gRPC handler, filters a refreshing table with a
     * time series filter, both directly and composed with another filter.
     */
    @Test
    public void testWhereWithoutLock() throws Exception {
        final long start = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2026-03-10T09:00:00 NY"));
        final QueryTable source = minuteTable(start, true);
        final TestClock testClock = new TestClock().setNanos(start + 30 * DateTimeUtils.MINUTE);

        final TimeSeriesFilter timeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).build();
        final Table filtered = callWithoutLock(() -> source.where(timeSeriesFilter));

        final TimeSeriesFilter composedTimeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).build();
        final WhereFilter composed =
                DisjunctiveFilter.of(composedTimeSeriesFilter, WhereFilterFactory.getExpression("Sentinel < 0"));
        final Table composedFiltered = callWithoutLock(() -> source.where(composed));

        // Rows are one minute apart. At 30 minutes, rows 0 through 24 are older than the five minute window; at 40
        // minutes, rows 0 through 34 are.
        assertTrue(filtered.isRefreshing());
        assertTrue(composedFiltered.isRefreshing());
        assertEquals(35, filtered.size());
        assertEquals(35, composedFiltered.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.setNanos(start + 40 * DateTimeUtils.MINUTE);
            timeSeriesFilter.runForUnitTests();
            composedTimeSeriesFilter.runForUnitTests();
        });
        assertEquals(25, filtered.size());
        assertEquals(25, composedFiltered.size());
    }

    /**
     * A caller that holds no lock filters a refreshing table with a time series filter during an update cycle, before
     * the table has been notified. The where reads previous values without waiting for the cycle, and its result then
     * follows the rows the cycle adds and the rows that later leave the window.
     */
    @Test
    public void testWhereWithoutLockDuringCycle() throws Exception {
        final long start = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2026-03-10T09:00:00 NY"));
        final QueryTable source = minuteTable(start, true);
        final TestClock testClock = new TestClock().setNanos(start + 30 * DateTimeUtils.MINUTE);
        final TimeSeriesFilter timeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).build();

        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ControlledUpdateGraph updateGraph = executionContext.getUpdateGraph().cast();
        final ExecutorService unlockedThread = Executors.newSingleThreadExecutor();
        final Table filtered;
        updateGraph.startCycleForUnitTests(false);
        try {
            addToTable(source, i(60, 61),
                    col("Timestamp", DateTimeUtils.epochNanosToInstant(start + 60 * DateTimeUtils.MINUTE),
                            DateTimeUtils.epochNanosToInstant(start + 61 * DateTimeUtils.MINUTE)),
                    intCol("Sentinel", 60, 61));
            // a where that waited for the update graph would not complete until the cycle does
            filtered = unlockedThread.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    return source.where(timeSeriesFilter);
                }
            }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            // At 30 minutes, rows 0 through 24 are older than the five minute window.
            assertEquals(35, filtered.size());
            source.notifyListeners(i(60, 61), i(), i());
        } finally {
            unlockedThread.shutdownNow();
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
        assertEquals(37, filtered.size());

        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.setNanos(start + 40 * DateTimeUtils.MINUTE);
            timeSeriesFilter.runForUnitTests();
        });
        // at 40 minutes, rows 0 through 34 are
        assertEquals(27, filtered.size());
    }

    /**
     * A caller that holds no lock filters a static table with a time series filter during an update cycle. The window
     * over the static table still refreshes, and has not yet notified, so the where reads previous values without
     * waiting for the cycle, and its result then follows the rows that leave the window.
     */
    @Test
    public void testWhereWithoutLockDuringCycleOnStaticTable() throws Exception {
        final long start = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2026-03-10T09:00:00 NY"));
        final QueryTable source = minuteTable(start, false);
        final TestClock testClock = new TestClock().setNanos(start + 30 * DateTimeUtils.MINUTE);
        final TimeSeriesFilter timeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).build();

        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ControlledUpdateGraph updateGraph = executionContext.getUpdateGraph().cast();
        final ExecutorService unlockedThread = Executors.newSingleThreadExecutor();
        final Table filtered;
        updateGraph.startCycleForUnitTests(false);
        try {
            // a where that waited for the update graph would not complete until the cycle does
            filtered = unlockedThread.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    return source.where(timeSeriesFilter);
                }
            }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            // At 30 minutes, rows 0 through 24 are older than the five minute window.
            assertEquals(35, filtered.size());
        } finally {
            unlockedThread.shutdownNow();
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }
        assertTrue(filtered.isRefreshing());

        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.setNanos(start + 40 * DateTimeUtils.MINUTE);
            timeSeriesFilter.runForUnitTests();
        });
        // at 40 minutes, rows 0 through 34 are
        assertEquals(25, filtered.size());
    }

    /**
     * A where that reads previous values during an update cycle cannot commit if the windowed table notifies on that
     * cycle before the where commits, because the filter would begin following the windowed table after the rows that
     * left the window were reported. The where reads again, and its result excludes those rows.
     */
    @Test
    public void testWhereRetriesWhenWindowChangesBeforeCommit() throws Exception {
        final long start = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2026-03-10T09:00:00 NY"));
        final QueryTable source = minuteTable(start, true);
        final TestClock testClock = new TestClock().setNanos(start + 30 * DateTimeUtils.MINUTE);
        final TimeSeriesFilter timeSeriesFilter =
                TimeSeriesFilter.newBuilder().columnName("Timestamp").period("PT5m").clock(testClock).build();

        // holds the first where attempt after it has read the source, and lets every later attempt through
        final AtomicBoolean armed = new AtomicBoolean(true);
        final CountDownLatch reached = new CountDownLatch(1);
        final CountDownLatch released = new CountDownLatch(1);
        final WhereFilter gate = new RowSetCapturingFilter() {
            @NotNull
            @Override
            public WritableRowSet filter(
                    @NotNull final RowSet selection,
                    @NotNull final RowSet fullSet,
                    @NotNull final Table table,
                    final boolean usePrev) {
                if (armed.getAndSet(false)) {
                    assertTrue("the first attempt reads previous values", usePrev);
                    reached.countDown();
                    try {
                        assertTrue(released.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                    } catch (final InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
                return super.filter(selection, fullSet, table, usePrev);
            }
        };

        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ControlledUpdateGraph updateGraph = executionContext.getUpdateGraph().cast();
        final ExecutorService unlockedThread = Executors.newSingleThreadExecutor();
        final Table filtered;
        updateGraph.startCycleForUnitTests(false);
        try {
            final Future<Table> result = unlockedThread.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    return source.where(Filter.and(timeSeriesFilter, gate));
                }
            });
            assertTrue(reached.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // time passes, and the windowed table reports the rows that left the window on this cycle
            testClock.setNanos(start + 40 * DateTimeUtils.MINUTE);
            timeSeriesFilter.runForUnitTests();
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.flushAllNormalNotificationsForUnitTests();

            released.countDown();
            filtered = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            released.countDown();
            unlockedThread.shutdownNow();
            updateGraph.completeCycleForUnitTests();
        }
        // at 40 minutes, rows 0 through 34 are older than the five minute window
        assertEquals(25, filtered.size());

        updateGraph.runWithinUnitTestCycle(() -> {
            testClock.setNanos(start + 50 * DateTimeUtils.MINUTE);
            timeSeriesFilter.runForUnitTests();
        });
        // and at 50 minutes, rows 0 through 44 are
        assertEquals(15, filtered.size());
    }

    /**
     * A table of 60 rows, one minute apart beginning at {@code start}, with a Sentinel column holding the row position.
     */
    private static QueryTable minuteTable(final long start, final boolean refreshing) {
        final int size = 60;
        final Instant[] times = new Instant[size];
        final int[] sentinel = new int[size];
        for (int ii = 0; ii < times.length; ++ii) {
            sentinel[ii] = ii;
            times[ii] = DateTimeUtils.epochNanosToInstant(start + (ii * DateTimeUtils.MINUTE));
        }
        return refreshing
                ? testRefreshingTable(RowSetFactory.flat(size).toTracking(), col("Timestamp", times),
                        intCol("Sentinel", sentinel))
                : testTable(RowSetFactory.flat(size).toTracking(), col("Timestamp", times),
                        intCol("Sentinel", sentinel));
    }

    /**
     * Call {@code operation} on a thread that holds neither update graph lock and may not initiate serial table
     * operations, as a gRPC handler does, and verify that the thread holds no lock once the operation returns or
     * throws.
     */
    private static <T> T callWithoutLock(final Callable<T> operation) throws Exception {
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final UpdateGraph updateGraph = executionContext.getUpdateGraph();
        final ExecutorService unlockedThread = Executors.newSingleThreadExecutor();
        try {
            return unlockedThread.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    assertFalse(updateGraph.serialTableOperationsSafe());
                    assertFalse(updateGraph.exclusiveLock().isHeldByCurrentThread());
                    assertFalse(updateGraph.sharedLock().isHeldByCurrentThread());
                    try {
                        return operation.call();
                    } finally {
                        assertFalse(updateGraph.sharedLock().isHeldByCurrentThread());
                    }
                }
            }).get();
        } finally {
            unlockedThread.shutdownNow();
        }
    }
}
