/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.base.Pair;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.UnsortedInstantGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.ReferentialIntegrity;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;

@Category(OutOfBandTest.class)
public class TestWindowCheck {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testWindowCheckIterative() {
        for (int seed = 0; seed < 1; ++seed) {
            testWindowCheckIterative(seed);
        }
    }

    /**
     * Run a window check over the course of a simulated day.
     *
     * We have a Timestamp column and a sentinel column.
     *
     * Time advances by one second per step, which randomly modifies the source table.
     *
     * The WindowEvalNugget verifies the original columns are unchanged and that the value of the InWindow column is
     * correct. A prev checker is added to ensure that getPrev works on the new table.
     */
    private void testWindowCheckIterative(int seed) {
        final Random random = new Random(seed);
        final Random combinedRandom = new Random(seed);

        final ColumnInfo<?, ?>[] columnInfo;
        final int size = 100;
        final Instant startTime = DateTimeUtils.parseInstant("2018-02-23T09:30:00 NY");
        final Instant endTime;
        if (SHORT_TESTS) {
            endTime = DateTimeUtils.parseInstant("2018-02-23T10:30:00 NY");
        } else {
            endTime = DateTimeUtils.parseInstant("2018-02-23T16:00:00 NY");
        }
        final QueryTable table = getTable(size, random, columnInfo = initColumnInfos(new String[] {"Timestamp", "C1"},
                new UnsortedInstantGenerator(startTime, endTime, 0.01),
                new IntGenerator(1, 100)));
        // Use a smaller step size so that the random walk on tableSize doesn't become unwieldy given the large number
        // of steps.
        final int stepSize = (int) Math.ceil(Math.sqrt(size));

        final TestClock clock = new TestClock();
        clock.now = DateTimeUtils.epochNanos(startTime);

        final WindowEvalNugget[] en;
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.exclusiveLock().lock();
        try {
            en = new WindowEvalNugget[] {
                    new WindowEvalNugget(clock, table)
            };
        } finally {
            updateGraph.exclusiveLock().unlock();
        }

        final int stepsPerTick = 1;

        int step = 0;

        while (clock.now < DateTimeUtils.epochNanos(endTime) + 600 * DateTimeUtils.SECOND) {
            ++step;
            final boolean combined = combinedRandom.nextBoolean();

            if (combined) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    advanceTime(clock, en);
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, size,
                            random, table, columnInfo);
                });
                TstUtils.validate("Step " + step, en);
            } else {
                updateGraph.runWithinUnitTestCycle(() -> advanceTime(clock, en));
                if (RefreshingTableTestCase.printTableUpdates) {
                    TstUtils.validate("Step = " + step + " time = " + DateTimeUtils.epochNanosToInstant(clock.now), en);
                }

                for (int ii = 0; ii < stepsPerTick; ++ii) {
                    if (RefreshingTableTestCase.printTableUpdates) {
                        System.out.println("Step " + step + "-" + ii);
                    }
                    RefreshingTableTestCase.simulateShiftAwareStep(step + "-" + ii, stepSize, random, table, columnInfo,
                            en);
                }
            }
        }
    }

    private void advanceTime(TestClock clock, WindowEvalNugget[] en) {
        clock.now += 5 * DateTimeUtils.SECOND;
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Ticking time to " + DateTimeUtils.epochNanosToInstant(clock.now));
        }
        for (final WindowEvalNugget wen : en) {
            wen.windowed.second.run();
        }
    }

    @Test
    public void testWindowCheckEmptyInitial() {
        base.setExpectError(false);

        final TestClock clock = new TestClock();
        final Instant startTime = DateTimeUtils.parseInstant("2018-02-23T09:30:00 NY");
        clock.now = DateTimeUtils.epochNanos(startTime);

        final Instant[] emptyInstantArray = new Instant[0];
        final QueryTable tableToCheck = testRefreshingTable(i().toTracking(),
                col("Timestamp", emptyInstantArray), intCol("Sentinel"));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Pair<Table, WindowCheck.TimeWindowListener> windowed = updateGraph.sharedLock().computeLocked(
                () -> WindowCheck.addTimeWindowInternal(clock, tableToCheck, "Timestamp",
                        DateTimeUtils.SECOND * 60, "InWindow", false));

        TableTools.showWithRowSet(windowed.first);

        updateGraph.runWithinUnitTestCycle(windowed.second::run);

    }

    @Test
    public void testWindowCheckGetPrev() {
        final TestClock timeProvider = new TestClock();
        final Instant startTime = DateTimeUtils.parseInstant("2022-07-14T09:30:00 NY");
        timeProvider.now = DateTimeUtils.epochNanos(startTime);

        final Instant[] initialValues = Stream.concat(Arrays.stream(
                new String[] {"2022-07-14T09:25:00 NY", "2022-07-14T09:30:00 NY", "2022-07-14T09:35:00 NY"})
                .map(DateTimeUtils::parseInstant), Stream.of((Instant) null)).toArray(Instant[]::new);
        final QueryTable tableToCheck = testRefreshingTable(i(0, 1, 2, 3).toTracking(),
                col("Timestamp", initialValues),
                intCol("Sentinel", 1, 2, 3, 4));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Pair<Table, WindowCheck.TimeWindowListener> windowed = updateGraph.sharedLock().computeLocked(
                () -> WindowCheck.addTimeWindowInternal(
                        timeProvider, tableToCheck, "Timestamp", DateTimeUtils.SECOND * 60, "InWindow", false));

        TableTools.showWithRowSet(windowed.first);

        updateGraph.runWithinUnitTestCycle(windowed.second::run);

        assertTableEquals(tableToCheck.updateView("InWindow = Sentinel == 4 ? null : Sentinel >= 2"), windowed.first);

        final ColumnSource<Boolean> resultSource = windowed.first.getColumnSource("InWindow", Boolean.class);
        Assert.assertEquals(resultSource.get(0), Boolean.FALSE);
        Assert.assertEquals(resultSource.get(1), Boolean.TRUE);
        Assert.assertEquals(resultSource.get(2), Boolean.TRUE);
        Assert.assertNull(resultSource.get(3));
        Assert.assertEquals(resultSource.getPrev(0), Boolean.FALSE);
        Assert.assertEquals(resultSource.getPrev(1), Boolean.TRUE);
        Assert.assertEquals(resultSource.getPrev(2), Boolean.TRUE);
        Assert.assertNull(resultSource.getPrev(3));

        updateGraph.startCycleForUnitTests();

        timeProvider.now = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2022-07-14T09:34:00 NY"));
        windowed.second.run();

        while (((QueryTable) windowed.first).getLastNotificationStep() < updateGraph.clock().currentStep()) {
            updateGraph.flushOneNotificationForUnitTests();
        }

        Assert.assertEquals(resultSource.get(0), Boolean.FALSE);
        Assert.assertEquals(resultSource.get(1), Boolean.FALSE);
        Assert.assertEquals(resultSource.get(2), Boolean.TRUE);
        Assert.assertNull(resultSource.get(3));
        Assert.assertEquals(resultSource.getPrev(0), Boolean.FALSE);
        Assert.assertEquals(resultSource.getPrev(1), Boolean.TRUE);
        Assert.assertEquals(resultSource.getPrev(2), Boolean.TRUE);
        Assert.assertNull(resultSource.getPrev(3));

        updateGraph.completeCycleForUnitTests();
    }

    @Test
    public void testWindowCheckStatic() {

        final TestClock timeProvider = new TestClock();
        final Instant startTime = DateTimeUtils.parseInstant("2022-07-14T09:30:00 NY");
        timeProvider.now = DateTimeUtils.epochNanos(startTime);

        final Instant[] initialValues = Stream.concat(Arrays.stream(
                new String[] {"2022-07-14T09:25:00 NY", "2022-07-14T09:30:00 NY", "2022-07-14T09:35:00 NY"})
                .map(DateTimeUtils::parseInstant), Stream.of((Instant) null)).toArray(Instant[]::new);
        final QueryTable tableToCheck = testTable(i(0, 1, 2, 3).toTracking(),
                col("Timestamp", initialValues),
                intCol("Sentinel", 1, 2, 3, 4));
        Assert.assertFalse(tableToCheck.isRefreshing());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Pair<Table, WindowCheck.TimeWindowListener> windowed = updateGraph.sharedLock().computeLocked(
                () -> WindowCheck.addTimeWindowInternal(
                        timeProvider, tableToCheck, "Timestamp", DateTimeUtils.SECOND * 60, "InWindow", false));

        TableTools.showWithRowSet(windowed.first);

        updateGraph.runWithinUnitTestCycle(windowed.second::run);

        assertTableEquals(tableToCheck.updateView("InWindow = Sentinel == 4 ? null : Sentinel >= 2"), windowed.first);

        final ColumnSource<Boolean> resultSource = windowed.first.getColumnSource("InWindow", Boolean.class);
        Assert.assertEquals(resultSource.get(0), Boolean.FALSE);
        Assert.assertEquals(resultSource.get(1), Boolean.TRUE);
        Assert.assertEquals(resultSource.get(2), Boolean.TRUE);
        Assert.assertNull(resultSource.get(3));
        Assert.assertEquals(resultSource.getPrev(0), Boolean.FALSE);
        Assert.assertEquals(resultSource.getPrev(1), Boolean.TRUE);
        Assert.assertEquals(resultSource.getPrev(2), Boolean.TRUE);
        Assert.assertNull(resultSource.getPrev(3));

        updateGraph.startCycleForUnitTests();

        timeProvider.now = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2022-07-14T09:34:00 NY"));
        windowed.second.run();

        while (((QueryTable) windowed.first).getLastNotificationStep() < updateGraph.clock().currentStep()) {
            updateGraph.flushOneNotificationForUnitTests();
        }

        Assert.assertEquals(resultSource.get(0), Boolean.FALSE);
        Assert.assertEquals(resultSource.get(1), Boolean.FALSE);
        Assert.assertEquals(resultSource.get(2), Boolean.TRUE);
        Assert.assertNull(resultSource.get(3));
        Assert.assertEquals(resultSource.getPrev(0), Boolean.FALSE);
        Assert.assertEquals(resultSource.getPrev(1), Boolean.TRUE);
        Assert.assertEquals(resultSource.getPrev(2), Boolean.TRUE);
        Assert.assertNull(resultSource.getPrev(3));

        updateGraph.completeCycleForUnitTests();
    }

    @Test
    public void testMemoryUsage() {
        final QueryTable inputTable = (QueryTable) TableTools.emptyTable(500_000_000)
                .updateView("Timestamp = '2022-07-01T00:00 NY'");
        inputTable.setRefreshing(true);
        System.gc();
        final long memStart = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("Start Memory: " + memStart);
        final Table withCheck = WindowCheck.addTimeWindow(
                inputTable,
                "Timestamp",
                60 * DateTimeUtils.SECOND,
                "InLastXSeconds");
        System.gc();
        final long memEnd = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("End Memory: " + memEnd);
        final long memChange = memEnd - memStart;
        System.out.println("Change: " + memChange);
        // this previously would require about 2 gigabytes, so we're doing better
        TestCase.assertTrue(memChange < 100_000_000);
        assertTableEquals(inputTable.updateView("InLastXSeconds=false"), withCheck);
    }

    private static class WindowEvalNugget implements EvalNuggetInterface {
        final Pair<Table, WindowCheck.TimeWindowListener> windowed;
        private final QueryTable table;
        @ReferentialIntegrity
        private final TableUpdateValidator validator;
        private final TestClock clock;
        private final long windowNanos;
        private Throwable exception;

        class FailureListener extends InstrumentedTableUpdateListener {
            FailureListener() {
                super("Failure Listener");
            }

            @Override
            public void onUpdate(TableUpdate upstream) {}

            @Override
            public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                exception = originalException;
                final StringWriter errors = new StringWriter();
                originalException.printStackTrace(new PrintWriter(errors));
                TestCase.fail(errors.toString());
            }
        }

        TableUpdateListener windowedFailureListener = new FailureListener();
        TableUpdateListener updateFailureListener = new FailureListener();

        WindowEvalNugget(TestClock clock, QueryTable table) {
            this.table = table;
            this.clock = clock;
            windowNanos = 300 * DateTimeUtils.SECOND;
            windowed =
                    WindowCheck.addTimeWindowInternal(clock, table, "Timestamp", windowNanos, "InWindow", false);
            validator = TableUpdateValidator.make((QueryTable) windowed.first);

            windowed.first.addUpdateListener(windowedFailureListener);
            validator.getResultTable().addUpdateListener(updateFailureListener);
        }

        @Override
        public void validate(String msg) {
            org.junit.Assert.assertNull(exception);

            TestCase.assertEquals(table.getRowSet(), windowed.first.getRowSet());
            final Map<String, ColumnSource<?>> map = table.getColumnSourceMap();
            final Map<String, ? extends ColumnSource<?>> map2 = windowed.first.getColumnSourceMap();
            TestCase.assertEquals(map.size(), map2.size() - 1);

            for (final Map.Entry<String, ? extends ColumnSource<?>> me : map2.entrySet()) {
                if (!me.getKey().equals("InWindow")) {
                    TestCase.assertEquals(map.get(me.getKey()), me.getValue());
                }
            }

            final ColumnSource<Instant> timestamp = table.getColumnSource("Timestamp");
            final ColumnSource<Boolean> inWindow = windowed.first.getColumnSource("InWindow");

            final long now = clock.now;

            for (final RowSet.Iterator it = windowed.first.getRowSet().iterator(); it.hasNext();) {
                final long key = it.nextLong();
                final Instant tableTime = timestamp.get(key);

                final Boolean actual = inWindow.get(key);
                if (tableTime == null) {
                    TestCase.assertNull(actual);
                } else {
                    final boolean expected = now - DateTimeUtils.epochNanos(tableTime) < windowNanos;
                    TestCase.assertEquals((boolean) actual, expected);
                }
            }

            windowed.second.validateQueue();
        }

        @Override
        public void show() {
            TableTools.show(windowed.first);
        }
    }
}
