package io.deephaven.db.tables.utils;

import io.deephaven.base.Pair;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.TimeProvider;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.test.junit4.JUnit4LiveTableTestCase;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.intCol;
import static io.deephaven.db.v2.TstUtils.*;

public class TestWindowCheck {
    private final JUnit4LiveTableTestCase base = new JUnit4LiveTableTestCase();

    @Before
    public void setUp() throws Exception {
        base.setUp();
    }

    @After
    public void tearDown() throws Exception {
        base.tearDown();
    }

    /**
     * Run a window check over the course of a simulated day.
     *
     * We have a Timestamp column and a sentinel column.
     *
     * Time advances by one second per step, which randomly modifies the source table.
     *
     * The WindowEvalNugget verifies the original columns are unchanged and that the value of the
     * InWindow column is correct. A prev checker is added to ensure that getPrev works on the new
     * table.
     */
    @Test
    public void testWindowCheckIterative() {
        final Random random = new Random(0);
        final Random combinedRandom = new Random(0);

        final TstUtils.ColumnInfo[] columnInfo;
        final int size = 100;
        final DBDateTime startTime = DBTimeUtils.convertDateTime("2018-02-23T09:30:00 NY");
        final DBDateTime endTime = DBTimeUtils.convertDateTime("2018-02-23T16:00:00 NY");
        final QueryTable table = getTable(size, random,
            columnInfo = initColumnInfos(new String[] {"Timestamp", "C1"},
                new TstUtils.UnsortedDateTimeGenerator(startTime, endTime, 0.01),
                new TstUtils.IntGenerator(1, 100)));
        // Use a smaller step size so that the random walk on tableSize doesn't become unwieldy
        // given the large number of steps.
        final int stepSize = (int) Math.ceil(Math.sqrt(size));

        final TestTimeProvider timeProvider = new TestTimeProvider();
        timeProvider.now = startTime.getNanos();

        final WindowEvalNugget[] en;
        LiveTableMonitor.DEFAULT.exclusiveLock().lock();
        try {
            en = new WindowEvalNugget[] {
                    new WindowEvalNugget(timeProvider, table)
            };
        } finally {
            LiveTableMonitor.DEFAULT.exclusiveLock().unlock();
        }

        final int stepsPerTick = 1;

        int step = 0;

        while (timeProvider.now < endTime.getNanos() + 600 * DBTimeUtils.SECOND) {
            step++;
            final boolean combined = combinedRandom.nextBoolean();

            if (combined) {
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    advanceTime(timeProvider, en);
                    GenerateTableUpdates.generateShiftAwareTableUpdates(
                        GenerateTableUpdates.DEFAULT_PROFILE, size, random, table, columnInfo);
                });
                TstUtils.validate("Step " + step, en);
            } else {
                LiveTableMonitor.DEFAULT
                    .runWithinUnitTestCycle(() -> advanceTime(timeProvider, en));
                if (LiveTableTestCase.printTableUpdates) {
                    TstUtils.validate(
                        "Step = " + step + " time = " + new DBDateTime(timeProvider.now), en);
                }

                for (int ii = 0; ii < stepsPerTick; ++ii) {
                    if (LiveTableTestCase.printTableUpdates) {
                        System.out.println("Step " + step + "-" + ii);
                    }
                    LiveTableTestCase.simulateShiftAwareStep(step + "-" + ii, stepSize, random,
                        table, columnInfo, en);
                }
            }
        }
    }

    private void advanceTime(TestTimeProvider timeProvider, WindowEvalNugget[] en) {
        timeProvider.now += 5 * DBTimeUtils.SECOND;
        if (LiveTableTestCase.printTableUpdates) {
            System.out.println("Ticking time to " + new DBDateTime(timeProvider.now));
        }
        for (final WindowEvalNugget wen : en) {
            wen.windowed.second.refresh();
        }
    }

    @Test
    public void testWindowCheckEmptyInitial() {
        base.setExpectError(false);

        final TestTimeProvider timeProvider = new TestTimeProvider();
        final DBDateTime startTime = DBTimeUtils.convertDateTime("2018-02-23T09:30:00 NY");
        timeProvider.now = startTime.getNanos();

        final DBDateTime[] emptyDateTimeArray = new DBDateTime[0];
        final Table tableToCheck =
            testRefreshingTable(i(), c("Timestamp", emptyDateTimeArray), intCol("Sentinel"));

        final Pair<Table, WindowCheck.TimeWindowListener> windowed = LiveTableMonitor.DEFAULT
            .sharedLock().computeLocked(() -> WindowCheck.addTimeWindowInternal(timeProvider,
                tableToCheck, "Timestamp", DBTimeUtils.SECOND * 60, "InWindow", false));

        TableTools.showWithIndex(windowed.first);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(windowed.second::refresh);

    }

    private static class TestTimeProvider implements TimeProvider {
        long now = 0;

        @Override
        public DBDateTime currentTime() {
            return new DBDateTime(now);
        }
    }

    private static class WindowEvalNugget implements EvalNuggetInterface {
        final Pair<Table, WindowCheck.TimeWindowListener> windowed;
        private final QueryTable table;
        private final TableUpdateValidator validator;
        private final TestTimeProvider timeProvider;
        private final long windowNanos;
        private Throwable exception;

        class FailureListener extends InstrumentedShiftAwareListener {
            FailureListener() {
                super("Failure Listener");
            }

            @Override
            public void onUpdate(Update upstream) {}

            @Override
            public void onFailureInternal(Throwable originalException,
                UpdatePerformanceTracker.Entry sourceEntry) {
                exception = originalException;
                final StringWriter errors = new StringWriter();
                originalException.printStackTrace(new PrintWriter(errors));
                TestCase.fail(errors.toString());
            }
        }

        ShiftAwareListener windowedFailureListener = new FailureListener();
        ShiftAwareListener updateFailureListener = new FailureListener();

        WindowEvalNugget(TestTimeProvider timeProvider, QueryTable table) {
            this.table = table;
            this.timeProvider = timeProvider;
            windowNanos = 300 * DBTimeUtils.SECOND;
            windowed = WindowCheck.addTimeWindowInternal(timeProvider, table, "Timestamp",
                windowNanos, "InWindow", false);
            validator = TableUpdateValidator.make((QueryTable) windowed.first);

            ((QueryTable) windowed.first).listenForUpdates(windowedFailureListener);
            validator.getResultTable().listenForUpdates(updateFailureListener);
        }

        @Override
        public void validate(String msg) {
            org.junit.Assert.assertNull(exception);

            TestCase.assertEquals(table.getIndex(), windowed.first.getIndex());
            final Map<String, ColumnSource> map = table.getColumnSourceMap();
            final Map<String, ? extends ColumnSource> map2 = windowed.first.getColumnSourceMap();
            TestCase.assertEquals(map.size(), map2.size() - 1);

            for (final Map.Entry<String, ? extends ColumnSource> me : map2.entrySet()) {
                if (!me.getKey().equals("InWindow")) {
                    TestCase.assertEquals(map.get(me.getKey()), me.getValue());
                }
            }

            // noinspection unchecked
            final ColumnSource<DBDateTime> timestamp = table.getColumnSource("Timestamp");
            // noinspection unchecked
            final ColumnSource<Boolean> inWindow = windowed.first.getColumnSource("InWindow");

            final long now = timeProvider.now;

            for (final Index.Iterator it = windowed.first.getIndex().iterator(); it.hasNext();) {
                final long key = it.nextLong();
                final DBDateTime tableTime = timestamp.get(key);

                final Boolean actual = inWindow.get(key);
                if (tableTime == null) {
                    TestCase.assertNull(actual);
                } else {
                    final boolean expected = now - tableTime.getNanos() < windowNanos;
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
