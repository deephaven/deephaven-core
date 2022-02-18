package io.deephaven.engine.util;

import io.deephaven.base.Pair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.time.TimeProvider;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.table.impl.TstUtils.*;

@Category(OutOfBandTest.class)
public class TestWindowCheck {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

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
    @Test
    public void testWindowCheckIterative() {
        final Random random = new Random(0);
        final Random combinedRandom = new Random(0);

        final TstUtils.ColumnInfo[] columnInfo;
        final int size = 100;
        final DateTime startTime = DateTimeUtils.convertDateTime("2018-02-23T09:30:00 NY");
        final DateTime endTime;
        if (SHORT_TESTS) {
            endTime = DateTimeUtils.convertDateTime("2018-02-23T10:30:00 NY");
        } else {
            endTime = DateTimeUtils.convertDateTime("2018-02-23T16:00:00 NY");
        }
        final QueryTable table = getTable(size, random, columnInfo = initColumnInfos(new String[] {"Timestamp", "C1"},
                new TstUtils.UnsortedDateTimeGenerator(startTime, endTime, 0.01),
                new TstUtils.IntGenerator(1, 100)));
        // Use a smaller step size so that the random walk on tableSize doesn't become unwieldy given the large number
        // of steps.
        final int stepSize = (int) Math.ceil(Math.sqrt(size));

        final TestTimeProvider timeProvider = new TestTimeProvider();
        timeProvider.now = startTime.getNanos();

        final WindowEvalNugget[] en;
        UpdateGraphProcessor.DEFAULT.exclusiveLock().lock();
        try {
            en = new WindowEvalNugget[] {
                    new WindowEvalNugget(timeProvider, table)
            };
        } finally {
            UpdateGraphProcessor.DEFAULT.exclusiveLock().unlock();
        }

        final int stepsPerTick = 1;

        int step = 0;

        while (timeProvider.now < endTime.getNanos() + 600 * DateTimeUtils.SECOND) {
            ++step;
            final boolean combined = combinedRandom.nextBoolean();

            if (combined) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                    advanceTime(timeProvider, en);
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, size,
                            random, table, columnInfo);
                });
                TstUtils.validate("Step " + step, en);
            } else {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> advanceTime(timeProvider, en));
                if (RefreshingTableTestCase.printTableUpdates) {
                    TstUtils.validate("Step = " + step + " time = " + new DateTime(timeProvider.now), en);
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

    private void advanceTime(TestTimeProvider timeProvider, WindowEvalNugget[] en) {
        timeProvider.now += 5 * DateTimeUtils.SECOND;
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Ticking time to " + new DateTime(timeProvider.now));
        }
        for (final WindowEvalNugget wen : en) {
            wen.windowed.second.run();
        }
    }

    @Test
    public void testWindowCheckEmptyInitial() {
        base.setExpectError(false);

        final TestTimeProvider timeProvider = new TestTimeProvider();
        final DateTime startTime = DateTimeUtils.convertDateTime("2018-02-23T09:30:00 NY");
        timeProvider.now = startTime.getNanos();

        final DateTime[] emptyDateTimeArray = new DateTime[0];
        final QueryTable tableToCheck = testRefreshingTable(i().toTracking(),
                c("Timestamp", emptyDateTimeArray), intCol("Sentinel"));

        final Pair<Table, WindowCheck.TimeWindowListener> windowed = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> WindowCheck.addTimeWindowInternal(timeProvider, tableToCheck, "Timestamp",
                        DateTimeUtils.SECOND * 60, "InWindow", false));

        TableTools.showWithRowSet(windowed.first);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(windowed.second::run);

    }

    private static class TestTimeProvider implements TimeProvider {
        long now = 0;

        @Override
        public DateTime currentTime() {
            return new DateTime(now);
        }
    }

    private static class WindowEvalNugget implements EvalNuggetInterface {
        final Pair<Table, WindowCheck.TimeWindowListener> windowed;
        private final QueryTable table;
        private final TableUpdateValidator validator;
        private final TestTimeProvider timeProvider;
        private final long windowNanos;
        private Throwable exception;

        class FailureListener extends InstrumentedTableUpdateListener {
            FailureListener() {
                super("Failure ShiftObliviousListener");
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

        WindowEvalNugget(TestTimeProvider timeProvider, QueryTable table) {
            this.table = table;
            this.timeProvider = timeProvider;
            windowNanos = 300 * DateTimeUtils.SECOND;
            windowed =
                    WindowCheck.addTimeWindowInternal(timeProvider, table, "Timestamp", windowNanos, "InWindow", false);
            validator = TableUpdateValidator.make((QueryTable) windowed.first);

            ((QueryTable) windowed.first).listenForUpdates(windowedFailureListener);
            validator.getResultTable().listenForUpdates(updateFailureListener);
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

            final ColumnSource<DateTime> timestamp = table.getColumnSource("Timestamp");
            final ColumnSource<Boolean> inWindow = windowed.first.getColumnSource("InWindow");

            final long now = timeProvider.now;

            for (final RowSet.Iterator it = windowed.first.getRowSet().iterator(); it.hasNext();) {
                final long key = it.nextLong();
                final DateTime tableTime = timestamp.get(key);

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
