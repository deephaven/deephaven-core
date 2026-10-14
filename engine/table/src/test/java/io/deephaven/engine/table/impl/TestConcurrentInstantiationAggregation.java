//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.SleepUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.util.SortedBy;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.annotations.ReflexiveUse;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class TestConcurrentInstantiationAggregation extends TestConcurrentInstantiationBase {
    @Test
    public void testSelectDistinct() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                col("y", "a", "b", "a", "c"));
        final Table expected1 = newTable(col("y", "a", "b", "c"));
        final Table expected2 = newTable(col("y", "a", "d", "b", "c"));
        final Table expected2outOfOrder = newTable(col("y", "a", "b", "c", "d"));

        updateGraph.startCycleForUnitTests(false);

        final Callable<Table> callable = () -> table.selectDistinct("y");

        final Table distinct1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(expected1, distinct1);

        TstUtils.addToTable(table, i(3), col("y", "d"));

        TstUtils.assertTableEquals(expected1, distinct1);

        final Table distinct2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(expected1, distinct2);
        TstUtils.assertTableEquals(expected1, prevTable(distinct2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table distinct3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(expected1, prevTable(distinct1));
        TstUtils.assertTableEquals(expected1, prevTable(distinct2));
        TstUtils.assertTableEquals(expected2, distinct3);
        TstUtils.assertTableEquals(expected2, prevTable(distinct3));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(expected2outOfOrder, distinct1);
        TstUtils.assertTableEquals(expected2outOfOrder, distinct2);
        TstUtils.assertTableEquals(expected2, distinct3);
        TstUtils.assertTableEquals(expected2, prevTable(distinct3));
    }

    @ReflexiveUse(referrers = "io.deephaven.engine.table.impl.TestConcurrentInstantiationAggregation")
    public static String identitySleep(String x) {
        SleepUtil.sleep(50);
        return x;
    }

    public static class BarrierFunction implements UnaryOperator<String> {
        final AtomicInteger invocationCount = new AtomicInteger(0);
        int sleepDuration = 50;

        @Override
        public String apply(String s) {
            synchronized (invocationCount) {
                invocationCount.incrementAndGet();
                invocationCount.notifyAll();
            }
            if (sleepDuration > 0) {
                SleepUtil.sleep(sleepDuration);
            }
            return s;
        }

        void waitForInvocation(int count, int timeoutMillis) throws InterruptedException {
            final long endTime = System.currentTimeMillis() + timeoutMillis;
            synchronized (invocationCount) {
                long now = System.currentTimeMillis();
                while (invocationCount.get() < count && now < endTime) {
                    invocationCount.wait(endTime - now);
                    now = System.currentTimeMillis();
                }
                if (invocationCount.get() < count) {
                    throw new RuntimeException("Invocation count did not advance.");
                }
            }
        }
    }

    @Test
    public void testSelectDistinctReset() throws ExecutionException, InterruptedException, TimeoutException {
        final BarrierFunction barrierFunction = new BarrierFunction();
        QueryScope.addParam("barrierFunction", barrierFunction);

        try {
            final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                    col("y", "a", "b", "a", "c"));
            final Table slowed = table.updateView("z=barrierFunction.apply(y)");
            final Table expected1 = newTable(col("z", "a", "b"));

            updateGraph.startCycleForUnitTests(false);

            final Callable<Table> callable = () -> slowed.selectDistinct("z");

            final Future<Table> future1 = pool.submit(callable);
            barrierFunction.waitForInvocation(2, 5000);

            System.out.println("Removing rows");
            removeRows(table, i(8));
            table.notifyListeners(i(), i(8), i());
            updateGraph.markSourcesRefreshedForUnitTests();

            barrierFunction.sleepDuration = 0;

            updateGraph.completeCycleForUnitTests();

            final Table distinct1 = future1.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
            TstUtils.assertTableEquals(expected1, distinct1);
        } finally {
            QueryScope.addParam("barrierFunction", null);
        }
    }

    @Test
    public void testSumBy() throws Exception {
        testByConcurrent(t -> t.sumBy("KeyColumn"));
        testByConcurrent(t -> t.absSumBy("KeyColumn"));
    }

    @Test
    public void testAvgBy() throws Exception {
        testByConcurrent(t -> t.avgBy("KeyColumn"));
    }

    @Test
    public void testVarBy() throws Exception {
        testByConcurrent(t -> t.varBy("KeyColumn"));
    }

    @Test
    public void testStdBy() throws Exception {
        testByConcurrent(t -> t.varBy("KeyColumn"));
    }

    @Test
    public void testCountBy() throws Exception {
        testByConcurrent(t -> t.varBy("KeyColumn"));
    }

    private static <T extends Table> T setAddOnly(@NotNull final T table) {
        // noinspection unchecked
        return (T) table.withAttributes(Map.of(Table.ADD_ONLY_TABLE_ATTRIBUTE, true));
    }

    @Test
    public void testMinMaxBy() throws Exception {
        testByConcurrent(t -> t.maxBy("KeyColumn"));
        testByConcurrent(t -> t.minBy("KeyColumn"));
        testByConcurrent(t -> setAddOnly(t).minBy("KeyColumn"), true, false, false, true);
        testByConcurrent(t -> setAddOnly(t).maxBy("KeyColumn"), true, false, false, true);
    }

    @Test
    public void testFirstLastBy() throws Exception {
        testByConcurrent(t -> t.firstBy("KeyColumn"));
        testByConcurrent(t -> t.lastBy("KeyColumn"));
    }

    @Test
    public void testSortedFirstLastBy() throws Exception {
        testByConcurrent(t -> SortedBy.sortedFirstBy(t, "IntCol", "KeyColumn"));
        testByConcurrent(t -> SortedBy.sortedLastBy(t, "IntCol", "KeyColumn"));
    }

    @Test
    public void testKeyedBy() throws Exception {
        testByConcurrent(t -> t.groupBy("KeyColumn"));
    }

    @Test
    public void testNoKeyBy() throws Exception {
        testByConcurrent(Table::groupBy, false, false, true, true);
    }

    @Test
    public void testPercentileBy() throws Exception {
        final Function<Table, String[]> nonKeyColumnNames = t -> t.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName).filter(cn -> !cn.equals("KeyColumn")).toArray(String[]::new);
        testByConcurrent(t -> t.dropColumns("KeyColumn").aggBy(AggPct(0.25, nonKeyColumnNames.apply(t))),
                false, false, true, true);
        testByConcurrent(t -> t.dropColumns("KeyColumn").aggBy(AggPct(0.75, nonKeyColumnNames.apply(t))),
                false, false, true, true);
        testByConcurrent(t -> t.medianBy("KeyColumn"));
    }

    @Test
    public void testAggCombo() throws Exception {
        testByConcurrent(t -> t.aggBy(List.of(AggAvg("AvgInt=IntCol"), AggCount("NumInts"),
                AggSum("SumDouble=DoubleCol"), AggMax("MaxDouble=DoubleCol")), "KeyColumn"));
    }

    @Test
    public void testWavgBy() throws Exception {
        testByConcurrent(t -> t.wavgBy("IntCol", "KeyColumn"), true, true, true, false);
        testByConcurrent(t -> t.wavgBy("IntCol", "KeyColumn"), true, false, true, false);
        testByConcurrent(t -> t.wavgBy("DoubleCol", "KeyColumn"), true, true, true, false);
        testByConcurrent(t -> t.wavgBy("DoubleCol", "KeyColumn"), true, false, true, false);
    }

    private void testByConcurrent(Function<Table, Table> function) throws Exception {
        testByConcurrent(function, true, false, true, true);
        testByConcurrent(function, true, true, true, true);
    }

    private void testByConcurrent(Function<Table, Table> function, boolean hasKeys, boolean withReset,
            boolean allowModifications, boolean haveBigNumerics) throws Exception {
        setExpectError(false);

        final QueryTable table = makeByConcurrentBaseTable(haveBigNumerics);
        final QueryTable table2 = makeByConcurrentStep2Table(allowModifications, haveBigNumerics);

        final BarrierFunction barrierFunction = withReset ? new BarrierFunction() : null;
        QueryScope.addParam("barrierFunction", barrierFunction);

        try {
            final Callable<Table> callable;
            final Table slowed;
            if (withReset) {
                ExecutionContext.getContext().getQueryLibrary()
                        .importStatic(TestConcurrentInstantiationAggregation.class);

                slowed = table.updateView("KeyColumn=barrierFunction.apply(KeyColumn)");
                callable = () -> {
                    final long start = System.currentTimeMillis();
                    System.out.println("Applying callable to slowed table.");
                    try {
                        return function.apply(slowed);
                    } finally {
                        System.out.println("Callable complete: " + (System.currentTimeMillis() - start));
                    }
                };
            } else {
                slowed = null;
                callable = () -> function.apply(table);
            }

            // We only care about the silent version of this table, as it's just a vessel to tick and ensure that the
            // resultant table is computed using the appropriate version.
            final Table expected1 = updateGraph.exclusiveLock().computeLocked(
                    () -> function.apply(table.silent()).select());
            final Table expected2 = updateGraph.exclusiveLock()
                    .computeLocked(() -> function.apply(table2));

            updateGraph.startCycleForUnitTests(false);

            final Future<Table> future1 = pool.submit(callable);
            final Table result1;
            if (withReset) {
                barrierFunction.waitForInvocation(2, 5000);
            }
            result1 = future1.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

            System.out.println("Result 1");
            TableTools.show(result1);
            System.out.println("Expected 1");
            TableTools.show(expected1);

            TstUtils.assertTableEquals(expected1, result1, TableDiff.DiffItems.DoublesExact);

            doByConcurrentAdditions(table, haveBigNumerics);
            if (allowModifications) {
                doByConcurrentModifications(table, haveBigNumerics);
            }
            final Table prevResult1a = prevTable(result1);

            System.out.println("PrevResulta 1");
            TableTools.show(prevResult1a);
            System.out.println("Expected 1");
            TableTools.show(expected1);

            TstUtils.assertTableEquals(expected1, prevResult1a, TableDiff.DiffItems.DoublesExact);

            final Table result2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

            System.out.println("Result 2");
            TableTools.show(result2);
            System.out.println("Expected 1");
            TableTools.show(expected1);

            // The column sources are redirected, and the underlying table has been updated without a notification
            // _yet_,
            // so the column sources have _already_ changed and we are inside an update cycle, so the value of get() is
            // indeterminate
            // therefore this assert is not really a valid thing to do.
            // TstUtils.assertTableEquals(expected1, result2);
            final Table prevResult2a = prevTable(result2);
            System.out.println("Prev Result 2a");
            TableTools.show(prevResult2a);

            TstUtils.assertTableEquals(expected1, prevResult2a, TableDiff.DiffItems.DoublesExact);

            table.notifyListeners(i(5, 9), i(), allowModifications ? i(8) : i());
            updateGraph.markSourcesRefreshedForUnitTests();

            final Future<Table> future3 = pool.submit(callable);
            if (withReset) {
                while (((QueryTable) slowed).getLastNotificationStep() != updateGraph.clock().currentStep()) {
                    updateGraph.flushOneNotificationForUnitTests();
                }
            }
            final Table result3 = future3.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

            System.out.println("Prev Result 1");
            final Table prevResult1b = prevTable(result1);
            TableTools.show(prevResult1b);
            TstUtils.assertTableEquals(expected1, prevResult1b, TableDiff.DiffItems.DoublesExact);

            System.out.println("Prev Result 2b");
            final Table prevResult2b = prevTable(result2);
            TableTools.show(prevResult2b);
            TstUtils.assertTableEquals(expected1, prevResult2b, TableDiff.DiffItems.DoublesExact);

            System.out.println("Result 3");
            TableTools.show(result3);
            System.out.println("Expected 2");
            TableTools.show(expected2);
            TstUtils.assertTableEquals(expected2, result3, TableDiff.DiffItems.DoublesExact);

            updateGraph.completeCycleForUnitTests();

            if (hasKeys) {
                TstUtils.assertTableEquals(expected2.sort("KeyColumn"), result1.sort("KeyColumn"),
                        TableDiff.DiffItems.DoublesExact);
                TstUtils.assertTableEquals(expected2.sort("KeyColumn"), result2.sort("KeyColumn"),
                        TableDiff.DiffItems.DoublesExact);
            } else {
                TstUtils.assertTableEquals(expected2, result1, TableDiff.DiffItems.DoublesExact);
                TstUtils.assertTableEquals(expected2, result2, TableDiff.DiffItems.DoublesExact);
            }
            TstUtils.assertTableEquals(expected2, result3, TableDiff.DiffItems.DoublesExact);

        } finally {
            QueryScope.addParam("barrierFunction", null);
        }
    }

    @Test
    public void testPartitionByConcurrent() throws Exception {
        testPartitionByConcurrent(false);
        testPartitionByConcurrent(true);
    }

    private void testPartitionByConcurrent(boolean withReset) throws Exception {
        setExpectError(false);

        final QueryTable table = makeByConcurrentBaseTable(false);
        final QueryTable table2 = makeByConcurrentStep2Table(true, false);

        final Callable<PartitionedTable> callable;
        final Table slowed;
        if (withReset) {
            ExecutionContext.getContext().getQueryLibrary().importStatic(TestConcurrentInstantiationAggregation.class);

            slowed = table.updateView("KeyColumn=identitySleep(KeyColumn)");
            callable = () -> slowed.partitionBy("KeyColumn");
        } else {
            slowed = null;
            callable = () -> table.partitionBy("KeyColumn");
        }

        // We only care about the silent version of this table, as it's just a vessel to tick and ensure that the
        // resultant table
        // is computed using the appropriate version.
        final Table expected1 = updateGraph.exclusiveLock().computeLocked(
                () -> table.silent().partitionBy("KeyColumn").merge().select());
        final Table expected2 = updateGraph.exclusiveLock().computeLocked(
                () -> table2.silent().partitionBy("KeyColumn").merge().select());

        updateGraph.startCycleForUnitTests(false);

        final Future<PartitionedTable> future1 = pool.submit(callable);
        final PartitionedTable result1;
        if (withReset) {
            SleepUtil.sleep(25);
        }
        result1 = future1.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        System.out.println("Result 1");
        final Table result1a = result1.constituentFor("a");
        final Table result1b = result1.constituentFor("b");
        final Table result1c = result1.constituentFor("c");
        TableTools.show(result1a);
        TableTools.show(result1b);
        TableTools.show(result1c);
        System.out.println("Expected 1");
        TableTools.show(expected1);

        TstUtils.assertTableEquals(expected1.where("KeyColumn = `a`"), result1a);
        TstUtils.assertTableEquals(expected1.where("KeyColumn = `b`"), result1b);
        TstUtils.assertTableEquals(expected1.where("KeyColumn = `c`"), result1c);

        doByConcurrentAdditions(table, false);
        doByConcurrentModifications(table, false);

        final PartitionedTable result2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        System.out.println("Result 2");
        final Table result2a = result2.constituentFor("a");
        final Table result2b = result2.constituentFor("b");
        final Table result2c = result2.constituentFor("c");
        final Table result2d_1 = result2.constituentFor("d");
        assertNull(result2d_1);

        TableTools.show(result2a);
        TableTools.show(result2b);
        TableTools.show(result2c);
        System.out.println("Expected 1");
        TableTools.show(expected1);

        table.notifyListeners(i(5, 9), i(), i(8));
        updateGraph.markSourcesRefreshedForUnitTests();

        final Future<PartitionedTable> future3 = pool.submit(callable);
        if (withReset) {
            while (((QueryTable) slowed).getLastNotificationStep() != updateGraph.clock().currentStep()) {
                updateGraph.flushOneNotificationForUnitTests();
            }
        }
        final PartitionedTable result3 = future3.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        System.out.println("Result 3");
        final Table result3a = result3.constituentFor("a");
        final Table result3b = result3.constituentFor("b");
        final Table result3c = result3.constituentFor("c");
        final Table result3d = result3.constituentFor("d");

        System.out.println("Expected 2");
        TableTools.show(expected2);

        TstUtils.assertTableEquals(expected2.where("KeyColumn = `a`"), result3a);
        TstUtils.assertTableEquals(expected2.where("KeyColumn = `b`"), result3b);
        assertNull(result3c);
        TstUtils.assertTableEquals(expected2.where("KeyColumn = `d`"), result3d);

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(expected2, result1.merge());
        TstUtils.assertTableEquals(expected2, result2.merge());
        TstUtils.assertTableEquals(expected2, result3.merge());
    }

    private QueryTable makeByConcurrentBaseTable(boolean haveBigNumerics) {
        final List<ColumnHolder<?>> columnHolders = new ArrayList<>(Arrays.asList(
                col("KeyColumn", "a", "b", "a", "c"),
                intCol("IntCol", 1, 2, 3, 4),
                doubleCol("DoubleCol", 100.1, 200.2, 300.3, 400.4),
                floatCol("FloatCol", 10.1f, 20.2f, 30.3f, 40.4f),
                shortCol("ShortCol", (short) 10, (short) 20, (short) 30, (short) 40),
                byteCol("ByteCol", (byte) 11, (byte) 12, (byte) 13, (byte) 14),
                charCol("CharCol", 'A', 'B', 'C', 'D'),
                longCol("LongCol", 10_000_000_000L, 20_000_000_000L, 30_000_000_000L, 40_000_000_000L)));

        if (haveBigNumerics) {
            columnHolders.add(col("BigDecCol", BigDecimal.valueOf(10000.1), BigDecimal.valueOf(20000.2),
                    BigDecimal.valueOf(40000.3), BigDecimal.valueOf(40000.4)));
            columnHolders.add(col("BigIntCol", BigInteger.valueOf(100000), BigInteger.valueOf(200000),
                    BigInteger.valueOf(300000), BigInteger.valueOf(400000)));
        }

        return TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

    private QueryTable makeByConcurrentStep2Table(boolean allowModifications, boolean haveBigNumerics) {
        final QueryTable table2 = makeByConcurrentBaseTable(haveBigNumerics);
        doByConcurrentAdditions(table2, haveBigNumerics);
        if (allowModifications) {
            doByConcurrentModifications(table2, haveBigNumerics);
        }
        return table2;

    }

    private void doByConcurrentModifications(QueryTable table, boolean haveBigNumerics) {
        final List<ColumnHolder<?>> columnHolders = new ArrayList<>(Arrays.asList(
                col("KeyColumn", "b"),
                intCol("IntCol", 7),
                doubleCol("DoubleCol", 700.7),
                floatCol("FloatCol", 70.7f),
                shortCol("ShortCol", (short) 70),
                byteCol("ByteCol", (byte) 17),
                charCol("CharCol", 'E'),
                longCol("LongCol", 70_000_000_000L)));
        if (haveBigNumerics) {
            columnHolders.addAll(Arrays.asList(
                    col("BigDecCol", BigDecimal.valueOf(70000.7)),
                    col("BigIntCol", BigInteger.valueOf(700000))));
        }

        TstUtils.addToTable(table, i(8), columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

    private void doByConcurrentAdditions(QueryTable table, boolean haveBigNumerics) {

        final List<ColumnHolder<?>> columnHolders = new ArrayList<>(Arrays.asList(
                col("KeyColumn", "d", "a"),
                intCol("IntCol", 5, 6),
                doubleCol("DoubleCol", 505.5, 600.6),
                floatCol("FloatCol", 50.5f, 60.6f),
                shortCol("ShortCol", (short) 50, (short) 60),
                byteCol("ByteCol", (byte) 15, (byte) 16),
                charCol("CharCol", 'E', 'F'),
                longCol("LongCol", 50_000_000_000L, 60_000_000_000L)));

        if (haveBigNumerics) {
            columnHolders.addAll(Arrays.asList(
                    col("BigDecCol", BigDecimal.valueOf(50000.5), BigDecimal.valueOf(60000.6)),
                    col("BigIntCol", BigInteger.valueOf(500000), BigInteger.valueOf(600000))));
        }

        TstUtils.addToTable(table, i(5, 9), columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }
}
