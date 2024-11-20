//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.FormulaCompilationException;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.vector.ObjectVector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.function.Basic.isNull;

@Category(OutOfBandTest.class)
public class TestRollingFormula extends BaseUpdateByTest {
    /**
     * These are used in the static tests and leverage the Numeric class functions for verification. Additional tests
     * are performed on BigInteger/BigDecimal columns as well.
     */
    final String[] primitiveColumns = new String[] {
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
    };

    /**
     * These are used in the ticking table evaluations where we verify dynamic vs static tables.
     */
    final String[] columns = new String[] {
            "charCol",
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            "bigIntCol",
            "bigDecimalCol",
    };

    final int STATIC_TABLE_SIZE = 1000;
    final int DYNAMIC_TABLE_SIZE = 100;
    final int DYNAMIC_UPDATE_SIZE = 10;
    final int DYNAMIC_UPDATE_STEPS = 20;

    @SuppressWarnings("unused") // Functions used via QueryLibrary
    @VisibleForTesting
    @TestUseOnly
    public static class Helpers {

        public static BigDecimal sumBigDecimal(ObjectVector<BigDecimal> bigDecimalObjectVector) {
            if (bigDecimalObjectVector == null) {
                return null;
            }

            BigDecimal sum = BigDecimal.ZERO;
            final long n = bigDecimalObjectVector.size();

            for (long i = 0; i < n; i++) {
                BigDecimal val = bigDecimalObjectVector.get(i);
                if (!isNull(val)) {
                    sum = sum.add(val);
                }
            }
            return sum;
        }

        public static BigInteger sumBigInteger(ObjectVector<BigInteger> bigIntegerObjectVector) {
            if (bigIntegerObjectVector == null) {
                return null;
            }

            BigInteger sum = BigInteger.ZERO;
            final long n = bigIntegerObjectVector.size();

            for (long i = 0; i < n; i++) {
                BigInteger val = bigIntegerObjectVector.get(i);
                if (!isNull(val)) {
                    sum = sum.add(val);
                }
            }
            return sum;
        }
    }

    // region Static Zero Key Tests

    @Test
    public void testStaticZeroKeyAllNullVector() {
        final int prevTicks = 1;
        final int postTicks = 0;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final int prevTicks = 100;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ZERO;

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwd() {
        final Duration prevTime = Duration.ZERO;
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    private void doTestStaticZeroKey(final int prevTicks, final int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        // Verify that the RollingFormula spec errors out when provided a paramToken and multiple input columns.
        try {
            t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out=sum(x)", "x", primitiveColumns));
            Assert.statementNeverExecuted();
        } catch (Exception exception) {
            Assert.assertion(exception instanceof FormulaCompilationException,
                    "exception instanceof FormulaCompilationException");
        }

        Table actual;
        Table expected;
        String[] updateStrings;
        String[] precomputeColumns;

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // sum vs. RollingGroup + sum (pre-adding 1 to each value)
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum(x + 1)", "x", primitiveColumns));

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + "+1").toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=sum(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns))
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // avg vs. RollingGroup + avg (pre-adding 1 to each value)
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x + 1)", "x", primitiveColumns));

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + "+1").toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns))
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // complex problem
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x * x + x)", "x", primitiveColumns));

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + " * " + c + " + " + c).toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns))
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "count(x)", "x",
                primitiveColumns));
        expected = t.updateBy(UpdateByOperation.RollingCount(prevTicks, postTicks, primitiveColumns));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Avg vs. RollingAvg
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x)", "x", primitiveColumns));
        expected = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, primitiveColumns));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Identity vs. RollingGroup
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "x", "x", columns));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns))
                .update(Arrays.stream(columns).map(c -> c + "=" + c + ".getDirect()").toArray(String[]::new));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // BigDecimal / BigInteger custom sum function vs. RollingSum
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        actual = t.updateBy(List.of(
                UpdateByOperation.RollingFormula(prevTicks, postTicks, "sumBigDecimal(x)", "x", "bigDecimalCol"),
                UpdateByOperation.RollingFormula(prevTicks, postTicks, "sumBigInteger(x)", "x", "bigIntCol")));

        // RollingSum returns null when the window is empty, replace that with zeros.
        expected = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks, "bigDecimalCol", "bigIntCol"))
                .update("bigDecimalCol=bigDecimalCol == null ? java.math.BigDecimal.ZERO : bigDecimalCol",
                        "bigIntCol=bigIntCol == null ? java.math.BigInteger.ZERO : bigIntCol");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Boolean count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "count(ifelse(x, (long)1, (long)0))",
                "x", "boolCol"));
        expected = t.updateBy(UpdateByOperation.RollingCount(prevTicks, postTicks, "boolCol"));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Multi-column formula tests
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        // zero input columns
        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out_val=-1L"));
        expected = t.update("out_val=-1L");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // single input column
        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out_val=sum(intCol)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol"))
                .update("out_val=sum(a)").dropColumns("a");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // two input columns
        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out_val=min(intCol) - max(longCol)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol", "b=longCol"))
                .update("out_val=min(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // three input columns
        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                        "out_val=sum(intCol) - max(longCol) + min(doubleCol)"));
        expected =
                t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol", "b=longCol", "c=doubleCol"))
                        .update("out_val=sum(a) - max(b) + min(c)").dropColumns("a", "b", "c");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out_val=sum(intCol) - max(longCol)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol", "b=longCol"))
                .update("out_val=sum(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    private void doTestStaticZeroKeyTimed(final Duration prevTime, final Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        // Verify that the RollingFormula spec errors out when provided a paramToken and multiple input columns.
        try {
            t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "out=sum(x)", "x", primitiveColumns));
            Assert.statementNeverExecuted();
        } catch (Exception exception) {
            Assert.assertion(exception instanceof FormulaCompilationException,
                    "exception instanceof FormulaCompilationException");
        }

        Table actual;
        Table expected;
        String[] updateStrings;
        String[] precomputeColumns;

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // sum vs. RollingGroup + sum (pre-adding 1 to each value)
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum(x + 1)", "x", primitiveColumns));

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + "+1").toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=sum(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns))
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // avg vs. RollingGroup + avg (pre-adding 1 to each value)
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x + 1)", "x", primitiveColumns));

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + "+1").toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns))
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // complex problem
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x * x + x)", "x", primitiveColumns));

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + " * " + c + " + " + c).toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns))
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "count(x)", "x", primitiveColumns));
        expected = t.updateBy(UpdateByOperation.RollingCount("ts", prevTime, postTime, primitiveColumns));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Avg vs. RollingAvg
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t
                .updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x)", "x", primitiveColumns));
        expected = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, primitiveColumns));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Identity vs. RollingGroup
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "x", "x", columns));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns))
                .update(Arrays.stream(columns).map(c -> c + "=" + c + ".getDirect()").toArray(String[]::new));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // BigDecimal / BigInteger custom sum function vs. RollingSum
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        actual = t.updateBy(List.of(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sumBigDecimal(x)", "x",
                        "bigDecimalCol"),
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sumBigInteger(x)", "x",
                        "bigIntCol")));

        // RollingSum returns null when the window is empty, replace that with zeros.
        expected = t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "bigDecimalCol", "bigIntCol"))
                .update("bigDecimalCol=bigDecimalCol == null ? java.math.BigDecimal.ZERO : bigDecimalCol",
                        "bigIntCol=bigIntCol == null ? java.math.BigInteger.ZERO : bigIntCol");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Boolean count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                "count(ifelse(x, (long)1, (long)0))", "x", "boolCol"));
        expected = t.updateBy(UpdateByOperation.RollingCount("ts", prevTime, postTime, "boolCol"));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Multi-column formula tests
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        // zero input columns
        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "out_val=-1L"));
        expected = t.update("out_val=-1L");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // single input column
        actual = t
                .updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "out_val=sum(intCol)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "a=intCol"))
                .update("out_val=sum(a)").dropColumns("a");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // two input columns
        actual = t
                .updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                        "out_val=min(intCol) - max(longCol)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "a=intCol", "b=longCol"))
                .update("out_val=min(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // three input columns
        actual = t
                .updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                        "out_val=sum(intCol) - max(longCol) + min(doubleCol)"));
        expected = t
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "a=intCol", "b=longCol",
                        "c=doubleCol"))
                .update("out_val=sum(a) - max(b) + min(c)").dropColumns("a", "b", "c");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                "out_val=intCol + longCol"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime,
                "a=intCol", "b=longCol"))
                .update("out_val=a + b").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    // endregion

    // region Static Bucketed Tests

    @Test
    public void testStaticGroupedBucketed() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticBucketed(true, prevTicks, postTicks);
    }

    @Test
    public void testStaticGroupedBucketedTimed() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticBucketedTimed(true, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedFwdRevWindowTimed() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    private void doTestStaticBucketed(boolean grouped, int prevTicks, int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        Table actual;
        Table expected;
        String[] updateStrings;
        String[] precomputeColumns;

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // sum vs. RollingGroup + sum (pre-adding 1 to each value)
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum(x + 1)", "x", primitiveColumns),
                "Sym");

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + "+1").toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=sum(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns), "Sym")
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // avg vs. RollingGroup + avg (pre-adding 1 to each value)
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x + 1)", "x", primitiveColumns),
                "Sym");

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + "+1").toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns), "Sym")
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // complex problem
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x * x + x)", "x", primitiveColumns), "Sym");

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + " * " + c + " + " + c).toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns), "Sym")
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "count(x)", "x", primitiveColumns),
                "Sym");
        expected = t.updateBy(UpdateByOperation.RollingCount(prevTicks, postTicks, primitiveColumns), "Sym");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Avg vs. RollingAvg
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x)", "x", primitiveColumns),
                "Sym");
        expected = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, primitiveColumns), "Sym");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Identity vs. RollingGroup
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "x", "x", columns));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns))
                .update(Arrays.stream(columns).map(c -> c + "=" + c + ".getDirect()").toArray(String[]::new));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // BigDecimal / BigInteger custom sum function vs. RollingSum
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        actual = t.updateBy(List.of(
                UpdateByOperation.RollingFormula(prevTicks, postTicks, "sumBigDecimal(x)", "x", "bigDecimalCol"),
                UpdateByOperation.RollingFormula(prevTicks, postTicks, "sumBigInteger(x)", "x", "bigIntCol")),
                "Sym");

        // RollingSum returns null when the window is empty, replace that with zeros.
        expected = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks, "bigDecimalCol", "bigIntCol"), "Sym")
                .update("bigDecimalCol=bigDecimalCol == null ? java.math.BigDecimal.ZERO : bigDecimalCol",
                        "bigIntCol=bigIntCol == null ? java.math.BigInteger.ZERO : bigIntCol");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Boolean count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "count(ifelse(x, (long)1, (long)0))",
                "x", "boolCol"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingCount(prevTicks, postTicks, "boolCol"), "Sym");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Multi-column formula tests
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        // zero input columns
        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out_val=-1L"), "Sym");
        expected = t.update("out_val=-1L");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // single input column
        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out_val=sum(intCol)"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol"), "Sym")
                .update("out_val=sum(a)").dropColumns("a");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // two input columns
        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out_val=min(intCol) - max(longCol)"),
                        "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol", "b=longCol"), "Sym")
                .update("out_val=min(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // three input columns
        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                        "out_val=sum(intCol) - max(longCol) + min(doubleCol)"), "Sym");
        expected =
                t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol", "b=longCol", "c=doubleCol"),
                        "Sym")
                        .update("out_val=sum(a) - max(b) + min(c)").dropColumns("a", "b", "c");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        actual = t
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "out_val=sum(intCol) - max(longCol)"),
                        "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol", "b=longCol"), "Sym")
                .update("out_val=sum(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // using the key column
        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                "out_val=sum(intCol) - max(longCol) + (Sym == null ? 0 : Sym.length())"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol", "b=longCol"), "Sym")
                .update("out_val=sum(a) - max(b) + (Sym == null ? 0 : Sym.length())").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // using the byte key column
        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                "out_val=byteCol == null ? -1 : byteCol + sum(intCol) - max(longCol)"), "byteCol");
        expected =
                t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "a=intCol", "b=longCol"), "byteCol")
                        .update("out_val=byteCol == null ? -1 : byteCol + sum(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        Table actual;
        Table expected;
        String[] updateStrings;
        String[] precomputeColumns;

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // sum vs. RollingGroup + sum (pre-adding 1 to each value)
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum(x + 1)", "x", primitiveColumns), "Sym");

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + "+1").toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=sum(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns), "Sym")
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // avg vs. RollingGroup + avg (pre-adding 1 to each value)
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x + 1)", "x", primitiveColumns), "Sym");

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + "+1").toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns), "Sym")
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // complex problem
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x * x + x)", "x", primitiveColumns),
                "Sym");

        precomputeColumns = Arrays.stream(primitiveColumns)
                .map(c -> c + "=" + c + " * " + c + " + " + c).toArray(String[]::new);
        updateStrings = Arrays.stream(primitiveColumns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
        expected = t.update(precomputeColumns)
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns), "Sym")
                .update(updateStrings);

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "count(x)", "x", primitiveColumns), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingCount("ts", prevTime, postTime, primitiveColumns), "Sym");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Avg vs. RollingAvg
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x)", "x", primitiveColumns),
                "Sym");
        expected = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, primitiveColumns), "Sym");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Identity vs. RollingGroup
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "x", "x", columns));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns))
                .update(Arrays.stream(columns).map(c -> c + "=" + c + ".getDirect()").toArray(String[]::new));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // BigDecimal / BigInteger custom sum function vs. RollingSum
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        actual = t.updateBy(List.of(
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sumBigDecimal(x)", "x",
                        "bigDecimalCol"),
                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sumBigInteger(x)", "x", "bigIntCol")),
                "Sym");

        // RollingSum returns null when the window is empty, replace that with zeros.
        expected = t
                .updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "bigDecimalCol", "bigIntCol"), "Sym")
                .update("bigDecimalCol=bigDecimalCol == null ? java.math.BigDecimal.ZERO : bigDecimalCol",
                        "bigIntCol=bigIntCol == null ? java.math.BigInteger.ZERO : bigIntCol");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Boolean count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                "count(ifelse(x, (long)1, (long)0))", "x", "boolCol"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingCount("ts", prevTime, postTime, "boolCol"), "Sym");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Multi-column formula tests
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        // zero input columns
        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "out_val=-1L"), "Sym");
        expected = t.update("out_val=-1L");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // single input column
        actual = t
                .updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "out_val=sum(intCol)"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "a=intCol"), "Sym")
                .update("out_val=sum(a)").dropColumns("a");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // two input columns
        actual = t
                .updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                        "out_val=min(intCol) - max(longCol)"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "a=intCol", "b=longCol"), "Sym")
                .update("out_val=min(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // three input columns
        actual = t
                .updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                        "out_val=sum(intCol) - max(longCol) + min(doubleCol)"), "Sym");
        expected = t
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "a=intCol", "b=longCol",
                        "c=doubleCol"), "Sym")
                .update("out_val=sum(a) - max(b) + min(c)").dropColumns("a", "b", "c");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                "out_val=intCol + longCol"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime,
                "a=intCol", "b=longCol"), "Sym")
                .update("out_val=a + b").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // using the key column
        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                "out_val=(Sym == null ? 0 : Sym.length()) + sum(intCol) - max(longCol)"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "a=intCol", "b=longCol"), "Sym")
                .update("out_val=(Sym == null ? 0 : Sym.length()) + sum(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        // using the byte key column
        actual = t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                "out_val=byteCol == null ? -1 : byteCol + sum(intCol) - max(longCol)"), "byteCol");
        expected =
                t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "a=intCol", "b=longCol"), "byteCol")
                        .update("out_val=byteCol == null ? -1 : byteCol + sum(a) - max(b)").dropColumns("a", "b");

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    // endregion

    // region Append Only Tests

    @Test
    public void testZeroKeyAppendOnlyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    private void doTestAppendOnly(boolean bucketed, int prevTicks, int postTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        final EvalNugget[] nuggets = new EvalNugget[] {
                // Single column formula tests
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum(x + 1)", "x",
                                primitiveColumns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum(x + 1)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x * x + x)", "x",
                                primitiveColumns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x * x + x)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sumBigDecimal(x)",
                                "x", "bigDecimalCol"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sumBigDecimal(x)",
                                "x", "bigDecimalCol"))),
                // Multi-column formula tests
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "const_val=5"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "const_val=5"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum=sum(longCol) / 2"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum=sum(longCol) / 2"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                        "sum=sum(intCol) + sum(longCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                "sum=sum(intCol) + sum(longCol)"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                        "sum=min(intCol) + min(longCol) * max(doubleCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                "sum=min(intCol) + min(longCol) * max(doubleCol)"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                        "out_col=Sym == null ? 0.0 : min(intCol) + min(longCol) * max(doubleCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                "out_col=true"))), // This is a dummy test, we care about the bucketed test
        };

        final Random billy = new Random(0xB177B177L);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    private void doTestAppendOnlyTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        final EvalNugget[] nuggets = new EvalNugget[] {
                // Single column formula tests
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum(x + 1)", "x",
                                primitiveColumns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum(x + 1)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x * x + x)", "x",
                                primitiveColumns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x * x + x)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "sumBigDecimal(x)", "x", "bigDecimalCol"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "sumBigDecimal(x)", "x", "bigDecimalCol"))),
                // Multi-column formula tests
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "const_val=5"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "const_val=5"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum=sum(longCol) / 2"),
                                "Sym")
                        : t.updateBy(
                                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum=sum(longCol) / 2"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                        "sum=sum(intCol) + sum(longCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "sum=sum(intCol) + sum(longCol)"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                        "sum=min(intCol) + min(longCol) * max(doubleCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "sum=min(intCol) + min(longCol) * max(doubleCol)"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                        "out_col=Sym == null ? null : min(intCol) + min(longCol) * max(doubleCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "out_col=true"))), // This is a dummy test, we care about the bucketed test
        };

        final Random billy = new Random(0xB177B177L);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    // endregion Append Only Tests

    // region General Ticking Tests

    @Test
    public void testZeroKeyGeneralTickingRev() {
        final long prevTicks = 100;
        final long fwdTicks = 0;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingRevExclusive() {
        final long prevTicks = 100;
        final long fwdTicks = -50;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingFwd() {
        final long prevTicks = 0;
        final long fwdTicks = 100;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingFwdExclusive() {
        final long prevTicks = -50;
        final long fwdTicks = 100;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestTicking(false, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestTickingTimed(true, prevTime, postTime);
    }

    private void doTestTicking(final boolean bucketed, final long prevTicks, final long postTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum(x + 1)", "x",
                                primitiveColumns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum(x + 1)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x * x + x)", "x",
                                primitiveColumns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x * x + x)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sumBigDecimal(x)",
                                "x", "bigDecimalCol"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sumBigDecimal(x)",
                                "x", "bigDecimalCol"))),
                // Multi-column formula tests
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "const_val=5"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "const_val=5"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum=sum(longCol) / 2"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum=sum(longCol) / 2"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                        "sum=sum(intCol) + sum(longCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                "sum=sum(intCol) + sum(longCol)"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                        "sum=min(intCol) + min(longCol) * max(doubleCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks,
                                "sum=min(intCol) + min(longCol) * max(doubleCol)"))),
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    private void doTestTickingTimed(final boolean bucketed, final Duration prevTime, final Duration postTime) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        final QueryTable t = result.t;

        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum(x + 1)", "x",
                                primitiveColumns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum(x + 1)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x * x + x)", "x",
                                primitiveColumns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x * x + x)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "sumBigDecimal(x)", "x", "bigDecimalCol"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "sumBigDecimal(x)", "x", "bigDecimalCol"))),
                // Multi-column formula tests
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "const_val=5"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "const_val=5"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum=sum(longCol) / 2"),
                                "Sym")
                        : t.updateBy(
                                UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum=sum(longCol) / 2"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                        "sum=sum(intCol) + sum(longCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "sum=sum(intCol) + sum(longCol)"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                        "sum=min(intCol) + min(longCol) * max(doubleCol)"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingFormula("ts", prevTime, postTime,
                                "sum=min(intCol) + min(longCol) * max(doubleCol)"))),
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTickingRevRedirected() {
        final int prevTicks = 100;
        final int postTicks = 0;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingFormula(prevTicks, postTicks, "sum(x + 1)", "x", primitiveColumns))),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingFormula(prevTicks, postTicks, "avg(x * x + x)", "x",
                                primitiveColumns))),
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    @Test
    public void testBucketedGeneralTickingTimedRevRedirected() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingFormula("ts", prevTime, postTime, "sum(x + 1)", "x",
                                primitiveColumns))),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingFormula("ts", prevTime, postTime, "avg(x * x + x)", "x",
                                primitiveColumns))),
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    // endregion

    // region Special Tests

    @Test
    public void testRepeatedColumnTypes() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131,
                new String[] {"intCol2", "longCol2"},
                new TestDataGenerator[] {
                        new IntGenerator(10, 100, .1),
                        new LongGenerator(10, 100, .1),
                }).t;

        final int prevTicks = 100;
        final int postTicks = 0;

        Table actual;
        Table expected;

        String[] testColumns = new String[] {"intCol", "intCol2", "longCol", "longCol2"};

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Count vs. RollingCount
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "count(x)", "x", testColumns));
        expected = t.updateBy(UpdateByOperation.RollingCount(prevTicks, postTicks, testColumns));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);

        ////////////////////////////////////////////////////////////////////////////////////////////////////
        // Identity vs. RollingGroup
        ////////////////////////////////////////////////////////////////////////////////////////////////////

        actual = t.updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "x", "x", testColumns));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, testColumns))
                .update(Arrays.stream(testColumns).map(c -> c + "=" + c + ".getDirect()").toArray(String[]::new));

        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testProxy() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131).t;

        final int prevTicks = 100;
        final int postTicks = 0;

        Table actual;

        PartitionedTable pt = t.partitionBy("Sym");
        actual = pt.proxy().updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "count(x)", "x", "intCol"))
                .target().merge();

        actual = pt.proxy()
                .updateBy(UpdateByOperation.RollingFormula(prevTicks, postTicks, "intCol_count=count(intCol)"))
                .target().merge();
    }

    // endregion
}
