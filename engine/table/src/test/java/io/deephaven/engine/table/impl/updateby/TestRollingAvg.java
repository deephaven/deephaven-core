package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.SortedDateTimeGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.vector.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.function.Basic.isNull;
import static io.deephaven.time.DateTimeUtils.convertDateTime;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestRollingAvg extends BaseUpdateByTest {
    final String[] rollingGroupPairs = new String[] {
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            "boolCol",
    };

    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    private String[] getFormulas(String[] columns) {
        return Arrays.stream(columns).map(c -> c + "=avg(" + c + ")").toArray(String[]::new);
    }

    @Test
    public void testStaticZeroKeyBigNumbers() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final Function<ObjectVector<BigInteger>, BigDecimal> avgBigInt = bigIntegerObjectVector -> {
            MathContext mathContextDefault = UpdateByControl.mathContextDefault();

            if (bigIntegerObjectVector == null) {
                return null;
            }

            BigDecimal sum = new BigDecimal(0);
            long count = 0;

            final long n = bigIntegerObjectVector.size();

            for (long i = 0; i < n; i++) {
                BigInteger val = bigIntegerObjectVector.get(i);
                if (!isNull(val)) {
                    final BigDecimal decVal = new BigDecimal(val);
                    sum = sum.add(decVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0) {
                return null;
            }
            return sum.divide(new BigDecimal(count), mathContextDefault);
        };
        QueryScope.addParam("avgBigInt", avgBigInt);

        final Function<ObjectVector<BigDecimal>, BigDecimal> avgBigDec = bigDecimalObjectVector -> {
            MathContext mathContextDefault = UpdateByControl.mathContextDefault();

            if (bigDecimalObjectVector == null) {
                return null;
            }

            BigDecimal sum = new BigDecimal(0);
            long count = 0;


            final long n = bigDecimalObjectVector.size();

            for (long i = 0; i < n; i++) {
                BigDecimal val = bigDecimalObjectVector.get(i);
                if (!isNull(val)) {
                    sum = sum.add(val, mathContextDefault);
                    count++;
                }
            }

            return sum.divide(new BigDecimal(count), mathContextDefault);
        };
        QueryScope.addParam("avgBigDec", avgBigDec);

        final int prevTicks = 100;
        final int postTicks = 0;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "bigIntCol", "bigDecimalCol")).
                update("bigIntCol=avgBigInt.apply(bigIntCol)", "bigDecimalCol=avgBigDec.apply(bigDecimalCol)");

        BigDecimal[] actualData = (BigDecimal[])actual.getColumn("bigIntCol").getDirect();
        Object[] expectedData = (Object[])expected.getColumn("bigIntCol").getDirect();

        Assert.eq(actualData.length, "array length", expectedData.length);
        for (int ii = 0; ii < actualData.length; ii++) {
            BigDecimal actualVal = actualData[ii];
            BigDecimal expectedVal = (BigDecimal)expectedData[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        actualData = (BigDecimal[])actual.getColumn("bigDecimalCol").getDirect();
        expectedData = (Object[])expected.getColumn("bigDecimalCol").getDirect();

        Assert.eq(actualData.length, "array length", expectedData.length);
        for (int ii = 0; ii < actualData.length; ii++) {
            BigDecimal actualVal = actualData[ii];
            BigDecimal expectedVal = (BigDecimal)expectedData[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }
    }


    // region Static Zero Key Tests
    @Test
    public void testStaticZeroKeyRev() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = 100;
        final int postTicks = 0;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyRevExclusive() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = 100;
        final int postTicks = -50;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyFwd() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = 0;
        final int postTicks = 100;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyFwdExclusive() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = -50;
        final int postTicks = 100;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = 100;
        final int postTicks = 100;

        doTestStaticZeroKey(false, prevTicks, postTicks);
    }

    private void doTestStaticZeroKey(boolean grouped, int prevTicks, int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0x31313131).t;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs), "Sym");
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, rollingGroupPairs), "Sym").
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyTimedRev() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ZERO;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyTimedRevExclusive() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyTimedFwd() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ZERO;
        final Duration postTime = Duration.ofMinutes(10);

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyTimedFwdExclusive() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testStaticZeroKeyTimedFwdRev() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(10);

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs));
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, rollingGroupPairs)).
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    // endregion

    // region Static Bucketed Tests

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
    public void testStaticGroupedBucketed() {
        final int prevTicks = 100;
        final int postTicks = 0;
        doTestStaticBucketed(true, prevTicks, postTicks);
    }

    private void doTestStaticBucketed(boolean grouped, int prevTicks, int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0x31313131).t;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs), "Sym");
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, rollingGroupPairs), "Sym").
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
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

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final Table actual = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs), "Sym");
        final Table expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, rollingGroupPairs), "Sym").
                update(getFormulas(rollingGroupPairs));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }

    // endregion

    // region Live Tests

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

    private void doTestAppendOnly(boolean bucketed, int prevTicks, int postTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131);
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs),
                                        "Sym")
                                : t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT
                    .runWithinUnitTestCycle(() -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
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

    private void doTestAppendOnlyTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(
                                UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs), "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT
                    .runWithinUnitTestCycle(() -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
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

    @Test
    public void testZeroKeyGeneralTickingRev() {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, false, false, true, 0x31313131);
        final QueryTable t = result.t;

        final long prevTicks = 100;
        final long fwdTicks = 0;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(prevTicks, fwdTicks, rollingGroupPairs));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTickingRevExclusive() {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, false, false, true, 0x31313131);
        final QueryTable t = result.t;

        final long prevTicks = 100;
        final long fwdTicks = -50;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(prevTicks, fwdTicks, rollingGroupPairs));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTickingFwd() {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, false, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(100, rollingGroupPairs));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTickingFwdExclusive() {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, false, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(-50, 100, rollingGroupPairs));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTickingRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs),
                                "Sym");
                    }
                }
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
    public void testBucketedGeneralTickingRevRedirected() {
        final int prevTicks = 100;
        final int postTicks = 0;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131);
        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingAvg(prevTicks, postTicks, rollingGroupPairs)),
                                ColumnName.from("Sym"));
                    }
                }
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
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});

        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingAvg("ts", prevTime, postTime, rollingGroupPairs)),
                                ColumnName.from("Sym"));
                    }
                }
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


    /*
     * Ideas for specialized tests: 1) Remove first index 2) Removed everything, add some back 3) Make sandwiches
     */
    // endregion


    // implement these calculations as pure rolling sums with local storage

    private byte[][] rollingGroup(byte[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new byte[0][0];
        }

        byte[][] result = new byte[values.length][];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new byte[size];
                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private short[][] rollingGroup(short[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new short[0][0];
        }

        short[][] result = new short[values.length][];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new short[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private int[][] rollingGroup(int[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new int[0][0];
        }

        int[][] result = new int[values.length][];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new int[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private long[][] rollingGroup(long[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0][0];
        }

        long[][] result = new long[values.length][];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new long[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private float[][] rollingGroup(float[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new float[0][0];
        }

        float[][] result = new float[values.length][];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new float[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private double[][] rollingGroup(double[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0][0];
        }

        double[][] result = new double[values.length][];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new double[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private Object[][] rollingGroup(Object[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0][0];
        }

        Object[][] result = new Object[values.length][];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new Object[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private byte[][] rollingGroupTime(byte[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new byte[0][0];
        }

        byte[][] result = new byte[values.length][];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new byte[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private short[][] rollingGroupTime(short[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new short[0][0];
        }

        short[][] result = new short[values.length][];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new short[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private int[][] rollingGroupTime(int[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new int[0][0];
        }

        int[][] result = new int[values.length][];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new int[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[][] rollingGroupTime(long[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0][0];
        }

        long[][] result = new long[values.length][];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new long[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private float[][] rollingGroupTime(float[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new float[0][0];
        }

        float[][] result = new float[values.length][];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new float[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private double[][] rollingGroupTime(double[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0][0];
        }

        double[][] result = new double[values.length][];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new double[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private Object[][] rollingGroupTime(Object[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0][0];
        }

        Object[][] result = new Object[values.length][];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new Object[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    final byte[][] convertToArray(ByteVector[] vectors) {
        final byte[][] result = new byte[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final short[][] convertToArray(ShortVector[] vectors) {
        final short[][] result = new short[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final int[][] convertToArray(IntVector[] vectors) {
        final int[][] result = new int[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final long[][] convertToArray(LongVector[] vectors) {
        final long[][] result = new long[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final float[][] convertToArray(FloatVector[] vectors) {
        final float[][] result = new float[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final double[][] convertToArray(DoubleVector[] vectors) {
        final double[][] result = new double[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final Object[][] convertToArray(ObjectVector[] vectors) {
        final Object[][] result = new Object[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final void assertWithRollingAvgTicks(final @NotNull Object expected, final @NotNull Object actual, Class type,
            int prevTicks, int postTicks) {

        if (expected instanceof byte[]) {
            assertArrayEquals(rollingGroup((byte[]) expected, prevTicks, postTicks),
                    convertToArray((ByteVector[]) actual));
        } else if (expected instanceof short[]) {
            assertArrayEquals(rollingGroup((short[]) expected, prevTicks, postTicks),
                    convertToArray((ShortVector[]) actual));
        } else if (expected instanceof int[]) {
            assertArrayEquals(rollingGroup((int[]) expected, prevTicks, postTicks),
                    convertToArray((IntVector[]) actual));
        } else if (expected instanceof long[]) {
            assertArrayEquals(rollingGroup((long[]) expected, prevTicks, postTicks),
                    convertToArray((LongVector[]) actual));
        } else if (expected instanceof float[]) {
            assertArrayEquals(rollingGroup((float[]) expected, prevTicks, postTicks),
                    convertToArray((FloatVector[]) actual));
        } else if (expected instanceof double[]) {
            assertArrayEquals(rollingGroup((double[]) expected, prevTicks, postTicks),
                    convertToArray((DoubleVector[]) actual));
        } else {
            if (type == BigDecimal.class || type == BigInteger.class) {
                assertArrayEquals(rollingGroup((Object[]) expected, prevTicks, postTicks),
                        convertToArray((ObjectVector[]) actual));
            }
        }
    }

    final void assertWithRollingAvgTime(final @NotNull Object expected, final @NotNull Object actual,
            final @NotNull long[] timestamps, Class type, long prevTime, long postTime) {

        if (expected instanceof byte[]) {
            assertArrayEquals(rollingGroupTime((byte[]) expected, timestamps, prevTime, postTime),
                    convertToArray((ByteVector[]) actual));
        } else if (expected instanceof short[]) {
            assertArrayEquals(rollingGroupTime((short[]) expected, timestamps, prevTime, postTime),
                    convertToArray((ShortVector[]) actual));
        } else if (expected instanceof int[]) {
            assertArrayEquals(rollingGroupTime((int[]) expected, timestamps, prevTime, postTime),
                    convertToArray((IntVector[]) actual));
        } else if (expected instanceof long[]) {
            assertArrayEquals(rollingGroupTime((long[]) expected, timestamps, prevTime, postTime),
                    convertToArray((LongVector[]) actual));
        } else if (expected instanceof float[]) {
            assertArrayEquals(rollingGroupTime((float[]) expected, timestamps, prevTime, postTime),
                    convertToArray((FloatVector[]) actual));
        } else if (expected instanceof double[]) {
            assertArrayEquals(rollingGroupTime((double[]) expected, timestamps, prevTime, postTime),
                    convertToArray((DoubleVector[]) actual));
        } else {
            if (type == BigDecimal.class || type == BigInteger.class) {
                assertArrayEquals(rollingGroupTime((Object[]) expected, timestamps, prevTime, postTime),
                        convertToArray((ObjectVector[]) actual));
            }
        }
    }
}
