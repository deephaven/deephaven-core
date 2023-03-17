package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.ShortGenerator;
import io.deephaven.engine.testutil.generator.SortedDateTimeGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.time.DateTimeUtils.convertDateTime;

@Category(OutOfBandTest.class)
public class TestRollingWAvg extends BaseUpdateByTest {
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
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            // "bigIntCol",
            // "bigDecimalCol",
    };

    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    private String[] getFormulas(String[] columns, String weightCol) {
        return Arrays.stream(columns)
                .map(c -> String.format("%s=wavg(%s,%s)", c, c, weightCol))
                .toArray(String[]::new);
    }

    // region Object Helper functions
    //
    // final Function<ObjectVector<BigInteger>, BigDecimal> avgBigInt = bigIntegerObjectVector -> {
    // MathContext mathContextDefault = UpdateByControl.mathContextDefault();
    //
    // if (bigIntegerObjectVector == null) {
    // return null;
    // }
    //
    // BigDecimal sum = new BigDecimal(0);
    // long count = 0;
    //
    // final long n = bigIntegerObjectVector.size();
    //
    // for (long i = 0; i < n; i++) {
    // BigInteger val = bigIntegerObjectVector.get(i);
    // if (!isNull(val)) {
    // final BigDecimal decVal = new BigDecimal(val);
    // sum = sum.add(decVal, mathContextDefault);
    // count++;
    // }
    // }
    // if (count == 0) {
    // return null;
    // }
    // return sum.divide(new BigDecimal(count), mathContextDefault);
    // };
    //
    // final Function<ObjectVector<BigDecimal>, BigDecimal> avgBigDec = bigDecimalObjectVector -> {
    // MathContext mathContextDefault = UpdateByControl.mathContextDefault();
    //
    // if (bigDecimalObjectVector == null) {
    // return null;
    // }
    //
    // BigDecimal sum = new BigDecimal(0);
    // long count = 0;
    //
    // final long n = bigDecimalObjectVector.size();
    //
    // for (long i = 0; i < n; i++) {
    // BigDecimal val = bigDecimalObjectVector.get(i);
    // if (!isNull(val)) {
    // sum = sum.add(val, mathContextDefault);
    // count++;
    // }
    // }
    // if (count == 0) {
    // return null;
    // }
    // return sum.divide(new BigDecimal(count), mathContextDefault);
    // };
    //
    // private void doTestStaticZeroKeyBigNumbers(final QueryTable t, final int prevTicks, final int fwdTicks) {
    // QueryScope.addParam("avgBigInt", avgBigInt);
    // QueryScope.addParam("avgBigDec", avgBigDec);
    //
    // Table actual = t.updateBy(UpdateByOperation.RollingAvg(prevTicks, fwdTicks, "bigIntCol", "bigDecimalCol"));
    // Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, "bigIntCol", "bigDecimalCol"))
    // .update("bigIntCol=avgBigInt.apply(bigIntCol)", "bigDecimalCol=avgBigDec.apply(bigDecimalCol)");
    //
    // BigDecimal[] biActual = (BigDecimal[]) actual.getColumn("bigIntCol").getDirect();
    // Object[] biExpected = (Object[]) expected.getColumn("bigIntCol").getDirect();
    //
    // Assert.eq(biActual.length, "array length", biExpected.length);
    // for (int ii = 0; ii < biActual.length; ii++) {
    // BigDecimal actualVal = biActual[ii];
    // BigDecimal expectedVal = (BigDecimal) biExpected[ii];
    // if (actualVal != null || expectedVal != null) {
    // Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
    // }
    // }
    //
    // BigDecimal[] bdActual = (BigDecimal[]) actual.getColumn("bigDecimalCol").getDirect();
    // Object[] bdExpected = (Object[]) expected.getColumn("bigDecimalCol").getDirect();
    //
    // Assert.eq(bdActual.length, "array length", bdExpected.length);
    // for (int ii = 0; ii < bdActual.length; ii++) {
    // BigDecimal actualVal = bdActual[ii];
    // BigDecimal expectedVal = (BigDecimal) bdExpected[ii];
    // if (actualVal != null || expectedVal != null) {
    // Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
    // }
    // }
    // }
    //
    // private void doTestStaticZeroKeyTimedBigNumbers(final QueryTable t, final Duration prevTime,
    // final Duration postTime) {
    // QueryScope.addParam("avgBigInt", avgBigInt);
    // QueryScope.addParam("avgBigDec", avgBigDec);
    //
    // Table actual = t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"));
    // Table expected =
    // t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"))
    // .update("bigIntCol=avgBigInt.apply(bigIntCol)", "bigDecimalCol=avgBigDec.apply(bigDecimalCol)");
    //
    // BigDecimal[] biActual = (BigDecimal[]) actual.getColumn("bigIntCol").getDirect();
    // Object[] biExpected = (Object[]) expected.getColumn("bigIntCol").getDirect();
    //
    // Assert.eq(biActual.length, "array length", biExpected.length);
    // for (int ii = 0; ii < biActual.length; ii++) {
    // BigDecimal actualVal = biActual[ii];
    // BigDecimal expectedVal = (BigDecimal) biExpected[ii];
    // if (actualVal != null || expectedVal != null) {
    // Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
    // }
    // }
    //
    // BigDecimal[] bdActual = (BigDecimal[]) actual.getColumn("bigDecimalCol").getDirect();
    // Object[] bdExpected = (Object[]) expected.getColumn("bigDecimalCol").getDirect();
    //
    // Assert.eq(bdActual.length, "array length", bdExpected.length);
    // for (int ii = 0; ii < bdActual.length; ii++) {
    // BigDecimal actualVal = bdActual[ii];
    // BigDecimal expectedVal = (BigDecimal) bdExpected[ii];
    // if (actualVal != null || expectedVal != null) {
    // Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
    // }
    // }
    // }
    //
    // private void doTestStaticBucketedBigNumbers(final QueryTable t, final int prevTicks, final int fwdTicks) {
    // QueryScope.addParam("avgBigInt", avgBigInt);
    // QueryScope.addParam("avgBigDec", avgBigDec);
    //
    // Table actual =
    // t.updateBy(UpdateByOperation.RollingAvg(prevTicks, fwdTicks, "bigIntCol", "bigDecimalCol"), "Sym");
    // Table expected =
    // t.updateBy(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, "bigIntCol", "bigDecimalCol"), "Sym")
    // .update("bigIntCol=avgBigInt.apply(bigIntCol)", "bigDecimalCol=avgBigDec.apply(bigDecimalCol)");
    //
    // BigDecimal[] biActual = (BigDecimal[]) actual.getColumn("bigIntCol").getDirect();
    // Object[] biExpected = (Object[]) expected.getColumn("bigIntCol").getDirect();
    //
    // Assert.eq(biActual.length, "array length", biExpected.length);
    // for (int ii = 0; ii < biActual.length; ii++) {
    // BigDecimal actualVal = biActual[ii];
    // BigDecimal expectedVal = (BigDecimal) biExpected[ii];
    // if (actualVal != null || expectedVal != null) {
    // Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
    // }
    // }
    //
    // BigDecimal[] bdActual = (BigDecimal[]) actual.getColumn("bigDecimalCol").getDirect();
    // Object[] bdExpected = (Object[]) expected.getColumn("bigDecimalCol").getDirect();
    //
    // Assert.eq(bdActual.length, "array length", bdExpected.length);
    // for (int ii = 0; ii < bdActual.length; ii++) {
    // BigDecimal actualVal = bdActual[ii];
    // BigDecimal expectedVal = (BigDecimal) bdExpected[ii];
    // if (actualVal != null || expectedVal != null) {
    // Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
    // }
    // }
    // }
    //
    // private void doTestStaticBucketedTimedBigNumbers(final QueryTable t, final Duration prevTime,
    // final Duration postTime) {
    // QueryScope.addParam("avgBigInt", avgBigInt);
    // QueryScope.addParam("avgBigDec", avgBigDec);
    //
    // Table actual =
    // t.updateBy(UpdateByOperation.RollingAvg("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"), "Sym");
    // Table expected = t
    // .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"), "Sym")
    // .update("bigIntCol=avgBigInt.apply(bigIntCol)", "bigDecimalCol=avgBigDec.apply(bigDecimalCol)");
    //
    // BigDecimal[] biActual = (BigDecimal[]) actual.getColumn("bigIntCol").getDirect();
    // Object[] biExpected = (Object[]) expected.getColumn("bigIntCol").getDirect();
    //
    // Assert.eq(biActual.length, "array length", biExpected.length);
    // for (int ii = 0; ii < biActual.length; ii++) {
    // BigDecimal actualVal = biActual[ii];
    // BigDecimal expectedVal = (BigDecimal) biExpected[ii];
    // if (actualVal != null || expectedVal != null) {
    // Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
    // }
    // }
    //
    // BigDecimal[] bdActual = (BigDecimal[]) actual.getColumn("bigDecimalCol").getDirect();
    // Object[] bdExpected = (Object[]) expected.getColumn("bigDecimalCol").getDirect();
    //
    // Assert.eq(bdActual.length, "array length", bdExpected.length);
    // for (int ii = 0; ii < bdActual.length; ii++) {
    // BigDecimal actualVal = bdActual[ii];
    // BigDecimal expectedVal = (BigDecimal) bdExpected[ii];
    // if (actualVal != null || expectedVal != null) {
    // Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
    // }
    // }
    // }
    // endregion

    // region Static Zero Key Tests

    @Test
    public void testStaticZeroKeyRev() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestStaticZeroKey(prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestStaticZeroKey(prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestStaticZeroKey(prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestStaticZeroKey(prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final int prevTicks = 100;
        final int fwdTicks = 100;

        doTestStaticZeroKey(prevTicks, fwdTicks);
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

    private void doTestStaticZeroKey(final int prevTicks, final int fwdTicks) {
        // Test with a double type weight value
        String weightCol = "doubleWeightCol";

        QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131,
                new String[] {weightCol},
                new TestDataGenerator[] {new DoubleGenerator(10.1, 20.1, .1)}).t;

        Table actual = t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, primitiveColumns));
        // We need the weight column as vector here for comparison
        Table expected = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, primitiveColumns),
                        UpdateByOperation.RollingGroup(prevTicks, fwdTicks, weightCol)))
                .update(getFormulas(primitiveColumns, weightCol));

        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        ///////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        weightCol = "shortWeightCol";

        t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131,
                new String[] {weightCol},
                new TestDataGenerator[] {new ShortGenerator((short) -6000, (short) 65535, .1)}).t;

        actual = t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, primitiveColumns));
        // We need the weight column as vector here for comparison
        expected = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, primitiveColumns),
                        UpdateByOperation.RollingGroup(prevTicks, fwdTicks, weightCol)))
                .update(getFormulas(primitiveColumns, weightCol));

        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        // doTestStaticZeroKeyBigNumbers(t, prevTicks, fwdTicks);
    }

    private void doTestStaticZeroKeyTimed(final Duration prevTime, final Duration postTime) {
        String weightCol = "doubleWeightCol";

        QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts", weightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new DoubleGenerator(10.1, 20.1, .1)}).t;

        Table actual = t.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, primitiveColumns));
        // We need the weight column as vector here for comparison.
        Table expected = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns),
                        UpdateByOperation.RollingGroup("ts", prevTime, postTime, weightCol)))
                .update(getFormulas(primitiveColumns, weightCol));
        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        ///////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        weightCol = "shortWeightCol";

        t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts", weightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new ShortGenerator((short) -6000, (short) 65535, .1)}).t;

        actual = t.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, primitiveColumns));
        // We need the weight column as vector here for comparison.
        expected = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns),
                        UpdateByOperation.RollingGroup("ts", prevTime, postTime, weightCol)))
                .update(getFormulas(primitiveColumns, weightCol));
        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        // doTestStaticZeroKeyTimedBigNumbers(t, prevTime, postTime);
    }

    // endregion

    // region Static Bucketed Tests

    @Test
    public void testStaticGroupedBucketed() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestStaticBucketed(true, prevTicks, fwdTicks);
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
        final int fwdTicks = 0;

        doTestStaticBucketed(false, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticBucketedRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestStaticBucketed(false, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticBucketedFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestStaticBucketed(false, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticBucketedFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestStaticBucketed(false, prevTicks, fwdTicks);
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

    private void doTestStaticBucketed(boolean grouped, int prevTicks, int fwdTicks) {
        // Test with a double type weight value
        String weightCol = "doubleWeightCol";

        QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131,
                new String[] {weightCol},
                new TestDataGenerator[] {new DoubleGenerator(10.1, 20.1, .1)}).t;

        Table actual =
                t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, primitiveColumns), "Sym");
        // We need the weight column as vector here for comparison
        Table expected = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, primitiveColumns),
                        UpdateByOperation.RollingGroup(prevTicks, fwdTicks, weightCol)),
                "Sym")
                .update(getFormulas(primitiveColumns, weightCol));

        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        ///////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        weightCol = "shortWeightCol";

        t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131,
                new String[] {weightCol},
                new TestDataGenerator[] {new ShortGenerator((short) -6000, (short) 65535, .1)}).t;

        actual = t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, primitiveColumns), "Sym");
        // We need the weight column as vector here for comparison
        expected = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, primitiveColumns),
                        UpdateByOperation.RollingGroup(prevTicks, fwdTicks, weightCol)),
                "Sym")
                .update(getFormulas(primitiveColumns, weightCol));

        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        // doTestStaticBucketedBigNumbers(t, prevTicks, fwdTicks);
    }

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        String weightCol = "doubleWeightCol";

        QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0xFFFABBBC,
                new String[] {"ts", weightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new DoubleGenerator(10.1, 20.1, .1)}).t;

        Table actual =
                t.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, primitiveColumns), "Sym");
        // We need the weight column as vector here for comparison.
        Table expected = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns),
                        UpdateByOperation.RollingGroup("ts", prevTime, postTime, weightCol)),
                "Sym")
                .update(getFormulas(primitiveColumns, weightCol));
        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        ///////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        weightCol = "shortWeightCol";

        t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0xFFFABBBC,
                new String[] {"ts", weightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new ShortGenerator((short) -6000, (short) 65535, .1)}).t;

        actual = t.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, primitiveColumns),
                "Sym");
        // We need the weight column as vector here for comparison.
        expected = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns),
                        UpdateByOperation.RollingGroup("ts", prevTime, postTime, weightCol)),
                "Sym")
                .update(getFormulas(primitiveColumns, weightCol));
        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        // doTestStaticBucketedTimedBigNumbers(t, prevTime, postTime);
    }

    // endregion

    // region Append Only Tests

    @Test
    public void testZeroKeyAppendOnlyRev() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdRev() {
        final int prevTicks = 50;
        final int fwdTicks = 50;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedAppendOnlyRev() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestAppendOnly(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestAppendOnly(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestAppendOnly(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestAppendOnly(true, prevTicks, fwdTicks);
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

    private void doTestAppendOnly(boolean bucketed, int prevTicks, int fwdTicks) {
        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {doubleWeightCol},
                new TestDataGenerator[] {new DoubleGenerator(10.1, 20.1, .1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns),
                                        "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT
                    .runWithinUnitTestCycle(() -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {shortWeightCol},
                new TestDataGenerator[] {new ShortGenerator((short) -6000, (short) 65535, .1)});
        final QueryTable t2 = result2.t;
        t2.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t2.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns),
                                        "Sym")
                                : t2.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT
                    .runWithinUnitTestCycle(() -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    private void doTestAppendOnlyTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0xFFFABBBC,
                new String[] {"ts", doubleWeightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new DoubleGenerator(10.1, 20.1, .1)});

        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                                columns),
                                        "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                                columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT
                    .runWithinUnitTestCycle(() -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0xFFFABBBC,
                new String[] {"ts", shortWeightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new ShortGenerator((short) -6000, (short) 65535, .1)});

        final QueryTable t2 = result2.t;
        t2.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t2.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                                columns),
                                        "Sym")
                                : t2.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                        columns));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT
                    .runWithinUnitTestCycle(() -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos));
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
        final int fwdTicks = 0;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestTicking(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestTicking(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestTicking(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdRev() {
        final int prevTicks = 50;
        final int fwdTicks = 50;

        doTestTicking(true, prevTicks, fwdTicks);
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

    private void doTestTicking(final boolean bucketed, final long prevTicks, final long fwdTicks) {
        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {doubleWeightCol},
                new TestDataGenerator[] {new DoubleGenerator(10.1, 20.1, .1)});
        final QueryTable t = result.t;

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns),
                                        "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {shortWeightCol},
                new TestDataGenerator[] {new ShortGenerator((short) -6000, (short) 65535, .1)});
        final QueryTable t2 = result2.t;

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t2.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns),
                                        "Sym")
                                : t2.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    private void doTestTickingTimed(final boolean bucketed, final Duration prevTime, final Duration postTime) {
        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0xFFFABBBC,
                new String[] {"ts", doubleWeightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new DoubleGenerator(10.1, 20.1, .1)});

        final QueryTable t = result.t;

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                                columns),
                                        "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                                columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0xFFFABBBC,
                new String[] {"ts", shortWeightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new ShortGenerator((short) -6000, (short) 65535, .1)});

        final QueryTable t2 = result2.t;

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t2.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                                columns),
                                        "Sym")
                                : t2.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                        columns));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTickingRevRedirected() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {doubleWeightCol},
                new TestDataGenerator[] {new DoubleGenerator(10.1, 20.1, .1)});
        final QueryTable t = result.t;

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns)),
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

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {shortWeightCol},
                new TestDataGenerator[] {new ShortGenerator((short) -6000, (short) 65535, .1)});
        final QueryTable t2 = result2.t;

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t2.updateBy(control,
                                List.of(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns)),
                                ColumnName.from("Sym"));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos, nuggets);
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

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0xFFFABBBC,
                new String[] {"ts", doubleWeightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new DoubleGenerator(10.1, 20.1, .1)});

        final QueryTable t = result.t;

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                        columns)),
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

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0xFFFABBBC,
                new String[] {"ts", shortWeightCol}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new ShortGenerator((short) -6000, (short) 65535, .1)});

        final QueryTable t2 = result2.t;

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t2.updateBy(control,
                                List.of(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                        columns)),
                                ColumnName.from("Sym"));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    // endregion
}
